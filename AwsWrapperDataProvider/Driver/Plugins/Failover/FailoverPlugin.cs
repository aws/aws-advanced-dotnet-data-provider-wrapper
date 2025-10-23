// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins.AuroraStaleDns;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;
using ThreadState = System.Threading.ThreadState;

namespace AwsWrapperDataProvider.Driver.Plugins.Failover;

/// <summary>
/// Failover Plugin v.2
/// This plugin provides cluster-aware failover features. The plugin switches connections upon
/// detecting communication related exceptions and/or cluster topology changes.
/// </summary>
public class FailoverPlugin : AbstractConnectionPlugin
{
    // Method names
    private const string MethodClose = "DbConnection.Close";
    private const string MethodCloseAsync = "DbConnection.CloseAsync";
    private const string MethodDispose = "DbConnection.Dispose";

    private static readonly ILogger<FailoverPlugin> Logger = LoggerUtils.GetLogger<FailoverPlugin>();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly int failoverTimeoutMs;
    private readonly FailoverMode failoverMode;
    private readonly string failoverReaderHostSelectorStrategy;
    private readonly bool enableConnectFailover;
    private readonly bool skipFailoverOnInterruptedThread;
    private readonly bool closedExplicitly = false;
    private readonly AuroraStaleDnsHelper auroraStaleDnsHelper;

    private IHostListProviderService? hostListProviderService;
    private RdsUrlType? rdsUrlType;

    private bool isClosed;
    private bool shouldThrowTransactionError = false;
    private Exception? lastExceptionDealtWith;

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>()
    {
        // Network-bound methods that might fail and trigger failover
        "DbConnection.Open",
        "DbConnection.OpenAsync",
        "DbConnection.BeginDbTransaction",
        "DbConnection.BeginDbTransactionAsync",

        "DbCommand.ExecuteNonQuery",
        "DbCommand.ExecuteNonQueryAsync",
        "DbCommand.ExecuteReader",
        "DbCommand.ExecuteReaderAsync",
        "DbCommand.ExecuteScalar",
        "DbCommand.ExecuteScalarAsync",

        "DbDataReader.Read",
        "DbDataReader.ReadAsync",
        "DbDataReader.NextResult",
        "DbDataReader.NextResultAsync",

        "DbTransaction.Commit",
        "DbTransaction.CommitAsync",
        "DbTransaction.Rollback",
        "DbTransaction.RollbackAsync",

        // Special methods
        "DbConnection.ClearWarnings",
        "initHostProvider",
    };

    public FailoverPlugin(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        this.props = props;

        // Initialize configuration settings using PropertyDefinition
        this.failoverTimeoutMs = (int)PropertyDefinition.FailoverTimeoutMs.GetInt(props)!;
        this.failoverMode = this.GetFailoverMode();
        this.failoverReaderHostSelectorStrategy = PropertyDefinition.ReaderHostSelectorStrategy.GetString(props)!;
        this.enableConnectFailover = PropertyDefinition.EnableConnectFailover.GetBoolean(props);
        this.skipFailoverOnInterruptedThread = PropertyDefinition.SkipFailoverOnInterruptedThread.GetBoolean(props);
        this.auroraStaleDnsHelper = new AuroraStaleDnsHelper(pluginService);
    }

    public override async Task<T> Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        if (this.pluginService.CurrentConnection != null
            && !this.CanDirectExecute(methodName)
            && !this.closedExplicitly
            && this.pluginService.CurrentConnection.State == ConnectionState.Closed)
        {
            await this.PickNewConnectionAsync();
        }

        if (this.CanDirectExecute(methodName))
        {
            return await methodFunc();
        }

        if (this.isClosed && !this.AllowedOnClosedConnection(methodName))
        {
            await this.InvalidInvocationOnClosedConnection();
        }

        try
        {
            var result = await methodFunc();
            return result;
        }
        catch (Exception exception)
        {
            await this.DealWithOriginalException(exception);
        }

        throw new UnreachableException("[FailoverPlugin] Should not reach here.");
    }

    private void InitFailoverMode()
    {
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);
        ArgumentNullException.ThrowIfNull(this.hostListProviderService.InitialConnectionHostSpec);

        this.rdsUrlType = RdsUtils.IdentifyRdsType(this.hostListProviderService.InitialConnectionHostSpec.Host);
    }

    public override async Task<DbConnection> OpenConnection(HostSpec? hostSpec, Dictionary<string, string> properties, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        this.InitFailoverMode();

        DbConnection? connection = null;

        if (!this.enableConnectFailover || hostSpec == null)
        {
            return await this.auroraStaleDnsHelper.OpenVerifiedConnectionAsync(
                isInitialConnection,
                this.hostListProviderService!,
                hostSpec!,
                properties,
                methodFunc);
        }

        var hostSpecWithAvailability = this.pluginService.GetHosts()
            .FirstOrDefault(x => x.Host == hostSpec.Host && x.Port == hostSpec.Port);

        if (hostSpecWithAvailability == null || hostSpecWithAvailability is not { Availability: HostAvailability.Unavailable })
        {
            try
            {
                connection = await this.auroraStaleDnsHelper.OpenVerifiedConnectionAsync(
                    isInitialConnection,
                    this.hostListProviderService!,
                    hostSpec!,
                    properties,
                    methodFunc);
            }
            catch (Exception e)
            {
                if (!this.ShouldExceptionTriggerConnectionSwitch(e))
                {
                    throw;
                }

                this.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Unavailable);

                try
                {
                    await this.FailoverAsync();
                }
                catch (FailoverSuccessException)
                {
                    connection = this.pluginService.CurrentConnection!;
                }
            }
        }
        else
        {
            try
            {
                this.pluginService.RefreshHostList();
                await this.FailoverAsync();
            }
            catch (FailoverSuccessException)
            {
                connection = this.pluginService.CurrentConnection!;
            }
        }

        if (connection == null)
        {
            throw new InvalidOperationException("Unable to establish connection");
        }

        if (isInitialConnection)
        {
            this.pluginService.RefreshHostList(connection);
        }

        return connection;
    }

    public override Task InitHostProvider(string initialUrl, Dictionary<string, string> properties, IHostListProviderService initHostListProviderService, ADONetDelegate initHostProviderFunc)
    {
        this.hostListProviderService = initHostListProviderService;
        initHostProviderFunc();
        return Task.CompletedTask;
    }

    private async Task InvalidInvocationOnClosedConnection()
    {
        if (!this.closedExplicitly)
        {
            this.isClosed = false;
            Logger.LogWarning("Connection was closed but not explicitly. Attempting to pick a new connection.");
            await this.PickNewConnectionAsync();
            throw new FailoverSuccessException("The active connection has changed. Please re-configure session state if required.");
        }

        throw new InvalidOperationException("Connection is closed");
    }

    private bool AllowedOnClosedConnection(string methodName)
    {
        return this.pluginService.TargetConnectionDialect.GetAllowedOnConnectionMethodNames().Contains(methodName);
    }

    private async Task DealWithOriginalException(Exception originalException)
    {
        Logger.LogDebug("Processing exception: {ExceptionMessage}", originalException.ToString());

        if (this.ShouldExceptionTriggerConnectionSwitch(originalException))
        {
            if (this.lastExceptionDealtWith == originalException)
            {
                throw originalException;
            }

            await this.InvalidateCurrentConnectionAsync();

            if (this.pluginService.CurrentHostSpec != null)
            {
                Logger.LogInformation("Marking host {Host} as unavailable.", this.pluginService.CurrentHostSpec.Host);
                this.pluginService.SetAvailability(this.pluginService.CurrentHostSpec.AsAliases(), HostAvailability.Unavailable);
            }

            await this.PickNewConnectionAsync();
            this.lastExceptionDealtWith = originalException;
        }

        throw originalException;
    }

    private async Task FailoverAsync()
    {
        Logger.LogInformation("Initiating failover in mode: {FailoverMode}.", this.failoverMode);

        if (this.failoverMode == FailoverMode.StrictWriter)
        {
            await this.FailoverWriterAsync();
        }
        else
        {
            await this.FailoverReaderAsync();
        }
    }

    private async Task FailoverReaderAsync()
    {
        Logger.LogInformation("Starting reader failover process.");
        await this.pluginService.ForceRefreshHostListAsync(false, 0);

        var result = await this.GetReaderFailoverConnectionAsync(DateTime.UtcNow.AddMilliseconds(this.failoverTimeoutMs));
        Logger.LogInformation("Reader failover successful. Switching to host: {Host}.", result.HostSpec.Host);

        this.pluginService.SetCurrentConnection(result.Connection, result.HostSpec);
        this.ThrowFailoverSuccessException();
    }

    private async Task<ReaderFailoverResult> GetReaderFailoverConnectionAsync(DateTime failoverEndTime)
    {
        var hosts = this.pluginService.GetHosts();
        var originalWriter = hosts.FirstOrDefault(h => h.Role == HostRole.Writer);
        bool isOriginalWriterStillWriter = false;

        do
        {
            // Update reader candidates, topology may have changed
            hosts = this.pluginService.GetHosts();
            Logger.LogDebug(LoggerUtils.LogTopology(hosts, $"All hosts: "));
            var readerCandidates = hosts.Where(h => h.Role == HostRole.Reader).ToHashSet();

            // First, try all original readers
            var remainingReaders = new HashSet<HostSpec>(readerCandidates);
            while (remainingReaders.Count > 0 && DateTime.UtcNow < failoverEndTime)
            {
                HostSpec? readerCandidate = null;
                try
                {
                    readerCandidate = this.pluginService.GetHostSpecByStrategy([.. remainingReaders], HostRole.Reader, this.failoverReaderHostSelectorStrategy);
                }
                catch (Exception ex)
                {
                    Logger.LogInformation(ex, LoggerUtils.LogTopology([.. remainingReaders], $"An error occurred while attempting to select a reader host candidate {readerCandidate} from Candidates"));
                    break;
                }

                if (readerCandidate is null)
                {
                    Logger.LogInformation(LoggerUtils.LogTopology([.. remainingReaders], "Unable to find reader in updated host list"));
                    break;
                }

                try
                {
                    DbConnection candidateConn = await this.pluginService.OpenConnection(readerCandidate, this.props, this, true);
                    var role = this.pluginService.GetHostRole(candidateConn);

                    if (role == HostRole.Reader || this.failoverMode != FailoverMode.StrictReader)
                    {
                        var updatedHostSpec = new HostSpec(readerCandidate.Host, readerCandidate.Port, role, readerCandidate.Availability);
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
                    }

                    // The role is Writer or Unknown, and we are in StrictReader mode
                    remainingReaders.Remove(readerCandidate);
                    await candidateConn.DisposeAsync().ConfigureAwait(false);

                    if (role == HostRole.Writer)
                    {
                        readerCandidates.Remove(readerCandidate);
                    }
                    else
                    {
                        Logger.LogInformation("Unable to determine host role for {readerCandidate}. Since failover mode is set to STRICT_READER and the host may be a writer, it will not be selected for reader failover.", readerCandidate.GetHostAndPort());
                    }
                }
                catch (DbException ex)
                {
                    Logger.LogInformation(ex, "Exception thrown when getting a reader candidate");
                    remainingReaders.Remove(readerCandidate);
                }
            }

            // Try the original writer, which may have been demoted to a reader
            if (originalWriter != null && DateTime.UtcNow <= failoverEndTime)
            {
                if (this.failoverMode == FailoverMode.StrictReader && isOriginalWriterStillWriter)
                {
                    continue;
                }

                try
                {
                    Logger.LogInformation("Trying the original writer {hostSpec} which may have been demoted to a reader", originalWriter);
                    DbConnection candidateConn = await this.pluginService.OpenConnection(originalWriter, this.props, this, true);
                    var role = this.pluginService.GetHostRole(candidateConn);

                    if (role == HostRole.Reader || this.failoverMode != FailoverMode.StrictReader)
                    {
                        var updatedHostSpec = new HostSpec(originalWriter.Host, originalWriter.Port, role, originalWriter.Availability);
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
                    }

                    await candidateConn.DisposeAsync().ConfigureAwait(false);

                    if (role == HostRole.Writer)
                    {
                        isOriginalWriterStillWriter = true;
                    }
                    else
                    {
                        Logger.LogInformation("Unable to determine host role for {originalWriter}. Since failover mode is set to STRICT_READER and the host may be a writer, it will not be selected for reader failover.", originalWriter.GetHostAndPort());
                    }
                }
                catch (DbException ex)
                {
                    // Continue to next iteration
                    Logger.LogInformation(ex, $"[Reader Failover] Failed to connect to host: {originalWriter.GetHostAndPort()}");
                }
            }
        }
        while (DateTime.UtcNow < failoverEndTime);

        throw new TimeoutException("Failover reader timeout");
    }

    private async Task FailoverWriterAsync()
    {
        // Force refresh host list and wait for topology to stabilize
        await this.pluginService.ForceRefreshHostListAsync(true, this.failoverTimeoutMs);

        var updatedHosts = this.pluginService.AllHosts;
        var writerCandidate = updatedHosts.FirstOrDefault(x => x.Role == HostRole.Writer);

        if (writerCandidate == null)
        {
            throw new FailoverFailedException("No writer host found in updated topology");
        }

        var allowedHosts = this.pluginService.GetHosts();
        if (!allowedHosts.Any(h => h.Host == writerCandidate.Host && h.Port == writerCandidate.Port))
        {
            throw new FailoverFailedException($"New writer {writerCandidate.Host}:{writerCandidate.Port} is not in allowed hosts list");
        }

        DbConnection writerCandidateConn;
        try
        {
            writerCandidateConn = await this.pluginService.OpenConnection(writerCandidate, this.props, this, true);
        }
        catch (Exception ex)
        {
            throw new FailoverFailedException($"Exception connecting to writer {writerCandidate.Host}", ex);
        }

        var role = this.pluginService.GetHostRole(writerCandidateConn);
        if (role != HostRole.Writer)
        {
            try
            {
                await writerCandidateConn.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Ignore close exception
            }

            throw new FailoverFailedException($"Unexpected role {role} for writer candidate {writerCandidate.Host}");
        }

        this.pluginService.SetCurrentConnection(writerCandidateConn, writerCandidate);
        this.ThrowFailoverSuccessException();
    }

    private void ThrowFailoverSuccessException()
    {
        Logger.LogTrace("Failover succeeded");

        if (this.shouldThrowTransactionError)
        {
            this.shouldThrowTransactionError = false;
            throw new TransactionStateUnknownException("Transaction resolution unknown. Please re-configure session state if required and try restarting transaction.");
        }

        throw new FailoverSuccessException("The active SQL connection has changed due to a connection failure. Please re-configure session state if required.");
    }

    private async Task InvalidateCurrentConnectionAsync()
    {
        Logger.LogTrace("Invalidating current connection...");
        try
        {
            if (this.pluginService.CurrentTransaction != null)
            {
                this.shouldThrowTransactionError = true;
                this.pluginService.CurrentTransaction = null;
            }
        }
        catch
        {
            // Swallow exception, current transaction should be useless anyway.
        }

        try
        {
            if (this.pluginService.CurrentConnection != null)
            {
                await this.pluginService.CurrentConnection.CloseAsync();
                Logger.LogTrace("Current connection {Type}@{Id} is closed.",
                    this.pluginService.CurrentConnection?.GetType().FullName,
                    RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection));
            }
        }
        catch (Exception ex)
        {
            // Swallow exception, current connection should be useless anyway.
            Logger.LogTrace("Error occoured when disposing current connection: {message}", ex.Message);
        }
    }

    private async Task PickNewConnectionAsync()
    {
        Logger.LogInformation("Picking a new connection.");
        if (this.isClosed && this.closedExplicitly)
        {
            Logger.LogInformation("Connection was closed explicitly. No failover will be performed.");
            return;
        }

        await this.FailoverAsync();
    }

    private bool ShouldExceptionTriggerConnectionSwitch(Exception exception)
    {
        if (!this.IsFailoverEnabled())
        {
            Logger.LogTrace("Cluster-aware failover is disabled.");
            return false;
        }

        if (this.skipFailoverOnInterruptedThread && Thread.CurrentThread.ThreadState == ThreadState.AbortRequested)
        {
            Logger.LogTrace("Do not start failover since the current thread is interrupted.");
            return false;
        }

        return this.pluginService.IsNetworkException(exception);
    }

    private bool CanDirectExecute(string methodName)
    {
        return methodName is MethodClose or MethodCloseAsync or MethodDispose;
    }

    private bool IsFailoverEnabled()
    {
        return this.rdsUrlType != RdsUrlType.RdsProxy && this.pluginService.AllHosts.Count > 0;
    }

    private FailoverMode GetFailoverMode()
    {
        var modeStr = PropertyDefinition.FailoverMode.GetString(this.props);
        if (string.IsNullOrEmpty(modeStr))
        {
            // Default based on connection type - this would need to be determined based on URL analysis
            return FailoverMode.StrictWriter;
        }

        return Enum.TryParse<FailoverMode>(modeStr, true, out var mode) ? mode : FailoverMode.StrictWriter;
    }
}
