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
using AwsWrapperDataProvider.Properties;
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
    private FailoverMode? failoverMode;

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
        this.failoverReaderHostSelectorStrategy = PropertyDefinition.ReaderHostSelectorStrategy.GetString(props)!;
        this.enableConnectFailover = PropertyDefinition.EnableConnectFailover.GetBoolean(props);
        this.skipFailoverOnInterruptedThread = PropertyDefinition.SkipFailoverOnInterruptedThread.GetBoolean(props);
        this.auroraStaleDnsHelper = new AuroraStaleDnsHelper(pluginService);
    }

    public override async Task<T> Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        Logger.LogDebug(Resources.FailoverPlugin_Execute_ExecuteCalled,
            methodName,
            this.isClosed,
            this.closedExplicitly);

        if (this.pluginService.CurrentConnection != null)
        {
            Logger.LogDebug(Resources.FailoverPlugin_Execute_CurrentConnectionState,
                this.pluginService.CurrentConnection.State,
                RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection),
                this.pluginService.CurrentConnection.DataSource);
        }

        if (this.pluginService.CurrentConnection != null
            && !this.CanDirectExecute(methodName)
            && !this.closedExplicitly
            && (this.pluginService.CurrentConnection.State == ConnectionState.Closed
            || this.pluginService.CurrentConnection.State == ConnectionState.Broken))
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
            await this.DealWithOriginalExceptionAsync(exception);
        }

        throw new UnreachableException(Resources.Error_FailoverPluginShouldNotReachHere);
    }

    private void InitFailoverMode()
    {
        if (this.rdsUrlType != null || this.failoverMode != null)
        {
            return;
        }

        ArgumentNullException.ThrowIfNull(this.hostListProviderService);
        ArgumentNullException.ThrowIfNull(this.hostListProviderService.InitialConnectionHostSpec);

        this.rdsUrlType = RdsUtils.IdentifyRdsType(this.hostListProviderService.InitialConnectionHostSpec.Host);

        var modeStr = PropertyDefinition.FailoverMode.GetString(this.props);
        if (Enum.TryParse<FailoverMode>(modeStr, true, out var mode))
        {
            this.failoverMode = mode;
        }
        else
        {
            this.failoverMode = this.rdsUrlType == RdsUrlType.RdsReaderCluster
                ? FailoverMode.ReaderOrWriter
                : FailoverMode.StrictWriter;
        }
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
                methodFunc,
                this);
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
                    methodFunc,
                    this);
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
                await this.pluginService.RefreshHostListAsync();
                await this.FailoverAsync();
            }
            catch (FailoverSuccessException)
            {
                connection = this.pluginService.CurrentConnection!;
            }
        }

        if (connection == null)
        {
            throw new InvalidOperationException(Resources.Error_FailedConnection);
        }

        if (isInitialConnection)
        {
            await this.pluginService.ForceRefreshHostListAsync(connection);
        }

        Logger.LogDebug(Resources.FailoverPlugin_OpenConnection_ReturningConnection,
            connection.State,
            connection.GetType().FullName,
            RuntimeHelpers.GetHashCode(connection),
            connection.DataSource);

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
        Logger.LogWarning(Resources.FailoverPlugin_InvalidInvocationOnClosedConnection_Called,
            this.closedExplicitly,
            this.isClosed);

        if (this.pluginService.CurrentConnection != null)
        {
            Logger.LogWarning(Resources.FailoverPlugin_InvalidInvocationOnClosedConnection_CurrentConnection,
                RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection),
                this.pluginService.CurrentConnection.State,
                this.pluginService.CurrentConnection.DataSource);
        }

        if (!this.closedExplicitly)
        {
            this.isClosed = false;
            Logger.LogWarning(Resources.FailoverPlugin_InvalidInvocationOnClosedConnection_AttemptingNewConnection);
            await this.PickNewConnectionAsync();
            throw new FailoverSuccessException(Resources.Error_FailoverSuccessActiveConnectionChanged);
        }

        throw new InvalidOperationException(Resources.Error_ConnectionIsClosed);
    }

    private bool AllowedOnClosedConnection(string methodName)
    {
        return this.pluginService.TargetConnectionDialect.GetAllowedOnConnectionMethodNames().Contains(methodName);
    }

    private async Task DealWithOriginalExceptionAsync(Exception originalException)
    {
        Logger.LogDebug(Resources.FailoverPlugin_DealWithOriginalExceptionAsync_ProcessingException, originalException.ToString());

        if (this.ShouldExceptionTriggerConnectionSwitch(originalException))
        {
            if (this.lastExceptionDealtWith == originalException)
            {
                throw originalException;
            }

            await this.InvalidateCurrentConnectionAsync();

            if (this.pluginService.CurrentHostSpec != null)
            {
                Logger.LogInformation(Resources.FailoverPlugin_DealWithOriginalExceptionAsync_MarkingHostUnavailable, this.pluginService.CurrentHostSpec.Host);
                this.pluginService.SetAvailability(this.pluginService.CurrentHostSpec.AsAliases(), HostAvailability.Unavailable);
            }

            await this.PickNewConnectionAsync();
            this.lastExceptionDealtWith = originalException;
        }

        throw originalException;
    }

    private async Task FailoverAsync()
    {
        Logger.LogInformation(Resources.FailoverPlugin_FailoverAsync_InitiatingFailover, this.failoverMode);

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
        Logger.LogInformation(Resources.FailoverPlugin_FailoverReaderAsync_StartingReaderFailover);
        await this.pluginService.ForceRefreshHostListAsync(false, 0);

        var result = await this.GetReaderFailoverConnectionAsync(DateTime.UtcNow.AddMilliseconds(this.failoverTimeoutMs));
        Logger.LogInformation(Resources.FailoverPlugin_FailoverReaderAsync_ReaderFailoverSuccessful, result.HostSpec.Host);

        this.pluginService.SetCurrentConnection(result.Connection, result.HostSpec);
        Logger.LogInformation(Resources.FailoverPlugin_FailoverReaderAsync_SetNewConnection,
            RuntimeHelpers.GetHashCode(result.Connection),
            result.Connection.State,
            result.Connection.DataSource,
            result.HostSpec.Host);
        this.ThrowFailoverSuccessException();
    }

    private async Task<ReaderFailoverResult> GetReaderFailoverConnectionAsync(DateTime failoverEndTime)
    {
        var hosts = this.pluginService.GetHosts();
        Logger.LogDebug(LoggerUtils.LogTopology(hosts, $"All hosts: "));
        var originalWriter = hosts.FirstOrDefault(h => h.Role == HostRole.Writer);
        bool isOriginalWriterStillWriter = false;

        do
        {
            // Update reader candidates, topology may have changed
            await this.pluginService.ForceRefreshHostListAsync(false, 10000);
            hosts = this.pluginService.GetHosts();
            hosts.ToList().ForEach(hostSpec => this.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Available));
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
                    Logger.LogInformation(ex, LoggerUtils.LogTopology([.. remainingReaders], string.Format(Resources.FailoverPlugin_GetReaderFailoverConnectionAsync_ErrorSelectingReaderHost, readerCandidate)));
                    break;
                }

                try
                {
                    DbConnection candidateConn = await this.pluginService.OpenConnection(readerCandidate, this.props, this, true);
                    var role = await this.pluginService.GetHostRole(candidateConn);

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
                        isOriginalWriterStillWriter = false;
                        readerCandidates.Remove(readerCandidate);
                    }
                    else
                    {
                        Logger.LogInformation(Resources.FailoverPlugin_GetReaderFailoverConnectionAsync_UnableToDetermineHostRole, readerCandidate.GetHostAndPort());
                    }
                }
                catch (DbException ex)
                {
                    Logger.LogInformation(ex, Resources.FailoverPlugin_GetReaderFailoverConnectionAsync_ExceptionGettingReaderCandidate);
                    remainingReaders.Remove(readerCandidate);
                }
            }

            // Try the original writer, which may have been demoted to a reader
            if (originalWriter != null && DateTime.UtcNow <= failoverEndTime)
            {
                if (this.failoverMode == FailoverMode.StrictReader && isOriginalWriterStillWriter)
                {
                    await Task.Delay(100);
                    continue;
                }

                try
                {
                    Logger.LogInformation(Resources.FailoverPlugin_GetReaderFailoverConnectionAsync_TryingOriginalWriter, originalWriter);
                    DbConnection candidateConn = await this.pluginService.OpenConnection(originalWriter, this.props, this, true);
                    var role = await this.pluginService.GetHostRole(candidateConn);

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
                        Logger.LogInformation(Resources.FailoverPlugin_GetReaderFailoverConnectionAsync_UnableToDetermineOriginalWriterRole, originalWriter.GetHostAndPort());
                    }
                }
                catch (DbException ex)
                {
                    // Continue to next iteration
                    Logger.LogInformation(ex, Resources.FailoverPlugin_GetReaderFailoverConnectionAsync_FailedToConnectToHost, originalWriter.GetHostAndPort());
                }
            }
        }
        while (DateTime.UtcNow < failoverEndTime);

        throw new FailoverFailedException(Resources.Error_FailoverReaderTimeout);
    }

    private async Task FailoverWriterAsync()
    {
        // Force refresh host list and wait for topology to stabilize
        await this.pluginService.ForceRefreshHostListAsync(true, this.failoverTimeoutMs);

        var updatedHosts = this.pluginService.AllHosts;
        var writerCandidate = updatedHosts.FirstOrDefault(x => x.Role == HostRole.Writer);

        if (writerCandidate == null)
        {
            throw new FailoverFailedException(Resources.Error_NoWriterHostFoundInUpdatedTopology);
        }

        var allowedHosts = this.pluginService.GetHosts();
        if (!allowedHosts.Any(h => h.Host == writerCandidate.Host && h.Port == writerCandidate.Port))
        {
            throw new FailoverFailedException(string.Format(Resources.Error_NewWriterNotInAllowedHostsList, writerCandidate.Host, writerCandidate.Port));
        }

        DbConnection writerCandidateConn;
        try
        {
            writerCandidateConn = await this.pluginService.OpenConnection(writerCandidate, this.props, this, true);
        }
        catch (Exception ex)
        {
            throw new FailoverFailedException(string.Format(Resources.Error_ExceptionConnectingToWriter, writerCandidate.Host), ex);
        }

        var role = await this.pluginService.GetHostRole(writerCandidateConn);
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

            throw new FailoverFailedException(string.Format(Resources.Error_UnexpectedRoleForWriterCandidate, writerCandidate.Host));
        }

        this.pluginService.SetCurrentConnection(writerCandidateConn, writerCandidate);
        Logger.LogInformation(Resources.FailoverPlugin_FailoverWriterAsync_SetNewConnection,
            RuntimeHelpers.GetHashCode(writerCandidateConn),
            writerCandidateConn.State,
            writerCandidate.Host);
        this.ThrowFailoverSuccessException();
    }

    private void ThrowFailoverSuccessException()
    {
        Logger.LogTrace(Resources.FailoverPlugin_ThrowFailoverSuccessException_FailoverSucceeded);

        if (this.pluginService.CurrentConnection != null)
        {
            Logger.LogDebug(Resources.FailoverPlugin_ThrowFailoverSuccessException_CurrentConnectionAfterFailover,
                RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection),
                this.pluginService.CurrentConnection.State,
                this.pluginService.CurrentConnection.DataSource);
        }

        if (this.shouldThrowTransactionError)
        {
            this.shouldThrowTransactionError = false;
            throw new TransactionStateUnknownException(Resources.Error_TransactionResolutionUnknown);
        }

        throw new FailoverSuccessException(Resources.Error_FailoverSuccessActiveConnectionChanged);
    }

    private async Task InvalidateCurrentConnectionAsync()
    {
        Logger.LogTrace(Resources.FailoverPlugin_InvalidateCurrentConnectionAsync_InvalidatingConnection);
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
                Logger.LogTrace(Resources.FailoverPlugin_InvalidateCurrentConnectionAsync_ConnectionClosed,
                    this.pluginService.CurrentConnection?.GetType().FullName,
                    RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection));
            }
        }
        catch (Exception ex)
        {
            // Swallow exception, current connection should be useless anyway.
            Logger.LogTrace(Resources.FailoverPlugin_InvalidateCurrentConnectionAsync_ErrorDisposingConnection, ex.Message);
        }
    }

    private async Task PickNewConnectionAsync()
    {
        Logger.LogInformation(Resources.FailoverPlugin_PickNewConnectionAsync_PickingNewConnection);
        if (this.isClosed && this.closedExplicitly)
        {
            Logger.LogInformation(Resources.FailoverPlugin_PickNewConnectionAsync_ConnectionClosedExplicitly);
            return;
        }

        await this.FailoverAsync();
    }

    private bool ShouldExceptionTriggerConnectionSwitch(Exception exception)
    {
        if (!this.IsFailoverEnabled())
        {
            Logger.LogTrace(Resources.FailoverPlugin_ShouldExceptionTriggerConnectionSwitch_FailoverDisabled);
            return false;
        }

        if (this.skipFailoverOnInterruptedThread && Thread.CurrentThread.ThreadState == ThreadState.AbortRequested)
        {
            Logger.LogTrace(Resources.FailoverPlugin_ShouldExceptionTriggerConnectionSwitch_ThreadInterrupted);
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
}
