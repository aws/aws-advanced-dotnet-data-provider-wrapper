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
    private Exception? lastExceptionDealtWith;
    private bool isInTransaction;

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>()
    {
        // Network-bound methods that might fail and trigger failover
        "DbConnection.Open",
        "DbConnection.OpenAsync",
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

        // Connection management methods
        MethodClose,
        MethodDispose,
        "DbConnection.Abort",

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

    public override T Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        if (this.pluginService.CurrentConnection != null
            && !this.CanDirectExecute(methodName)
            && !this.closedExplicitly
            && this.pluginService.CurrentConnection.State == ConnectionState.Closed)
        {
            this.PickNewConnection();
        }

        if (this.CanDirectExecute(methodName))
        {
            Logger.LogTrace("Direct executing...");
            return methodFunc();
        }

        if (this.isClosed && !this.AllowedOnClosedConnection(methodName))
        {
            this.InvalidInvocationOnClosedConnection();
        }

        try
        {
            Logger.LogTrace("Direct executing 2...");
            var result = methodFunc();
            Logger.LogTrace("Got result after direct executing 2");

            return result;
        }
        catch (Exception exception)
        {
            this.DealWithOriginalException(exception);
        }

        throw new UnreachableException("[FailoverPlugin] Should not reach here.");
    }

    private void InitFailoverMode()
    {
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);
        ArgumentNullException.ThrowIfNull(this.hostListProviderService.InitialConnectionHostSpec);

        this.rdsUrlType = RdsUtils.IdentifyRdsType(this.hostListProviderService.InitialConnectionHostSpec.Host);
    }

    public override DbConnection OpenConnection(HostSpec? hostSpec, Dictionary<string, string> properties, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc)
    {
        this.InitFailoverMode();

        if (!this.enableConnectFailover || hostSpec == null)
        {
            return this.auroraStaleDnsHelper.OpenVerifiedConnection(
                isInitialConnection,
                this.hostListProviderService!,
                hostSpec!,
                properties,
                methodFunc);
        }

        var hostSpecWithAvailability = this.pluginService.GetHosts()
            .FirstOrDefault(x => x.Host == hostSpec.Host && x.Port == hostSpec.Port);

        if (hostSpecWithAvailability is not { Availability: HostAvailability.Unavailable })
        {
            try
            {
                return this.auroraStaleDnsHelper.OpenVerifiedConnection(
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
                    this.Failover();
                }
                catch (FailoverSuccessException)
                {
                    return this.pluginService.CurrentConnection!;
                }
            }
        }
        else
        {
            try
            {
                this.pluginService.RefreshHostList();
                this.Failover();
            }
            catch (FailoverSuccessException)
            {
                return this.pluginService.CurrentConnection!;
            }
        }

        throw new InvalidOperationException("Unable to establish connection");
    }

    public override void InitHostProvider(string initialUrl, Dictionary<string, string> properties, IHostListProviderService initHostListProviderService, ADONetDelegate initHostProviderFunc)
    {
        this.hostListProviderService = initHostListProviderService;
        initHostProviderFunc();
    }

    private void InvalidInvocationOnClosedConnection()
    {
        if (!this.closedExplicitly)
        {
            this.isClosed = false;
            this.PickNewConnection();
            Logger.LogWarning("Connection was closed but not explicitly. Attempting to pick a new connection.");
            throw new FailoverSuccessException("The active connection has changed. Please re-configure session state if required.");
        }

        throw new InvalidOperationException("Connection is closed");
    }

    private bool AllowedOnClosedConnection(string methodName)
    {
        return this.pluginService.TargetConnectionDialect.GetAllowedOnConnectionMethodNames().Contains(methodName);
    }

    private void DealWithOriginalException(Exception originalException)
    {
        Logger.LogDebug("Processing exception: {ExceptionMessage}", originalException.Message);

        if (this.ShouldExceptionTriggerConnectionSwitch(originalException))
        {
            Logger.LogWarning("Exception triggers failover: {ExceptionMessage}", originalException.Message);

            if (this.lastExceptionDealtWith == originalException)
            {
                throw originalException;
            }

            this.InvalidateCurrentConnection();

            if (this.pluginService.CurrentHostSpec != null)
            {
                Logger.LogInformation("Marking host {Host} as unavailable.", this.pluginService.CurrentHostSpec.Host);
                this.pluginService.SetAvailability(this.pluginService.CurrentHostSpec.AsAliases(), HostAvailability.Unavailable);
            }

            this.PickNewConnection();
            this.lastExceptionDealtWith = originalException;
        }

        throw originalException;
    }

    private void Failover()
    {
        Logger.LogInformation("Initiating failover in mode: {FailoverMode}.", this.failoverMode);

        if (this.failoverMode == FailoverMode.StrictWriter)
        {
            this.FailoverWriter();
        }
        else
        {
            this.FailoverReader();
        }
    }

    private void FailoverReader()
    {
        Logger.LogInformation("Starting reader failover process.");
        this.pluginService.ForceRefreshHostList(false, 0);

        var result = this.GetReaderFailoverConnection(DateTime.UtcNow.AddMilliseconds(this.failoverTimeoutMs));
        Logger.LogInformation("Reader failover successful. Switching to host: {Host}.", result.HostSpec.Host);

        this.pluginService.SetCurrentConnection(result.Connection, result.HostSpec);
        this.ThrowFailoverSuccessException();
    }

    private ReaderFailoverResult GetReaderFailoverConnection(DateTime failoverEndTime)
    {
        var hosts = this.pluginService.GetHosts();
        var readerCandidates = hosts.Where(h => h.Role == HostRole.Reader).ToHashSet();
        var originalWriter = hosts.FirstOrDefault(h => h.Role == HostRole.Writer);
        bool isOriginalWriterStillWriter = false;

        do
        {
            // First, try all original readers
            var remainingReaders = new HashSet<HostSpec>(readerCandidates);
            while (remainingReaders.Count > 0 && DateTime.UtcNow < failoverEndTime)
            {
                HostSpec readerCandidate;
                try
                {
                    readerCandidate = this.pluginService.GetHostSpecByStrategy(HostRole.Reader, this.failoverReaderHostSelectorStrategy);
                    if (!remainingReaders.Contains(readerCandidate))
                    {
                        break;
                    }
                }
                catch (Exception)
                {
                    break;
                }

                try
                {
                    DbConnection candidateConn = this.pluginService.OpenConnection(readerCandidate, this.props, this);
                    var role = this.pluginService.GetHostRole(candidateConn);

                    if (role == HostRole.Reader || this.failoverMode != FailoverMode.StrictReader)
                    {
                        var updatedHostSpec = new HostSpec(readerCandidate.Host, readerCandidate.Port, role, readerCandidate.Availability);
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
                    }

                    // The role is Writer or Unknown, and we are in StrictReader mode
                    remainingReaders.Remove(readerCandidate);
                    candidateConn.Dispose();

                    if (role == HostRole.Writer)
                    {
                        readerCandidates.Remove(readerCandidate);
                    }
                }
                catch (Exception)
                {
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
                    DbConnection candidateConn = this.pluginService.OpenConnection(originalWriter, this.props, this);
                    var role = this.pluginService.GetHostRole(candidateConn);

                    if (role == HostRole.Reader || this.failoverMode != FailoverMode.StrictReader)
                    {
                        var updatedHostSpec = new HostSpec(originalWriter.Host, originalWriter.Port, role, originalWriter.Availability);
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
                    }

                    candidateConn.Dispose();

                    if (role == HostRole.Writer)
                    {
                        isOriginalWriterStillWriter = true;
                    }
                }
                catch (Exception)
                {
                    // Continue to next iteration
                }
            }
        }
        while (DateTime.UtcNow < failoverEndTime);

        throw new TimeoutException("Failover reader timeout");
    }

    private void FailoverWriter()
    {
        // Force refresh host list and wait for topology to stabilize
        this.pluginService.ForceRefreshHostList(true, this.failoverTimeoutMs);

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
            writerCandidateConn = this.pluginService.OpenConnection(writerCandidate, this.props, this);
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
                writerCandidateConn.Dispose();
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
        if (this.isInTransaction)
        {
            this.isInTransaction = false;
            throw new TransactionStateUnknownException("Transaction resolution unknown. Please re-configure session state if required and try restarting transaction.");
        }

        throw new FailoverSuccessException("The active SQL connection has changed due to a connection failure. Please re-configure session state if required.");
    }

    private void InvalidateCurrentConnection()
    {
        Logger.LogTrace("Invalidating current connection...");
        var conn = this.pluginService.CurrentConnection;
        if (conn == null)
        {
            return;
        }

        // TODO : handle when transaction support is added.

        // bool isInTransaction = false;
        //
        // if (this.pluginService.IsInTransaction())
        // {
        //     isInTransaction = true;
        //     try
        //     {
        //         conn.Rollback(); // if conn.Rollback() exists — see note below
        //     }
        //     catch
        //     {
        //         // Swallow exception
        //     }
        // }

        try
        {
            if (conn.State != ConnectionState.Closed)
            {
                conn.Dispose();
            }
        }
        catch (Exception ex)
        {
            // Swallow exception, current connection should be useless anyway.
            Logger.LogWarning("Error occoured when disposing current connection: {message}", ex.Message);
        }

        Logger.LogTrace("Current connection {Type}@{Id} is invalidated.", conn.GetType().FullName, RuntimeHelpers.GetHashCode(conn));
    }

    private void PickNewConnection()
    {
        Logger.LogInformation("Picking a new connection.");
        if (this.isClosed && this.closedExplicitly)
        {
            Logger.LogInformation("Connection was closed explicitly. No failover will be performed.");
            return;
        }

        this.Failover();
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
