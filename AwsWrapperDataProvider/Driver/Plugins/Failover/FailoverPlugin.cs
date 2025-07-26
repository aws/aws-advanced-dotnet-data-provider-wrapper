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
    private const string MethodAbort = "DbConnection.Abort";
    private const string MethodClose = "DbConnection.Close";
    private const string MethodIsClosed = "DbConnection.IsClosed";
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
        "DbConnection.Close",
        "DbConnection.Dispose",
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
        this.failoverTimeoutMs = PropertyDefinition.FailoverTimeoutMs.GetInt(props) ?? 300000;
        this.failoverMode = this.GetFailoverMode();
        this.failoverReaderHostSelectorStrategy = PropertyDefinition.ReaderHostSelectorStrategy.GetString(props)!;
        this.enableConnectFailover = PropertyDefinition.EnableConnectFailover.GetBoolean(props);
        this.skipFailoverOnInterruptedThread = PropertyDefinition.SkipFailoverOnInterruptedThread.GetBoolean(props);
        this.auroraStaleDnsHelper = new AuroraStaleDnsHelper(pluginService);
    }

    public override T Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        Logger.LogDebug("Executing method {MethodName}.", methodName);

        if (this.pluginService.CurrentConnection != null
            && !this.CanDirectExecute(methodName)
            && !this.closedExplicitly
            && this.pluginService.CurrentConnection.State == ConnectionState.Closed)
        {
            Logger.LogWarning("Connection is closed. Picking a new connection.");
            this.PickNewConnection();
        }

        if (this.CanDirectExecute(methodName))
        {
            Logger.LogDebug("Direct execution allowed for method {MethodName}.", methodName);
            return methodFunc();
        }

        if (this.isClosed && !this.AllowedOnClosedConnection(methodName))
        {
            Logger.LogError("Invalid invocation of method {MethodName} on a closed connection.", methodName);
            this.InvalidInvocationOnClosedConnection();
        }

        try
        {
            return methodFunc();
        }
        catch (Exception exception)
        {
            Logger.LogError(exception, "Exception during execution of method {MethodName}.", methodName);
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

        if (hostSpecWithAvailability == null || hostSpecWithAvailability.Availability != HostAvailability.Unavailable)
        {
            try
            {
                var connection = methodFunc();
                if (isInitialConnection)
                {
                    this.pluginService.RefreshHostList(connection);
                }

                return connection;
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
                    DbConnection candidateConn = this.pluginService.OpenConnection(readerCandidate, this.props, false, this);
                    var role = this.pluginService.GetHostRole(candidateConn);

                    if (role == HostRole.Reader || this.failoverMode != FailoverMode.StrictReader)
                    {
                        var updatedHostSpec = new HostSpec(readerCandidate.Host, readerCandidate.Port, role, readerCandidate.Availability);
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
                    }

                    // The role is Writer or Unknown, and we are in StrictReader mode
                    remainingReaders.Remove(readerCandidate);
                    candidateConn.Close();

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
                    DbConnection candidateConn = this.pluginService.OpenConnection(originalWriter, this.props, false, this);
                    var role = this.pluginService.GetHostRole(candidateConn);

                    if (role == HostRole.Reader || this.failoverMode != FailoverMode.StrictReader)
                    {
                        var updatedHostSpec = new HostSpec(originalWriter.Host, originalWriter.Port, role, originalWriter.Availability);
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
                    }

                    candidateConn.Close();

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
            writerCandidateConn = this.pluginService.OpenConnection(writerCandidate, this.props, false, this);
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
                writerCandidateConn.Close();
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
                conn.Close();
            }
        }
        catch
        {
            // Swallow exception, current connection should be useless anyway.
        }
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
            return false;
        }

        if (this.skipFailoverOnInterruptedThread && Thread.CurrentThread.ThreadState == ThreadState.AbortRequested)
        {
            return false;
        }

        return this.pluginService.IsNetworkException(exception);
    }

    private bool CanDirectExecute(string methodName)
    {
        return methodName == MethodClose ||
               methodName == MethodIsClosed ||
               methodName == MethodAbort ||
               methodName == MethodDispose;
    }

    private bool IsFailoverEnabled()
    {
        // Check if this is an Aurora cluster URL type
        bool isAuroraCluster = this.rdsUrlType == RdsUrlType.RdsWriterCluster ||
                               this.rdsUrlType == RdsUrlType.RdsReaderCluster ||
                               this.rdsUrlType == RdsUrlType.RdsCustomCluster;

        if (!isAuroraCluster)
        {
            return false;
        }

        // For Aurora clusters, allow failover even if host list is not yet populated
        // This can happen during initial connection or if host discovery is still in progress
        return true;
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
