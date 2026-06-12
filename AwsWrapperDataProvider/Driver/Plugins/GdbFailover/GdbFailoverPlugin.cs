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

using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.GdbFailover;

/// <summary>
/// Global Database Failover Plugin.
/// Extends <see cref="FailoverPlugin"/> with home-region-aware failover using 7 failover modes.
/// </summary>
public class GdbFailoverPlugin : FailoverPlugin
{
    private const string TelemetryFailover = "failover";

    private static readonly ILogger<GdbFailoverPlugin> Logger = LoggerUtils.GetLogger<GdbFailoverPlugin>();

    protected GlobalDbFailoverMode? activeHomeFailoverMode;
    protected GlobalDbFailoverMode? inactiveHomeFailoverMode;
    protected string? homeRegion;

    public GdbFailoverPlugin(IPluginService pluginService, Dictionary<string, string> props)
        : base(pluginService, props)
    {
    }

    protected override void InitFailoverMode()
    {
        if (this.rdsUrlType != null)
        {
            return;
        }

        ArgumentNullException.ThrowIfNull(this.hostListProviderService);
        ArgumentNullException.ThrowIfNull(this.hostListProviderService.InitialConnectionHostSpec);

        var initialHost = this.hostListProviderService.InitialConnectionHostSpec.Host;
        this.rdsUrlType = RdsUtils.IdentifyRdsType(initialHost);

        this.homeRegion = PropertyDefinition.FailoverHomeRegion.GetString(this.props);
        if (string.IsNullOrEmpty(this.homeRegion))
        {
            if (!this.rdsUrlType.HasRegion)
            {
                throw new InvalidOperationException(
                    Resources.GdbFailoverPlugin_InitFailoverMode_MissingHomeRegion);
            }

            this.homeRegion = RdsUtils.GetRdsRegion(initialHost);
            if (string.IsNullOrEmpty(this.homeRegion))
            {
                throw new InvalidOperationException(
                    Resources.GdbFailoverPlugin_InitFailoverMode_MissingHomeRegion);
            }
        }

        Logger.LogDebug(Resources.GdbFailoverPlugin_InitFailoverMode_FailoverHomeRegion, this.homeRegion);

        this.activeHomeFailoverMode = GlobalDbFailoverModeExtensions.FromValue(
            PropertyDefinition.ActiveHomeFailoverMode.GetString(this.props));
        this.inactiveHomeFailoverMode = GlobalDbFailoverModeExtensions.FromValue(
            PropertyDefinition.InactiveHomeFailoverMode.GetString(this.props));

        if (this.activeHomeFailoverMode == null)
        {
            this.activeHomeFailoverMode = this.rdsUrlType == RdsUrlType.RdsWriterCluster
                                          || this.rdsUrlType == RdsUrlType.RdsGlobalWriterCluster
                ? GlobalDbFailoverMode.StrictWriter
                : GlobalDbFailoverMode.HomeReaderOrWriter;
        }

        if (this.inactiveHomeFailoverMode == null)
        {
            this.inactiveHomeFailoverMode = this.rdsUrlType == RdsUrlType.RdsWriterCluster
                                            || this.rdsUrlType == RdsUrlType.RdsGlobalWriterCluster
                ? GlobalDbFailoverMode.StrictWriter
                : GlobalDbFailoverMode.HomeReaderOrWriter;
        }

        Logger.LogDebug(
            Resources.GdbFailoverPlugin_InitFailoverMode_FailoverModes,
            this.activeHomeFailoverMode,
            this.inactiveHomeFailoverMode);
    }

    protected override async Task FailoverAsync()
    {
        ITelemetryFactory telemetryFactory = this.pluginService.TelemetryFactory;
        ITelemetryContext telemetryContext = telemetryFactory.OpenTelemetryContext(
            TelemetryFailover, TelemetryTraceLevel.Nested);

        var failoverStartTime = DateTime.UtcNow;
        var failoverEndTime = failoverStartTime.AddMilliseconds(this.failoverTimeoutMs);

        try
        {
            Logger.LogInformation(Resources.GdbFailoverPlugin_FailoverAsync_StartingFailover);

            if (!await this.pluginService.ForceRefreshHostListAsync(true, this.failoverTimeoutMs))
            {
                // Let's assume it's a writer failover.
                this.writerFailoverTriggered.Inc();
                this.writerFailoverFailed.Inc();
                Logger.LogError(Resources.GdbFailoverPlugin_FailoverAsync_UnableToRefreshHostList);
                throw new FailoverFailedException(Resources.GdbFailoverPlugin_FailoverAsync_UnableToRefreshHostList);
            }

            var updatedHosts = this.pluginService.AllHosts;
            var writerCandidate = updatedHosts.FirstOrDefault(x => x.Role == HostRole.Writer);

            if (writerCandidate == null)
            {
                this.writerFailoverTriggered.Inc();
                this.writerFailoverFailed.Inc();
                var message = LoggerUtils.LogTopology(
                    updatedHosts, Resources.GdbFailoverPlugin_FailoverAsync_NoWriterFoundInTopology);
                Logger.LogError("{Message}", message);
                throw new FailoverFailedException(message);
            }

            // Determine writer region and select failover mode
            var writerRegion = RdsUtils.GetRdsRegion(writerCandidate.Host);
            var isHomeRegion = this.homeRegion!.Equals(writerRegion, StringComparison.OrdinalIgnoreCase);
            Logger.LogDebug(Resources.GdbFailoverPlugin_FailoverAsync_IsHomeRegion, isHomeRegion);

            var currentFailoverMode = isHomeRegion
                ? this.activeHomeFailoverMode!.Value
                : this.inactiveHomeFailoverMode!.Value;
            Logger.LogDebug(Resources.GdbFailoverPlugin_FailoverAsync_CurrentFailoverMode, currentFailoverMode);

            switch (currentFailoverMode)
            {
                case GlobalDbFailoverMode.StrictWriter:
                    await this.FailoverToWriter(failoverEndTime);
                    break;
                case GlobalDbFailoverMode.StrictHomeReader:
                    await this.FailoverToAllowedHost(
                        () => this.pluginService.GetHosts()
                            .Where(x => x.Role == HostRole.Reader
                                        && this.homeRegion.Equals(
                                            RdsUtils.GetRdsRegion(x.Host), StringComparison.OrdinalIgnoreCase))
                            .ToHashSet(),
                        HostRole.Reader,
                        failoverEndTime);
                    break;
                case GlobalDbFailoverMode.StrictOutOfHomeReader:
                    await this.FailoverToAllowedHost(
                        () => this.pluginService.GetHosts()
                            .Where(x => x.Role == HostRole.Reader
                                        && !this.homeRegion.Equals(
                                            RdsUtils.GetRdsRegion(x.Host), StringComparison.OrdinalIgnoreCase))
                            .ToHashSet(),
                        HostRole.Reader,
                        failoverEndTime);
                    break;
                case GlobalDbFailoverMode.StrictAnyReader:
                    await this.FailoverToAllowedHost(
                        () => this.pluginService.GetHosts()
                            .Where(x => x.Role == HostRole.Reader)
                            .ToHashSet(),
                        HostRole.Reader,
                        failoverEndTime);
                    break;
                case GlobalDbFailoverMode.HomeReaderOrWriter:
                    await this.FailoverToAllowedHost(
                        () => this.pluginService.GetHosts()
                            .Where(x => x.Role == HostRole.Writer
                                        || (x.Role == HostRole.Reader
                                            && this.homeRegion.Equals(
                                                RdsUtils.GetRdsRegion(x.Host), StringComparison.OrdinalIgnoreCase)))
                            .ToHashSet(),
                        null,
                        failoverEndTime);
                    break;
                case GlobalDbFailoverMode.OutOfHomeReaderOrWriter:
                    await this.FailoverToAllowedHost(
                        () => this.pluginService.GetHosts()
                            .Where(x => x.Role == HostRole.Writer
                                        || (x.Role == HostRole.Reader
                                            && !this.homeRegion.Equals(
                                                RdsUtils.GetRdsRegion(x.Host), StringComparison.OrdinalIgnoreCase)))
                            .ToHashSet(),
                        null,
                        failoverEndTime);
                    break;
                case GlobalDbFailoverMode.AnyReaderOrWriter:
                    await this.FailoverToAllowedHost(
                        () => this.pluginService.GetHosts().ToHashSet(),
                        null,
                        failoverEndTime);
                    break;
                default:
                    throw new NotSupportedException(
                        string.Format(
                            Resources.GdbFailoverPlugin_FailoverAsync_UnsupportedFailoverMode,
                            currentFailoverMode));
            }

            Logger.LogInformation(
                Resources.GdbFailoverPlugin_FailoverAsync_EstablishedConnection,
                this.pluginService.CurrentHostSpec);
            this.ThrowFailoverSuccessException();
        }
        catch (FailoverSuccessException ex)
        {
            telemetryContext.SetSuccess(true);
            telemetryContext.SetException(ex);
            throw;
        }
        catch (TransactionStateUnknownException ex)
        {
            // Failover succeeded but an open transaction was lost. From the
            // failover-span perspective the failover itself still succeeded.
            telemetryContext.SetSuccess(true);
            telemetryContext.SetException(ex);
            throw;
        }
        catch (Exception ex)
        {
            telemetryContext.SetSuccess(false);
            telemetryContext.SetException(ex);
            throw;
        }
        finally
        {
            Logger.LogTrace(
                Resources.GdbFailoverPlugin_FailoverAsync_FailoverElapsed,
                (long)(DateTime.UtcNow - failoverStartTime).TotalMilliseconds);

            telemetryContext.CloseContext();

            // PostCopy MUST come after CloseContext.
            if (this.telemetryFailoverAdditionalTopTrace)
            {
                telemetryFactory.PostCopy(telemetryContext, TelemetryTraceLevel.ForceTopLevel);
            }
        }
    }

    /// <summary>
    /// Failover to the current writer using a retry loop that re-evaluates the writer candidate
    /// on each iteration. This keeps trying until the failover timeout elapses.
    /// The retry is required because the initial topology returned by <c>ForceRefreshHostListAsync</c>
    /// may be stale when the dialect has not yet been confirmed (for example, when the very first connection attempt
    /// fails because the writer is unreachable). In that case the cached topology eventually
    /// contains the new writer and subsequent calls to <c>RefreshHostListAsync</c> pick it up.
    /// </summary>
    protected virtual async Task FailoverToWriter(DateTime failoverEndTime)
    {
        this.writerFailoverTriggered.Inc();

        FailoverResult? result = null;
        try
        {
            result = await this.GetAllowedFailoverConnectionAsync(
                () =>
                {
                    var allHosts = this.pluginService.AllHosts;
                    var writer = allHosts.FirstOrDefault(x => x.Role == HostRole.Writer);
                    if (writer == null)
                    {
                        return [];
                    }

                    var allowedHosts = this.pluginService.GetHosts();
                    if (!allowedHosts.Any(h => h.Host == writer.Host && h.Port == writer.Port))
                    {
                        // Expected during failover: AllHosts may briefly report a writer that GetHosts() has
                        // filtered out (allowed/blocked hosts, custom endpoints, stale topology, etc.). Returning
                        // an empty set signals the retry loop to refresh topology and try again.
                        Logger.LogTrace(
                            Resources.GdbFailoverPlugin_FailoverToWriter_NewWriterNotInAllowedHostsLog,
                            writer.Host,
                            LoggerUtils.LogTopology(allowedHosts, string.Empty));
                        return [];
                    }

                    return [writer];
                },
                HostRole.Writer,
                failoverEndTime);
            this.pluginService.SetCurrentConnection(result.Connection, result.HostSpec);
            Logger.LogInformation(
                Resources.GdbFailoverPlugin_FailoverToWriter_ConnectedToWriter,
                result.HostSpec.Host);
            result = null; // Prevent connection from being closed in finally block
            this.writerFailoverSuccess.Inc();
        }
        catch (TimeoutException)
        {
            this.writerFailoverFailed.Inc();
            var allHosts = this.pluginService.AllHosts;
            var writer = allHosts.FirstOrDefault(x => x.Role == HostRole.Writer);
            var writerHost = writer?.Host ?? string.Empty;
            Logger.LogError(
                Resources.GdbFailoverPlugin_FailoverToWriter_ExceptionConnectingToWriter,
                writerHost);
            throw new FailoverFailedException(
                string.Format(
                    Resources.GdbFailoverPlugin_FailoverToWriter_ExceptionConnectingToWriter,
                    writerHost));
        }
        catch (Exception)
        {
            this.writerFailoverFailed.Inc();
            throw;
        }
        finally
        {
            if (result != null && result.Connection != this.pluginService.CurrentConnection)
            {
                try
                {
                    await result.Connection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Ignore
                }
            }
        }
    }

    protected virtual async Task FailoverToAllowedHost(
        Func<HashSet<HostSpec>> allowedHostsSupplier,
        HostRole? verifyRole,
        DateTime failoverEndTime)
    {
        this.readerFailoverTriggered.Inc();

        FailoverResult? result = null;
        try
        {
            result = await this.GetAllowedFailoverConnectionAsync(
                allowedHostsSupplier, verifyRole, failoverEndTime);
            this.pluginService.SetCurrentConnection(result.Connection, result.HostSpec);
            result = null; // Prevent connection from being closed in finally block
            this.readerFailoverSuccess.Inc();
        }
        catch (TimeoutException)
        {
            this.readerFailoverFailed.Inc();
            Logger.LogError(Resources.GdbFailoverPlugin_FailoverToAllowedHost_UnableToConnectToReader);
            throw new FailoverFailedException(
                Resources.GdbFailoverPlugin_FailoverToAllowedHost_UnableToConnectToReader);
        }
        catch (Exception)
        {
            this.readerFailoverFailed.Inc();
            throw;
        }
        finally
        {
            if (result != null && result.Connection != this.pluginService.CurrentConnection)
            {
                try
                {
                    await result.Connection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Ignore
                }
            }
        }

        Logger.LogInformation(
            Resources.GdbFailoverPlugin_FailoverAsync_EstablishedConnection,
            this.pluginService.CurrentHostSpec);
    }

    protected virtual async Task<FailoverResult> GetAllowedFailoverConnectionAsync(
        Func<HashSet<HostSpec>> allowedHostsSupplier,
        HostRole? verifyRole,
        DateTime failoverEndTime)
    {
        do
        {
            await this.pluginService.RefreshHostListAsync();
            var updatedAllowedHosts = allowedHostsSupplier();

            // Make a copy with availability set to Available
            var remainingAllowedHosts = updatedAllowedHosts
                .Select(x => this.pluginService.HostSpecBuilder
                    .CopyFrom(x)
                    .WithAvailability(HostAvailability.Available)
                    .Build())
                .ToHashSet();

            if (remainingAllowedHosts.Count == 0)
            {
                await Task.Delay(100);
                continue;
            }

            while (remainingAllowedHosts.Count > 0 && DateTime.UtcNow < failoverEndTime)
            {
                HostSpec? candidateHost = null;
                try
                {
                    candidateHost = this.pluginService.GetHostSpecByStrategy(
                        remainingAllowedHosts.ToList(),
                        verifyRole ?? HostRole.Reader,
                        this.failoverReaderHostSelectorStrategy);
                }
                catch (Exception)
                {
                    // Strategy can't get a host according to requested conditions.
                }

                if (candidateHost == null)
                {
                    Logger.LogDebug(
                        Resources.GdbFailoverPlugin_GetAllowedFailoverConnectionAsync_NoCandidateFound,
                        verifyRole);
                    await Task.Delay(100);
                    break;
                }

                DbConnection? candidateConn = null;
                try
                {
                    candidateConn = await this.pluginService.OpenConnection(
                        candidateHost, this.props, this, true);

                    // Verify role if required
                    var role = verifyRole == null
                        ? (HostRole?)null
                        : await this.pluginService.GetHostRole(candidateConn);

                    if (verifyRole == null || verifyRole == role)
                    {
                        var updatedHostSpec = new HostSpec(candidateHost, role ?? candidateHost.Role);
                        return new FailoverResult(candidateConn, updatedHostSpec);
                    }

                    // Role mismatch
                    remainingAllowedHosts.Remove(candidateHost);
                    await candidateConn.DisposeAsync().ConfigureAwait(false);
                    candidateConn = null;
                }
                catch (Exception)
                {
                    remainingAllowedHosts.Remove(candidateHost);
                    if (candidateConn != null)
                    {
                        try
                        {
                            await candidateConn.DisposeAsync().ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // Ignore
                        }
                    }
                }
            }
        }
        while (DateTime.UtcNow < failoverEndTime);

        throw new TimeoutException(
            Resources.GdbFailoverPlugin_GetAllowedFailoverConnectionAsync_FailoverReaderTimeout);
    }
}
