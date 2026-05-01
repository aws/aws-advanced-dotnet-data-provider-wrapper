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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.GdbFailover;

/// <summary>
/// Global Database Failover Plugin.
/// Extends <see cref="FailoverPlugin"/> with home-region-aware failover using 7 failover modes.
/// </summary>
public class GdbFailoverPlugin : FailoverPlugin
{
    private static readonly ILogger<GdbFailoverPlugin> Logger = LoggerUtils.GetLogger<GdbFailoverPlugin>();

    protected GlobalDbFailoverMode? activeHomeFailoverMode;
    protected GlobalDbFailoverMode? inactiveHomeFailoverMode;
    protected string? homeRegion;
    private new RdsUrlType? rdsUrlType;

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
                    "FailoverHomeRegion is required when connecting via a global endpoint or IP address.");
            }

            this.homeRegion = RdsUtils.GetRdsRegion(initialHost);
            if (string.IsNullOrEmpty(this.homeRegion))
            {
                throw new InvalidOperationException(
                    "FailoverHomeRegion is required when connecting via a global endpoint or IP address.");
            }
        }

        Logger.LogDebug("GdbFailoverPlugin: failoverHomeRegion={HomeRegion}", this.homeRegion);

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
            "GdbFailoverPlugin: activeHomeFailoverMode={ActiveMode}, inactiveHomeFailoverMode={InactiveMode}",
            this.activeHomeFailoverMode,
            this.inactiveHomeFailoverMode);
    }

    protected override async Task FailoverAsync()
    {
        var failoverEndTime = DateTime.UtcNow.AddMilliseconds(this.failoverTimeoutMs);

        Logger.LogInformation("GdbFailoverPlugin: Starting GDB failover.");

        if (!await this.pluginService.ForceRefreshHostListAsync(true, this.failoverTimeoutMs))
        {
            Logger.LogError("GdbFailoverPlugin: Unable to refresh host list.");
            throw new FailoverFailedException("Unable to refresh host list.");
        }

        var updatedHosts = this.pluginService.AllHosts;
        var writerCandidate = updatedHosts.FirstOrDefault(x => x.Role == HostRole.Writer);

        if (writerCandidate == null)
        {
            var message = LoggerUtils.LogTopology(updatedHosts, "No writer host found in updated topology.");
            Logger.LogError("{Message}", message);
            throw new FailoverFailedException(message);
        }

        // Determine writer region and select failover mode
        var writerRegion = RdsUtils.GetRdsRegion(writerCandidate.Host);
        var isHomeRegion = this.homeRegion!.Equals(writerRegion, StringComparison.OrdinalIgnoreCase);
        Logger.LogDebug("GdbFailoverPlugin: isHomeRegion={IsHomeRegion}", isHomeRegion);

        var currentFailoverMode = isHomeRegion
            ? this.activeHomeFailoverMode!.Value
            : this.inactiveHomeFailoverMode!.Value;
        Logger.LogDebug("GdbFailoverPlugin: currentFailoverMode={FailoverMode}", currentFailoverMode);

        switch (currentFailoverMode)
        {
            case GlobalDbFailoverMode.StrictWriter:
                await this.FailoverToWriter(writerCandidate);
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
                throw new NotSupportedException($"Unsupported failover mode: {currentFailoverMode}");
        }

        Logger.LogInformation(
            "GdbFailoverPlugin: Established connection to {Host}.",
            this.pluginService.CurrentHostSpec);
        this.ThrowFailoverSuccessException();
    }

    protected virtual async Task FailoverToWriter(HostSpec writerCandidate)
    {
        var allowedHosts = this.pluginService.GetHosts();
        if (!allowedHosts.Any(h => h.Host == writerCandidate.Host && h.Port == writerCandidate.Port))
        {
            var topologyString = LoggerUtils.LogTopology(allowedHosts, string.Empty);
            Logger.LogError(
                "GdbFailoverPlugin: New writer {Host} is not in allowed hosts list. {Topology}",
                writerCandidate.Host,
                topologyString);
            throw new FailoverFailedException(
                $"New writer {writerCandidate.Host} is not in allowed hosts list.");
        }

        DbConnection writerCandidateConn;
        try
        {
            writerCandidateConn = await this.pluginService.OpenConnection(
                writerCandidate, this.props, this, true);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                "GdbFailoverPlugin: Exception connecting to writer {Host}.",
                writerCandidate.Host);
            throw new FailoverFailedException(
                $"Exception connecting to writer {writerCandidate.Host}.", ex);
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

            Logger.LogError(
                "GdbFailoverPlugin: Unexpected role {Role} for writer candidate {Host}.",
                role,
                writerCandidate.Host);
            throw new FailoverFailedException(
                $"Unexpected role {role} for writer candidate {writerCandidate.Host}.");
        }

        this.pluginService.SetCurrentConnection(writerCandidateConn, writerCandidate);
        Logger.LogInformation(
            "GdbFailoverPlugin: Connected to writer {Host}.",
            writerCandidate.Host);
    }

    protected virtual async Task FailoverToAllowedHost(
        Func<HashSet<HostSpec>> allowedHostsSupplier,
        HostRole? verifyRole,
        DateTime failoverEndTime)
    {
        ReaderFailoverResult? result = null;
        try
        {
            result = await this.GetAllowedFailoverConnectionAsync(
                allowedHostsSupplier, verifyRole, failoverEndTime);
            this.pluginService.SetCurrentConnection(result.Connection, result.HostSpec);
            result = null; // Prevent connection from being closed in finally block
        }
        catch (TimeoutException)
        {
            Logger.LogError("GdbFailoverPlugin: Unable to connect to reader within timeout.");
            throw new FailoverFailedException("Unable to connect to reader within timeout.");
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
            "GdbFailoverPlugin: Established connection to {Host}.",
            this.pluginService.CurrentHostSpec);
    }

    protected virtual async Task<ReaderFailoverResult> GetAllowedFailoverConnectionAsync(
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
                        "GdbFailoverPlugin: No candidate found for role {Role}. Retrying.",
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
                        return new ReaderFailoverResult(candidateConn, updatedHostSpec);
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

        throw new TimeoutException("Failover reader timeout.");
    }
}
