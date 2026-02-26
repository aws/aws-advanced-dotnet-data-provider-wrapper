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

using System.Collections.Concurrent;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Dialect;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Routing;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Properties;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Routing;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection;

public class BlueGreenStatusProvider
{
    private static readonly ILogger<BlueGreenStatusProvider> Logger = LoggerUtils.GetLogger<BlueGreenStatusProvider>();

    private static readonly string MonitoringPropertyPrefix = "blue-green-monitoring-";

    protected readonly HostSpecBuilder hostSpecBuilder = new();
    protected readonly BlueGreenStatusMonitor?[] monitors = new BlueGreenStatusMonitor?[2];
    protected int[] interimStatusHashes = { 0, 0 };
    protected int lastContextHash;
    protected BlueGreenInterimStatus?[] interimStatuses = { null, null };
    protected readonly ConcurrentDictionary<string, string?> hostIpAddresses = new();
    protected readonly ConcurrentDictionary<string, (HostSpec, HostSpec?)> correspondingNodes = new();
    protected readonly ConcurrentDictionary<string, BlueGreenRoleType> roleByHost = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> iamHostSuccessfulConnects = new();
    protected readonly ConcurrentDictionary<string, DateTime> greenNodeChangeNameTimes = new();
    protected BlueGreenStatus? summaryStatus;
    protected BlueGreenPhaseType? latestStatusPhase = BlueGreenPhaseType.NOT_CREATED;

    private int monitorResetOnInProgressCompleted; // 0 = false, 1 = true
    private int monitorResetOnTopologyCompleted;


    protected bool rollback;
    protected bool blueDnsUpdateCompleted;
    protected bool greenDnsRemoved;
    protected bool greenTopologyChanged;
    protected int allGreenNodesChangedName;
    protected long postStatusEndTimeNano;
    protected readonly object processStatusLock = new();

    protected readonly Dictionary<BlueGreenIntervalRate, long> statusCheckIntervalMap = new();
    protected readonly long switchoverTimeoutNano;

    protected readonly IPluginService pluginService;
    protected readonly Dictionary<string, string> props;
    protected readonly string bgdId;
    protected readonly string clusterId;
    protected ConcurrentDictionary<string, PhaseTimeInfo> phaseTimeNano = new();

    public BlueGreenStatusProvider(
        IPluginService pluginService,
        Dictionary<string, string> props,
        string bgdId,
        string clusterId)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.bgdId = bgdId;
        this.clusterId = clusterId;

        this.statusCheckIntervalMap[BlueGreenIntervalRate.BASELINE] =
            props.TryGetValue(PropertyDefinition.BgIntervalBaselineMs.Name, out var baseline)
                ? long.Parse(baseline)
                : long.Parse(PropertyDefinition.BgIntervalBaselineMs.DefaultValue!);
        this.statusCheckIntervalMap[BlueGreenIntervalRate.INCREASED] =
            props.TryGetValue(PropertyDefinition.BgIntervalIncreasedMs.Name, out var increased)
                ? long.Parse(increased)
                : long.Parse(PropertyDefinition.BgIntervalIncreasedMs.DefaultValue!);
        this.statusCheckIntervalMap[BlueGreenIntervalRate.HIGH] =
            !props.TryGetValue(PropertyDefinition.BgIntervalHighMs.Name, out var high)
                ? long.Parse(PropertyDefinition.BgIntervalHighMs.DefaultValue!) 
                : long.Parse(high);

        long switchoverTimeoutMs = props.TryGetValue(PropertyDefinition.BgSwitchoverTimeoutMs.Name, out var timeout)
            ? long.Parse(timeout)
            : long.Parse(PropertyDefinition.BgSwitchoverTimeoutMs.DefaultValue!);
        this.switchoverTimeoutNano = TimeSpan.FromMilliseconds(switchoverTimeoutMs).Ticks * 100;

        IDialect dialect = this.pluginService.Dialect;
        if (BlueGreenConnectionDialectHelper.IsBlueGreenConnectionDialect(dialect))
        {
            this.InitMonitoring();
        }
        else
        {
            Logger.LogWarning(Resources.BlueGreenStatusProvider_UnsupportedDialect, this.bgdId, dialect.GetType().Name);
        }
    }

    protected void InitMonitoring()
    {
        var sourceMonitor = new BlueGreenStatusMonitor(
            BlueGreenRoleType.SOURCE,
            this.bgdId,
            this.pluginService.CurrentHostSpec!,
            this.pluginService,
            this.GetMonitoringProperties(),
            this.statusCheckIntervalMap,
            this.PrepareStatus);
        this.monitors[(int)BlueGreenRoleType.SOURCE] = sourceMonitor;
        sourceMonitor.Start();

        var targetMonitor = new BlueGreenStatusMonitor(
            BlueGreenRoleType.TARGET,
            this.bgdId,
            this.pluginService.CurrentHostSpec!,
            this.pluginService,
            this.GetMonitoringProperties(),
            this.statusCheckIntervalMap,
            this.PrepareStatus);
        this.monitors[(int)BlueGreenRoleType.TARGET] = targetMonitor;
        targetMonitor.Start();
    }

    protected Dictionary<string, string> GetMonitoringProperties()
    {
        var monitoringConnProperties = new Dictionary<string, string>(this.props);

        foreach (var key in this.props.Keys.Where(p => p.StartsWith(MonitoringPropertyPrefix)).ToList())
        {
            var newKey = key[MonitoringPropertyPrefix.Length..];
            monitoringConnProperties[newKey] = this.props[key];
            monitoringConnProperties.Remove(key);
        }

        monitoringConnProperties[BlueGreenConnectionPlugin.BgSkipRoutingInForceConnect] = "true";

        return monitoringConnProperties;
    }

    protected void PrepareStatus(BlueGreenRoleType role, BlueGreenInterimStatus interimStatus)
    {
        Logger.LogInformation($"[bgdId: {this.bgdId}] PrepareStatus called for role: {role}, phase: {interimStatus.BlueGreenPhase}");

        lock (this.processStatusLock)
        {
            // Detect changes
            int statusHash = interimStatus.GetCustomHashCode();
            int contextHash = this.GetContextHash();
            if (this.interimStatusHashes[(int)role] == statusHash
                && this.lastContextHash == contextHash)
            {
                // no changes detected
                return;
            }

            // There are some changes detected. Let's update summary status.
            Logger.LogTrace(Resources.BlueGreenStatusProvider_PrepareStatus_InterimStatus, this.bgdId, role, interimStatus);

            this.UpdatePhase(role, interimStatus);

            // Store interimStatus and corresponding hash
            this.interimStatuses[(int)role] = interimStatus;
            this.interimStatusHashes[(int)role] = statusHash;
            this.lastContextHash = contextHash;

            // Update map of IP addresses.
            foreach (var kvp in interimStatus.StartIpAddressesByHostMap)
            {
                this.hostIpAddresses[kvp.Key] = kvp.Value;
            }

            // Update roleByHost based on provided host names.
            foreach (var hostName in interimStatus.HostNames)
            {
                this.roleByHost[hostName.ToLower()] = role;
            }

            this.UpdateCorrespondingNodes();
            this.UpdateSummaryStatus(role, interimStatus);
            this.UpdateMonitors();
            this.UpdateStatusCache();
            this.LogCurrentContext();

            // Log final switchover results.
            this.LogSwitchoverFinalSummary();

            this.ResetContextWhenCompleted();
        }
    }

    protected void UpdatePhase(BlueGreenRoleType role, BlueGreenInterimStatus interimStatus)
    {
        var roleStatus = this.interimStatuses[(int)role];
        var latestInterimPhase = roleStatus?.BlueGreenPhase ?? BlueGreenPhaseType.NOT_CREATED;

        if ((int)interimStatus.BlueGreenPhase < (int)latestInterimPhase)
        {
            this.rollback = true;
            Logger.LogTrace(Resources.BlueGreenStatusProvider_UpdatePhase_Rollback, this.bgdId);
        }

        // Do not allow status moves backward (unless it's rollback).
        // That could be caused by updating blue/green nodes delays.
        if (!this.rollback)
        {
            if ((int)interimStatus.BlueGreenPhase >= (int)this.latestStatusPhase)
            {
                this.latestStatusPhase = interimStatus.BlueGreenPhase;
            }
        }
        else
        {
            if ((int)interimStatus.BlueGreenPhase < (int)this.latestStatusPhase)
            {
                this.latestStatusPhase = interimStatus.BlueGreenPhase;
            }
        }
    }

    protected void UpdateStatusCache()
    {
        var latestStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
        BlueGreenConnectionCache.Instance.Set(this.bgdId, this.summaryStatus);
        this.StorePhaseTime(this.summaryStatus!.CurrentPhase);

        // Notify all waiting threads that status is updated.
        // Those waiting threads are waiting on an existing status so we need to notify on it.
        if (latestStatus != null)
        {
            lock (latestStatus)
            {
                Monitor.PulseAll(latestStatus);
            }
        }
    }

    /// <summary>
    /// Update corresponding nodes
    /// Blue writer node is mapped to a green writer node.
    /// Blue reader nodes are mapped to green reader nodes.
    /// </summary>
    protected void UpdateCorrespondingNodes()
    {
        this.correspondingNodes.Clear();

        var sourceInterimStatus = this.interimStatuses[(int)BlueGreenRoleType.SOURCE];
        var targetInterimStatus = this.interimStatuses[(int)BlueGreenRoleType.TARGET];

        if (sourceInterimStatus != null && sourceInterimStatus.StartTopology?.Any() == true
            && targetInterimStatus != null && targetInterimStatus.StartTopology?.Any() == true)
        {
            var blueWriterHostSpec = GetWriterHost(BlueGreenRoleType.SOURCE);
            var greenWriterHostSpec = GetWriterHost(BlueGreenRoleType.TARGET);
            var sortedBlueReaderHostSpecs = GetReaderHosts(BlueGreenRoleType.SOURCE);
            var sortedGreenReaderHostSpecs = GetReaderHosts(BlueGreenRoleType.TARGET);

            if (blueWriterHostSpec != null)
            {
                // greenWriterHostSpec can be null but that will be handled properly by corresponding routing.
                this.correspondingNodes[blueWriterHostSpec.Host] = (blueWriterHostSpec, greenWriterHostSpec);
            }

            if (sortedBlueReaderHostSpecs?.Any() == true)
            {
                // Map blue readers to green nodes.
                if (sortedGreenReaderHostSpecs?.Any() == true)
                {
                    int greenIndex = 0;
                    foreach (var blueHostSpec in sortedBlueReaderHostSpecs)
                    {
                        this.correspondingNodes[blueHostSpec.Host] = (blueHostSpec, sortedGreenReaderHostSpecs[greenIndex++]);
                        greenIndex %= sortedGreenReaderHostSpecs.Count;
                    }
                }
                else
                {
                    // There's no green reader nodes. We have to map all blue reader nodes to the green writer.
                    foreach (var blueHostSpec in sortedBlueReaderHostSpecs)
                    {
                        this.correspondingNodes[blueHostSpec.Host] = (blueHostSpec, greenWriterHostSpec);
                    }
                }
            }
        }

        bool? any = false;
        foreach (var name in sourceInterimStatus.HostNames)
        {
            any = true;
            break;
        }

        bool? any1 = false;
        foreach (var name in targetInterimStatus.HostNames)
        {
            any1 = true;
            break;
        }

        if (any == true && any1 == true)
        {
            var blueHosts = sourceInterimStatus.HostNames;
            var greenHosts = targetInterimStatus.HostNames;

            // Find corresponding cluster hosts
            var blueClusterHost = blueHosts.FirstOrDefault(RdsUtils.IsWriterClusterDns);
            var greenClusterHost = greenHosts.FirstOrDefault(RdsUtils.IsWriterClusterDns);

            if (!string.IsNullOrEmpty(blueClusterHost) && !string.IsNullOrEmpty(greenClusterHost))
            {
                this.correspondingNodes.TryAdd(blueClusterHost,
                    (this.hostSpecBuilder.WithHost(blueClusterHost).Build(), this.hostSpecBuilder.WithHost(greenClusterHost).Build()));
            }

            // Find corresponding cluster reader hosts
            var blueClusterReaderHost = blueHosts.FirstOrDefault(RdsUtils.IsReaderClusterDns);
            var greenClusterReaderHost = greenHosts.FirstOrDefault(RdsUtils.IsReaderClusterDns);

            if (!string.IsNullOrEmpty(blueClusterReaderHost)
                && !string.IsNullOrEmpty(greenClusterReaderHost))
            {
                this.correspondingNodes.TryAdd(blueClusterReaderHost,
                    (this.hostSpecBuilder.WithHost(blueClusterReaderHost).Build(), this.hostSpecBuilder.WithHost(greenClusterReaderHost).Build()));
            }

            foreach (var blueHost in blueHosts.Where(RdsUtils.IsRdsClusterDns))
            {
                var customClusterName = RdsUtils.GetRdsClusterId(blueHost);
                if (customClusterName != null)
                {
                    var greenHost = greenHosts
                        .FirstOrDefault(x => RdsUtils.IsRdsClusterDns(x)
                                             && customClusterName.Equals(
                                                 BlueGreenConnectionUtils.RemoveGreenInstancePrefix(RdsUtils.GetRdsClusterId(x))));

                    if (greenHost != null)
                    {
                        this.correspondingNodes.TryAdd(blueHost,
                            (this.hostSpecBuilder.WithHost(blueHost).Build(), this.hostSpecBuilder.WithHost(greenHost).Build()));
                    }
                }
            }
        }
    }

    protected HostSpec? GetWriterHost(BlueGreenRoleType role)
    {
        var interimStatus = this.interimStatuses[(int)role];
        return interimStatus?.StartTopology?.FirstOrDefault(x => x.Role == HostRole.Writer);
    }

    protected List<HostSpec>? GetReaderHosts(BlueGreenRoleType role)
    {
        var interimStatus = this.interimStatuses[(int)role];
        return interimStatus?.StartTopology?
            .Where(x => x.Role != HostRole.Writer)
            .OrderBy(x => x.Host)
            .ToList();
    }

    protected void UpdateSummaryStatus(BlueGreenRoleType role, BlueGreenInterimStatus interimStatus)
    {
        switch (this.latestStatusPhase)
        {
            case BlueGreenPhaseType.NOT_CREATED:
                this.summaryStatus = new BlueGreenStatus(this.bgdId, BlueGreenPhaseType.NOT_CREATED);
                break;
            case BlueGreenPhaseType.CREATED:
                this.UpdateDnsFlags(role, interimStatus);
                this.summaryStatus = this.GetStatusOfCreated();
                break;
            case BlueGreenPhaseType.PREPARATION:
                this.StartSwitchoverTimer();
                this.UpdateDnsFlags(role, interimStatus);
                this.summaryStatus = this.GetStatusOfPreparation();
                break;
            case BlueGreenPhaseType.IN_PROGRESS:
                this.UpdateDnsFlags(role, interimStatus);
                this.summaryStatus = this.GetStatusOfInProgress();
                this.ResetMonitors(this.monitorResetOnInProgressCompleted, "- start");
                break;
            case BlueGreenPhaseType.POST:
                this.UpdateDnsFlags(role, interimStatus);
                this.summaryStatus = this.GetStatusOfPost();
                break;
            case BlueGreenPhaseType.COMPLETED:
                this.UpdateDnsFlags(role, interimStatus);
                this.summaryStatus = this.GetStatusOfCompleted();
                break;
            default:
                throw new NotSupportedException(
                    string.Format(Resources.BlueGreenStatusProvider_UnknownPhase, this.bgdId, this.latestStatusPhase));
        }
    }

    protected void UpdateMonitors()
    {
        switch (this.summaryStatus!.CurrentPhase)
        {
            case BlueGreenPhaseType.NOT_CREATED:
                foreach (var monitor in this.monitors.Where(x => x != null))
                {
                    monitor!.SetIntervalRate(BlueGreenIntervalRate.BASELINE);
                    monitor.SetCollectIpAddresses(false);
                    monitor.SetCollectTopology(false);
                    monitor.SetUseIpAddress(false);
                }

                break;
            case BlueGreenPhaseType.CREATED:
                foreach (var monitor in this.monitors.Where(x => x != null))
                {
                    monitor!.SetIntervalRate(BlueGreenIntervalRate.INCREASED);
                    monitor.SetCollectIpAddresses(true);
                    monitor.SetCollectTopology(true);
                    monitor.SetUseIpAddress(false);
                    if (this.rollback)
                    {
                        monitor.ResetCollectedData();
                    }
                }
                break;
            case BlueGreenPhaseType.PREPARATION:
            case BlueGreenPhaseType.IN_PROGRESS:
            case BlueGreenPhaseType.POST:
                foreach (var monitor in this.monitors.Where(x => x != null))
                {
                    monitor!.SetIntervalRate(BlueGreenIntervalRate.HIGH);
                    monitor.SetCollectIpAddresses(false);
                    monitor.SetCollectTopology(false);
                    monitor.SetUseIpAddress(true);
                }
                break;
            case BlueGreenPhaseType.COMPLETED:
                foreach (var monitor in this.monitors.Where(x => x != null))
                {
                    monitor!.SetIntervalRate(BlueGreenIntervalRate.BASELINE);
                    monitor.SetCollectIpAddresses(false);
                    monitor.SetCollectTopology(false);
                    monitor.SetUseIpAddress(false);
                    monitor.ResetCollectedData();
                }

                // Stop monitoring old1 cluster/instance.
                if (!this.rollback && monitors[(int)BlueGreenRoleType.SOURCE] != null)
                {
                    monitors[(int)BlueGreenRoleType.SOURCE]!.SetStop(true);
                }
                break;
            default:
                throw new NotSupportedException(
                    string.Format(Resources.BlueGreenStatusProvider_UnknownPhase,
                        this.bgdId, this.summaryStatus.CurrentPhase));
        }
    }

    protected void UpdateDnsFlags(BlueGreenRoleType role, BlueGreenInterimStatus interimStatus)
    {
        if (role == BlueGreenRoleType.SOURCE && !this.blueDnsUpdateCompleted && interimStatus.AllStartTopologyIpChanged)
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_UpdateDnsFlags_BlueDnsCompleted, this.bgdId);
            this.blueDnsUpdateCompleted = true;
            this.StoreBlueDnsUpdateTime();
        }

        if (role == BlueGreenRoleType.TARGET && !this.greenDnsRemoved && interimStatus.AllStartTopologyEndpointsRemoved)
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_UpdateDnsFlags_GreenDnsRemoved, this.bgdId);
            this.greenDnsRemoved = true;
            this.StoreGreenDnsRemoveTime();
        }

        if (role == BlueGreenRoleType.TARGET && !this.greenTopologyChanged && interimStatus.AllTopologyChanged)
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_UpdateDnsFlags_GreenTopologyChanged, this.bgdId);
            this.greenTopologyChanged = true;
            this.StoreGreenTopologyChangeTime();
            this.ResetMonitors(this.monitorResetOnTopologyCompleted, "- green topology");
        }
    }
    
    protected void LogCurrentContext()
    {
        if (!Logger.IsEnabled(LogLevel.Trace))
        {
            // We can skip this log message if FINEST level is in effect
            // and more detailed message is going to be printed few lines below.
            Logger.LogDebug("[bgdId: '{0}'] BG status: {1}",
                this.bgdId,
                this.summaryStatus == null || this.summaryStatus.CurrentPhase == null
                    ? "<null>"
                    : this.summaryStatus.CurrentPhase.ToString());
        }

        Logger.LogTrace("[bgdId: '{0}'] Summary status:\n{1}",
            this.bgdId,
            this.summaryStatus?.ToString() ?? "<null>");

        Logger.LogTrace("Corresponding nodes:\n{0}",
            string.Join("\n", this.correspondingNodes
                .Select(x => $"   {x.Key} -> {(x.Value.Item2 == null ? "<null>" : x.Value.Item2.GetHostAndPort())}")));

        Logger.LogTrace("Phase times:\n{0}",
            string.Join("\n", this.phaseTimeNano
                .Select(x => $"   {x.Key} -> {x.Value.Timestamp}")));

        Logger.LogTrace("Green node certificate change times:\n{0}",
            string.Join("\n", this.greenNodeChangeNameTimes
                .Select(x => $"   {x.Key} -> {x.Value}")));

        Logger.LogTrace("\n" +
                        "   latestStatusPhase: {0}\n" +
                        "   blueDnsUpdateCompleted: {1}\n" +
                        "   greenDnsRemoved: {2}\n" +
                        "   greenNodeChangedName: {3}\n" +
                        "   greenTopologyChanged: {4}",
            this.latestStatusPhase,
            this.blueDnsUpdateCompleted,
            this.greenDnsRemoved,
            Interlocked.CompareExchange(ref this.allGreenNodesChangedName, 0, 0) == 1,
            this.greenTopologyChanged);
    }


    protected void ResetMonitors(int monitorResetCompleted, string eventName)
    {
        if (Interlocked.CompareExchange(ref monitorResetCompleted, 1, 0) == 0)
        {
            var blueEndpoints = this.summaryStatus!.RoleByHost
                .Where(x => x.Value == BlueGreenRoleType.SOURCE)
                .Select(x => x.Key)
                .ToHashSet();

            // TODO: Reset monitors with blueEndpoints
            this.StoreMonitorResetTime(eventName);
        }
    }

    // New connect requests: go to blue or green nodes; default behaviour; no routing
    // Existing connections: default behaviour; no action
    // Execute calls: default behaviour; no action
    protected BlueGreenStatus GetStatusOfCreated()
    {
        return new BlueGreenStatus(
            this.bgdId,
            BlueGreenPhaseType.CREATED,
            [],
            [],
            this.roleByHost,
            this.correspondingNodes);
    }

    /// <summary>
    /// New connect requests to blue: route to corresponding IP address.
    /// New connect requests to green: route to corresponding IP address
    /// New connect requests with IP address: default behaviour; no routing
    /// Existing connections: default behaviour; no action
    /// Execute calls: default behaviour; no action
    /// </summary>
    /// <returns>Blue/Green status.</returns>
    protected BlueGreenStatus GetStatusOfPreparation()
    {
        // We want to limit switchover duration to DEFAULT_POST_STATUS_DURATION_NANO.
        if (this.IsSwitchoverTimerExpired())
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_SwitchoverTimeout);
            if (this.rollback)
            {
                return this.GetStatusOfCreated();
            }

            return this.GetStatusOfCompleted();
        }

        var connectRouting = this.AddSubstituteBlueWithIpAddressConnectRouting();

        return new BlueGreenStatus(
            this.bgdId,
            BlueGreenPhaseType.PREPARATION,
            connectRouting,
            [],
            this.roleByHost,
            this.correspondingNodes);
    }

    protected List<IConnectRouting> AddSubstituteBlueWithIpAddressConnectRouting()
    {
        var connectRouting = new List<IConnectRouting>();

        foreach (var entry in this.roleByHost)
        {
            string host = entry.Key;
            BlueGreenRoleType role = entry.Value;
            (HostSpec, HostSpec?) nodePair = this.correspondingNodes.GetValueOrDefault(host);

            if (role != BlueGreenRoleType.SOURCE || nodePair.Item2 == null)
            {
                continue;
            }

            HostSpec blueHostSpec = nodePair.Item1;
            string? blueIp = this.hostIpAddresses.GetValueOrDefault(blueHostSpec.Host);
            HostSpec blueIpHostSpec;

            if (string.IsNullOrEmpty(blueIp))
            {
                blueIpHostSpec = blueHostSpec;
            }
            else
            {
                blueIpHostSpec = this.hostSpecBuilder.CopyFrom(blueHostSpec).WithHost(blueIp).Build();
            }

            connectRouting.Add(new SubstituteConnectRouting(
                host,
                role,
                blueIpHostSpec,
                [blueHostSpec],
                null));

            BlueGreenInterimStatus? interimStatus = this.interimStatuses[(int)role];
            if (interimStatus == null)
            {
                continue;
            }

            connectRouting.Add(new SubstituteConnectRouting(
                this.GetHostAndPort(host, interimStatus.Port),
                role,
                blueIpHostSpec,
                [blueHostSpec],
                null));
        }

        return connectRouting;
    }

    /// <summary>
    /// New connect requests to blue: suspend or route to corresponding IP address (depending on settings).
    /// New connect requests to green: suspend
    /// New connect requests with IP address: suspend
    /// Existing connections: default behaviour; no action
    /// Execute calls: suspend
    /// </summary>
    /// <returns>Blue/Green status.</returns>
    protected BlueGreenStatus GetStatusOfInProgress()
    {
        if (this.IsSwitchoverTimerExpired())
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_SwitchoverTimeout);
            if (this.rollback)
            {
                return this.GetStatusOfCreated();
            }
            return this.GetStatusOfCompleted();
        }

        var connectRouting = new List<IConnectRouting>
        {
            new SuspendConnectRouting(null, BlueGreenRoleType.SOURCE, this.bgdId),
            new SuspendConnectRouting(null, BlueGreenRoleType.TARGET, this.bgdId),
        };

        // All connect calls with IP address that belongs to blue or green node should be suspended.
        foreach (var ipAddress in this.hostIpAddresses.Values
            .Where(ip => !string.IsNullOrEmpty(ip))
            .Distinct()!)
        {
            BlueGreenInterimStatus? interimStatus;

            // Try to confirm that the ipAddress belongs to one of the blue nodes
            interimStatus = this.interimStatuses[(int)BlueGreenRoleType.SOURCE];
            if (interimStatus != null)
            {
                if (interimStatus.StartIpAddressesByHostMap.Values
                    .Any(x => !string.IsNullOrEmpty(x) && x.Equals(ipAddress)))
                {
                    connectRouting.Add(new SuspendConnectRouting(ipAddress, null, this.bgdId));
                    connectRouting.Add(new SuspendConnectRouting(
                        this.GetHostAndPort(ipAddress, interimStatus.Port),
                        null,
                        this.bgdId));
                    continue;
                }
            }

            // Try to confirm that the ipAddress belongs to one of the green nodes
            interimStatus = this.interimStatuses[(int)BlueGreenRoleType.TARGET];
            if (interimStatus != null)
            {
                if (interimStatus.StartIpAddressesByHostMap.Values
                    .Any(x => !string.IsNullOrEmpty(x) && x.Equals(ipAddress)))
                {
                    connectRouting.Add(new SuspendConnectRouting(ipAddress, null, this.bgdId));
                    connectRouting.Add(new SuspendConnectRouting(
                        this.GetHostAndPort(ipAddress, interimStatus.Port),
                        null,
                        this.bgdId));
                }
            }
        }

        // All blue and green traffic should be on hold.
        var executeRouting = new List<IExecuteRouting>
        {
            new SuspendExecuteRouting(null, BlueGreenRoleType.SOURCE, this.bgdId),
            new SuspendExecuteRouting(null, BlueGreenRoleType.TARGET, this.bgdId),
        };

        // All traffic through connections with IP addresses that belong to blue or green nodes should be on hold.
        foreach (var ipAddress in this.hostIpAddresses.Values
            .Where(ip => !string.IsNullOrEmpty(ip))
            .Distinct()!)
        {
            // Try to confirm that the ipAddress belongs to one of the blue nodes
            var interimStatus = this.interimStatuses[(int)BlueGreenRoleType.SOURCE];
            if (interimStatus != null)
            {
                if (interimStatus.StartIpAddressesByHostMap.Values
                    .Any(x => !string.IsNullOrEmpty(x) && x.Equals(ipAddress)))
                {
                    executeRouting.Add(new SuspendExecuteRouting(ipAddress, null, this.bgdId));
                    executeRouting.Add(new SuspendExecuteRouting(
                        this.GetHostAndPort(ipAddress, interimStatus.Port),
                        null,
                        this.bgdId));
                    continue;
                }
            }

            // Try to confirm that the ipAddress belongs to one of the green nodes
            interimStatus = this.interimStatuses[(int)BlueGreenRoleType.TARGET];
            if (interimStatus != null)
            {
                if (interimStatus.StartIpAddressesByHostMap.Values
                    .Any(x => !string.IsNullOrEmpty(x) && x.Equals(ipAddress)))
                {
                    executeRouting.Add(new SuspendExecuteRouting(ipAddress, null, this.bgdId));
                    executeRouting.Add(new SuspendExecuteRouting(
                        this.GetHostAndPort(ipAddress, interimStatus.Port),
                        null,
                        this.bgdId));
                    continue;
                }
            }

            executeRouting.Add(new SuspendExecuteRouting(ipAddress, null, this.bgdId));
        }

        return new BlueGreenStatus(
            this.bgdId,
            BlueGreenPhaseType.IN_PROGRESS,
            connectRouting,
            executeRouting,
            this.roleByHost,
            this.correspondingNodes);
    }

    protected BlueGreenStatus GetStatusOfPost()
    {
        if (this.IsSwitchoverTimerExpired())
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_SwitchoverTimeout);
            if (this.rollback)
            {
                return this.GetStatusOfCreated();
            }

            return this.GetStatusOfCompleted();
        }

        var connectRouting = new List<IConnectRouting>();
        var executeRouting = new List<IExecuteRouting>();
        this.CreatePostRouting(connectRouting);

        return new BlueGreenStatus(
            this.bgdId,
            BlueGreenPhaseType.POST,
            connectRouting,
            executeRouting,
            this.roleByHost,
            this.correspondingNodes);
    }

    protected void CreatePostRouting(List<IConnectRouting> connectRouting)
    {
        if (!this.blueDnsUpdateCompleted || Interlocked.CompareExchange(ref this.allGreenNodesChangedName, 0, 0) == 0)
        {
            // New connect calls to blue nodes should be routed to green nodes.
            foreach (var x in this.roleByHost.Where(x => x.Value == BlueGreenRoleType.SOURCE && this.correspondingNodes.ContainsKey(x.Key)))
            {
                string blueHost = x.Key;
                bool isBlueHostInstance = RdsUtils.IsRdsInstance(blueHost);

                var nodePair = this.correspondingNodes.GetValueOrDefault(x.Key);
                HostSpec? blueHostSpec = nodePair.Item1;
                HostSpec? greenHostSpec = nodePair.Item2;

                if (greenHostSpec == null)
                {
                    // A corresponding host is not found. We need to suspend this call.
                    connectRouting.Add(new SuspendUntilCorrespondingNodeFoundConnectRouting(blueHost, x.Value, this.bgdId));

                    var interimStatus = this.interimStatuses[(int)x.Value];
                    if (interimStatus != null)
                    {
                        connectRouting.Add(new SuspendUntilCorrespondingNodeFoundConnectRouting(
                            this.GetHostAndPort(blueHost, interimStatus.Port), x.Value, this.bgdId));
                    }
                }
                else
                {
                    string greenHost = greenHostSpec.Host;
                    string? greenIp = this.hostIpAddresses.GetValueOrDefault(greenHostSpec.Host);
                    HostSpec greenHostSpecWithIp = string.IsNullOrEmpty(greenIp)
                        ? greenHostSpec
                        : this.hostSpecBuilder.CopyFrom(greenHostSpec).WithHost(greenIp).Build();

                    // Check whether green host is already been connected with blue (no-prefixes) IAM host name.
                    List<HostSpec> iamHosts;
                    if (this.IsAlreadySuccessfullyConnected(greenHost, blueHost))
                    {
                        // Green node has already changed its name, and it's not a new blue node (no prefixes).
                        iamHosts = [blueHostSpec];
                    }
                    else
                    {
                        // Green node isn't yet changed its name, so we need to try both possible IAM host options.
                        iamHosts = [greenHostSpec, blueHostSpec];
                    }

                    connectRouting.Add(new SubstituteConnectRouting(
                        blueHost,
                        x.Value,
                        greenHostSpecWithIp,
                        iamHosts,
                        isBlueHostInstance ? (iamHost) => this.RegisterIamHost(greenHost, iamHost) : null));

                    var interimStatus = this.interimStatuses[(int)x.Value];
                    if (interimStatus != null)
                    {
                        connectRouting.Add(new SubstituteConnectRouting(
                            this.GetHostAndPort(blueHost, interimStatus.Port),
                            x.Value,
                            greenHostSpecWithIp,
                            iamHosts,
                            isBlueHostInstance ? (iamHost) => this.RegisterIamHost(greenHost, iamHost) : null));
                    }
                }
            }
        }

        if (!this.greenTopologyChanged)
        {
            // Green topology is not yet updated so different plugins may be misled and try to connect to green endpoint.
            // Reroute green endpoints to IP addresses
            foreach ((string greenHost, BlueGreenRoleType value) in this.roleByHost.Where(x => x.Value == BlueGreenRoleType.TARGET))
            {
                bool isGreenHostInstance = RdsUtils.IsRdsInstance(greenHost);
                string blueHost = BlueGreenConnectionUtils.RemoveGreenInstancePrefix(greenHost);
                var interimStatus = this.interimStatuses[(int)value];

                HostSpec blueHostSpec = interimStatus == null
                    ? this.hostSpecBuilder.WithHost(blueHost).Build()
                    : this.hostSpecBuilder.WithHost(blueHost).WithPort(interimStatus.Port).Build();
                HostSpec greenHostSpec = interimStatus == null
                    ? this.hostSpecBuilder.WithHost(greenHost).Build()
                    : this.hostSpecBuilder.WithHost(greenHost).WithPort(interimStatus.Port).Build();

                string? greenIp = this.hostIpAddresses.GetValueOrDefault(greenHost);
                HostSpec greenHostSpecWithIp = string.IsNullOrEmpty(greenIp)
                    ? this.hostSpecBuilder.WithHost(greenHost).Build()
                    : (interimStatus == null
                        ? this.hostSpecBuilder.WithHost(greenIp).Build()
                        : this.hostSpecBuilder.WithHost(greenIp).WithPort(interimStatus.Port).Build());

                // Check if the green host has connected to the blue (no-prefixes) IAM host name.
                List<HostSpec> iamHosts;
                if (this.IsAlreadySuccessfullyConnected(greenHost, blueHost))
                {
                    // Green node has already changed its name, and it's a new blue node (no prefixes).
                    iamHosts = [blueHostSpec];
                }
                else
                {
                    // Green node has not changed its name, so we need to try both possible IAM host options.
                    iamHosts = [greenHostSpec, blueHostSpec];
                }

                connectRouting.Add(new SubstituteConnectRouting(
                    greenHost,
                    value,
                    greenHostSpecWithIp,
                    iamHosts,
                    isGreenHostInstance ? (iamHost) => this.RegisterIamHost(greenHost, iamHost) : null));

                if (interimStatus != null)
                {
                    connectRouting.Add(new SubstituteConnectRouting(
                        this.GetHostAndPort(greenHost, interimStatus.Port),
                        value,
                        greenHostSpecWithIp,
                        iamHosts,
                        isGreenHostInstance ? (iamHost) => this.RegisterIamHost(greenHost, iamHost) : null));
                }
            }
        }
        else if (!this.greenDnsRemoved)
        {
            // Green topology has already changed.
            // New connect calls to green endpoints should be rejected.
            connectRouting.Add(new RejectConnectRouting(null, BlueGreenRoleType.TARGET));
        }
    }

    protected BlueGreenStatus GetStatusOfCompleted()
    {
        if (this.IsSwitchoverTimerExpired())
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_SwitchoverTimeout);
            if (this.rollback)
            {
                return this.GetStatusOfCreated();
            }

            return new BlueGreenStatus(
                this.bgdId,
                BlueGreenPhaseType.COMPLETED,
                [],
                [],
                this.roleByHost,
                this.correspondingNodes);
        }

        // BGD reports that it's completed but DNS hasn't yet updated completely.
        // Pretend that status isn't (yet) completed.
        if (!this.blueDnsUpdateCompleted || !this.greenDnsRemoved)
        {
            return this.GetStatusOfPost();
        }

        return new BlueGreenStatus(
            this.bgdId,
            BlueGreenPhaseType.COMPLETED,
            [],
            [],
            this.roleByHost,
            new Dictionary<string, (HostSpec, HostSpec?)>());
    }

    protected void RegisterIamHost(string? connectHost, string iamHost)
    {
        bool differentNodeNames = connectHost != null && !connectHost.Equals(iamHost);

        if (differentNodeNames)
        {
            if (!this.IsAlreadySuccessfullyConnected(connectHost!, iamHost))
            {
                this.greenNodeChangeNameTimes.TryAdd(connectHost, DateTime.UtcNow);
                Logger.LogTrace(Resources.BlueGreenStatusProvider_RegisterIamHost_GreenNodeChangedName, connectHost, iamHost);
            }
        }

        if (connectHost != null)
        {
            this.iamHostSuccessfulConnects
                .GetOrAdd(connectHost, _ => new ConcurrentDictionary<string, byte>())
                .TryAdd(iamHost, 0);
        }

        if (!differentNodeNames)
        {
            return;
        }

        // Check all IAM host changed their names
        bool allHostChangedNames = this.iamHostSuccessfulConnects
            .Where(x => x.Value.Count > 0)
            .All(x => x.Value.Keys.Any(y => !x.Key.Equals(y)));

        if (!allHostChangedNames || Interlocked.CompareExchange(ref this.allGreenNodesChangedName, 0, 0) != 0)
        {
            return;
        }

        Logger.LogTrace("allGreenNodesChangedName: true");
        Interlocked.Exchange(ref this.allGreenNodesChangedName, 1);
        this.StoreGreenNodeChangeNameTime();
    }

    protected bool IsAlreadySuccessfullyConnected(string connectHost, string iamHost)
    {
        return this.iamHostSuccessfulConnects
            .GetOrAdd(connectHost, _ => new ConcurrentDictionary<string, byte>())
            .ContainsKey(iamHost);
    }

    protected void ResetContextWhenCompleted()
    {
        BlueGreenStatus blueGreenStatus = this.summaryStatus;
        bool switchoverCompleted =
            blueGreenStatus != null && ((!this.rollback && blueGreenStatus!.CurrentPhase == BlueGreenPhaseType.COMPLETED)
                                        || (this.rollback && blueGreenStatus.CurrentPhase == BlueGreenPhaseType.CREATED));

        bool hasActiveSwitchoverPhases = this.phaseTimeNano
            .Any(x => x.Value.Phase != null && ((BlueGreenPhaseType)x.Value.Phase).IsActiveSwitchoverOrCompleted());

        if (switchoverCompleted && hasActiveSwitchoverPhases)
        {
            Logger.LogTrace(Resources.BlueGreenStatusProvider_ResetContext);
            this.rollback = false;
            this.summaryStatus = null;
            this.latestStatusPhase = BlueGreenPhaseType.NOT_CREATED;
            this.phaseTimeNano.Clear();
            this.blueDnsUpdateCompleted = false;
            this.greenDnsRemoved = false;
            this.greenTopologyChanged = false;
            Interlocked.Exchange(ref this.allGreenNodesChangedName, 0);
            this.postStatusEndTimeNano = 0;
            this.interimStatusHashes = [0, 0];
            this.lastContextHash = 0;
            this.interimStatuses = [null, null];
            this.hostIpAddresses.Clear();
            this.correspondingNodes.Clear();
            this.roleByHost.Clear();
            this.iamHostSuccessfulConnects.Clear();
            this.greenNodeChangeNameTimes.Clear();
            Interlocked.Exchange(ref this.monitorResetOnInProgressCompleted, 0);
            Interlocked.Exchange(ref this.monitorResetOnTopologyCompleted, 0);
        }
    }

    protected void StartSwitchoverTimer()
    {
        if (this.postStatusEndTimeNano == 0)
        {
            this.postStatusEndTimeNano = this.GetNanoTime() + this.switchoverTimeoutNano;
        }
    }

    protected bool IsSwitchoverTimerExpired()
    {
        return this.postStatusEndTimeNano > 0 && this.postStatusEndTimeNano < this.GetNanoTime();
    }



    protected int GetContextHash()
    {
        int result = this.GetValueHash(1, Interlocked.CompareExchange(ref this.allGreenNodesChangedName, 0, 0).ToString());
        result = this.GetValueHash(result, this.iamHostSuccessfulConnects.Count.ToString());
        return result;
    }
    
    protected int GetValueHash(int currentHash, string val)
    {
        return currentHash * 31 + val.GetHashCode();
    }
    
    protected string GetHostAndPort(string host, int port)
    {
        if (port > 0)
        {
            return $"{host}:{port}";
        }

        return host;
    }
    
    protected long GetNanoTime()
    {
        return DateTime.UtcNow.Ticks * 100;
    }

    protected void StorePhaseTime(BlueGreenPhaseType? phase)
    {
        if (phase == null)
        {
            return;
        }
        this.phaseTimeNano.TryAdd(
            phase.ToString() + (this.rollback ? " (rollback)" : string.Empty),
            new PhaseTimeInfo(DateTime.UtcNow, this.GetNanoTime(), phase));
    }

    protected void StoreBlueDnsUpdateTime()
    {
        this.phaseTimeNano.TryAdd(
            "Blue DNS updated" + (this.rollback ? " (rollback)" : string.Empty),
            new PhaseTimeInfo(DateTime.UtcNow, this.GetNanoTime(), null));
    }

    protected void StoreGreenDnsRemoveTime()
    {
        this.phaseTimeNano.TryAdd(
            "Green DNS removed" + (this.rollback ? " (rollback)" : string.Empty),
            new PhaseTimeInfo(DateTime.UtcNow, this.GetNanoTime(), null));
    }

    protected void StoreGreenNodeChangeNameTime()
    {
        this.phaseTimeNano.TryAdd(
            "Green node certificates changed" + (this.rollback ? " (rollback)" : string.Empty),
            new PhaseTimeInfo(DateTime.UtcNow, this.GetNanoTime(), null));
    }

    protected void StoreGreenTopologyChangeTime()
    {
        this.phaseTimeNano.TryAdd(
            "Green topology changed" + (this.rollback ? " (rollback)" : string.Empty),
            new PhaseTimeInfo(DateTime.UtcNow, this.GetNanoTime(), null));
    }

    protected void StoreMonitorResetTime(string eventName)
    {
        this.phaseTimeNano.TryAdd(
            "Monitors reset " + eventName + (this.rollback ? " (rollback)" : string.Empty),
            new PhaseTimeInfo(DateTime.UtcNow, this.GetNanoTime(), null));
    }

    protected void LogSwitchoverFinalSummary()
    {
        bool switchoverCompleted =
            (!this.rollback && this.summaryStatus!.CurrentPhase == BlueGreenPhaseType.COMPLETED)
            || (this.rollback && this.summaryStatus.CurrentPhase == BlueGreenPhaseType.CREATED);

        bool hasActiveSwitchoverPhases = this.phaseTimeNano
            .Any(x => x.Value.Phase != null && BlueGreenPhase.IsActiveSwitchoverOrCompleted((BlueGreenPhaseType)x.Value.Phase));

        if (!switchoverCompleted || !hasActiveSwitchoverPhases)
        {
            return;
        }

        const int eventNameLeftPadChars = 5;
        const int eventNameDefaultFieldSize = 31;
        int maxEventNameLength = this.phaseTimeNano.Keys
            .Select(x => x.Length + eventNameLeftPadChars)
            .DefaultIfEmpty(eventNameDefaultFieldSize)
            .Max();

        BlueGreenPhaseType timeZeroPhase = this.rollback ? BlueGreenPhaseType.PREPARATION : BlueGreenPhaseType.IN_PROGRESS;
        string timeZeroKey = this.rollback ? timeZeroPhase + " (rollback)" : timeZeroPhase.ToString();
        this.phaseTimeNano.TryGetValue(timeZeroKey, out PhaseTimeInfo? timeZero);

        string divider = "---------------------------------------------------"
                         + new string('-', maxEventNameLength)
                         + "\n";

        string logMessage =
            $"[bgdId: '{this.bgdId}']"
            + "\n" + divider
            + string.Format("{0,-28} {1,21} {2," + maxEventNameLength + "}\n",
                "timestamp",
                "time offset (ms)",
                "event")
            + divider
            + string.Join("\n", this.phaseTimeNano
                .OrderBy(y => y.Value.TimestampNano)
                .Select(x => string.Format("{0,28} {1,18} ms {{2," + maxEventNameLength + "}}",
                    x.Value.Timestamp,
                    timeZero == null ? string.Empty : (x.Value.TimestampNano - timeZero.TimestampNano) / 1_000_000,
                    x.Key)))
            + "\n" + divider;

        Logger.LogInformation(logMessage);
    }

    public class PhaseTimeInfo
    {
        public DateTime Timestamp { get; }
        public long TimestampNano { get; }
        public BlueGreenPhaseType? Phase { get; }

        public PhaseTimeInfo(DateTime timestamp, long timestampNano, BlueGreenPhaseType? phase)
        {
            this.Timestamp = timestamp;
            this.TimestampNano = timestampNano;
            this.Phase = phase;
        }
    }
}
