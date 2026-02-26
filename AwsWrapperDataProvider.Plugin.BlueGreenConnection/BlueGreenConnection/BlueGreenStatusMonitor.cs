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
using System.Data.Common;
using System.Net;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Dialect;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Properties;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection;

public class BlueGreenStatusMonitor
{
    private static readonly ILogger<BlueGreenStatusMonitor> Logger = LoggerUtils.GetLogger<BlueGreenStatusMonitor>();

    protected static readonly long DefaultCheckIntervalMs = TimeSpan.FromMinutes(5).Milliseconds;
    protected static readonly string BgClusterId = "941d00a8-8238-4f7d-bf59-771bff783a8e";
    protected static readonly string LatestKnownVersion = "1.0";
    protected static readonly HashSet<string> KnownVersions = new() { LatestKnownVersion };

    protected readonly IDialect currentDialect;
    protected readonly IPluginService pluginService;
    protected readonly string bgdId;
    protected readonly Dictionary<string, string> props;
    protected readonly BlueGreenRoleType role;
    protected readonly OnBlueGreenStatusChange onBlueGreenStatusChange;
    protected readonly Dictionary<BlueGreenIntervalRate, long> statusCheckIntervalMap;
    protected readonly HostSpec initialHostSpec;

    private readonly CancellationTokenSource concellationTokenSource = new();
    private readonly SemaphoreSlim sleepWaitObj = new(0);
    private Task? monitoringTask;
    private Task? openConnectionTask;

    protected bool collectIpAddresses = true;
    protected bool collectTopology = true;
    protected BlueGreenIntervalRate intervalRate = BlueGreenIntervalRate.BASELINE;
    protected bool useIpAddress;

    protected IHostListProvider? hostListProvider;
    protected IList<HostSpec> startTopology;
    protected IList<HostSpec> currentTopology;
    protected ConcurrentDictionary<string, string?> startIpAddressesByHostMap = new();
    protected ConcurrentDictionary<string, string?> currentIpAddressesByHostMap = new();

    protected bool allStartTopologyIpChanged;
    protected bool allStartTopologyEndpointsRemoved;
    protected bool allTopologyChanged;
    protected BlueGreenPhaseType? currentPhase = BlueGreenPhaseType.NOT_CREATED;
    protected ConcurrentBag<string> hostNames = new();

    protected string version = "1.0";
    protected int port = -1;

    protected DbConnection? connection;
    protected HostSpec? connectionHostSpec;
    protected string? connectedIpAddress;
    protected bool connectionHostSpecCorrect;
    protected bool panicMode = true;
    private static readonly int DefaultTimeoutSec = 10;

    protected BlueGreenConnectionUtils blueGreenConnectionUtils = new();

    public BlueGreenStatusMonitor(
        BlueGreenRoleType role,
        string bgdId,
        HostSpec initialHostSpec,
        IPluginService pluginService,
        Dictionary<string, string> props,
        Dictionary<BlueGreenIntervalRate, long> statusCheckIntervalMap,
        OnBlueGreenStatusChange onBlueGreenStatusChange)
    {
        this.role = role;
        this.bgdId = bgdId;
        this.initialHostSpec = initialHostSpec;
        this.pluginService = pluginService;
        this.props = props;
        this.statusCheckIntervalMap = statusCheckIntervalMap;
        this.onBlueGreenStatusChange = onBlueGreenStatusChange;
        this.currentDialect = pluginService.Dialect;
    }

    public void Start()
    {
        if (this.monitoringTask != null && !this.monitoringTask.IsCompleted)
        {
            this.concellationTokenSource.Cancel();
            this.monitoringTask.Wait(TimeSpan.FromSeconds(5));
        }

        this.monitoringTask = Task.Run(() => this.RunMonitoringLoop(this.concellationTokenSource.Token));
    }

    protected async Task RunMonitoringLoop(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    BlueGreenPhaseType? oldPhase = this.currentPhase;
                    await this.OpenConnection();
                    await this.CollectStatus();
                    await this.CollectTopology();
                    this.CollectHostIpAddresses();
                    this.UpdateIpAddressFlags();

                    if (this.currentPhase != null && oldPhase != this.currentPhase)
                    {
                        Logger.LogTrace(Resources.BlueGreenStatusMonitor_RunMonitoringLoop_StatusChanged);
                    }

                    this.onBlueGreenStatusChange(
                        this.role,
                        new BlueGreenInterimStatus(
                            this.currentPhase,
                            this.version,
                            this.port,
                            this.startTopology,
                            this.currentTopology,
                            this.startIpAddressesByHostMap.ToDictionary(x => x.Key, x => x.Value),
                            this.currentIpAddressesByHostMap.ToDictionary(x => x.Key, x => x.Value),
                            this.hostNames.ToHashSet(),
                            this.allStartTopologyIpChanged,
                            this.allStartTopologyEndpointsRemoved,
                            this.allTopologyChanged));

                    // TODO: Atomic Boolean for panicMode
                    long delayMs = this.statusCheckIntervalMap.GetValueOrDefault(
                        this.panicMode ? BlueGreenIntervalRate.HIGH : this.intervalRate,
                        DefaultCheckIntervalMs);

                    await this.Delay(delayMs, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                    // Log exception
                }
            }
        }
        finally
        {
            this.CloseConnection();
        }
    }

    protected async Task Delay(long delayMs, CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow;
        var end = start.AddMilliseconds(delayMs);
        var currentIntervalRate = this.intervalRate;
        var currentPanic = this.panicMode;
        var minDelay = Math.Min(delayMs, 50);

        do
        {
            await this.sleepWaitObj.WaitAsync(TimeSpan.FromMilliseconds(minDelay), cancellationToken);
        }
        while (this.intervalRate == currentIntervalRate
                 && DateTime.UtcNow < end
                 && !cancellationToken.IsCancellationRequested
                 && currentPanic == this.panicMode);
    }

    public void SetIntervalRate(BlueGreenIntervalRate rate)
    {
        this.intervalRate = rate;
        this.NotifyChanges();
    }

    public void SetCollectIpAddresses(bool collect) => this.collectIpAddresses = collect;
    public void SetCollectTopology(bool collect) => this.collectTopology = collect;
    public void SetUseIpAddress(bool use) => this.useIpAddress = use;

    public void SetStop(bool stop)
    {
        if (stop)
        {
            this.concellationTokenSource.Cancel();
        }

        this.NotifyChanges();
    }

    public void ResetCollectedData()
    {
        this.startIpAddressesByHostMap.Clear();
        this.startTopology = new List<HostSpec>();
        this.hostNames = new ConcurrentBag<string>();
    }

    protected void CollectHostIpAddresses()
    {
        this.currentIpAddressesByHostMap.Clear();

        foreach (var host in this.hostNames)
        {
            this.currentIpAddressesByHostMap.TryAdd(host, this.GetIpAddress(host));
        }

        if (this.collectIpAddresses)
        {
            this.startIpAddressesByHostMap.Clear();
            foreach (var kvp in this.currentIpAddressesByHostMap)
            {
                this.startIpAddressesByHostMap[kvp.Key] = kvp.Value;
            }
        }
    }

    protected void UpdateIpAddressFlags()
    {
        if (this.collectTopology)
        {
            this.allStartTopologyIpChanged = false;
            this.allStartTopologyEndpointsRemoved = false;
            this.allTopologyChanged = false;
            return;
        }

        if (!this.collectIpAddresses)
        {
            this.allStartTopologyIpChanged = this.startTopology.Any() && this.startTopology.All(x =>
            {
                var host = x.Host;
                var startIp = this.startIpAddressesByHostMap.GetValueOrDefault(host);
                var currentIp = this.currentIpAddressesByHostMap.GetValueOrDefault(host);
                return startIp != null && currentIp != null && startIp != currentIp;
            });
        }

        this.allStartTopologyEndpointsRemoved = this.startTopology.Any() && this.startTopology.All(x =>
        {
            var host = x.Host;
            var startIp = this.startIpAddressesByHostMap.GetValueOrDefault(host);
            var currentIp = this.currentIpAddressesByHostMap.GetValueOrDefault(host);
            return startIp != null && currentIp == null;
        });

        if (!this.collectTopology)
        {
            var startTopologyNodes = this.startTopology.Select(x => x.Host).ToHashSet();
            this.allTopologyChanged = this.currentTopology.Any()
                                 && startTopologyNodes.Any()
                                 && this.currentTopology.All(x => !startTopologyNodes.Contains(x.Host));
        }
    }

    protected string? GetIpAddress(string host)
    {
        try
        {
            return Dns.GetHostAddresses(host).FirstOrDefault()?.ToString();
        }
        catch
        {
            return null;
        }
    }

    protected async Task CollectTopology()
    {
        if (this.hostListProvider == null || this.connection == null)
            return;

        this.currentTopology = await this.hostListProvider.ForceRefreshAsync(this.connection);

        if (this.collectTopology)
        {
            this.startTopology = this.currentTopology;
        }

        if (this.currentTopology.Count > 0 && this.collectTopology)
        {
            foreach (var host in this.currentTopology.Select(x => x.Host))
            {
                this.hostNames.Add(host);
            }
        }
    }

    protected void CloseConnection()
    {
        var conn = this.connection;
        this.connection = null;
        conn?.Close();
        conn?.Dispose();
    }

    protected async Task CollectStatus()
    {
        if (this.connection is not { State: System.Data.ConnectionState.Open })
        {
            return;
        }

        try
        {
            if (!BlueGreenConnectionDialectHelper.IsBlueGreenStatusAvailable(this.currentDialect, this.connection))
            {
                if (this.connection.State != System.Data.ConnectionState.Closed)
                {
                    this.currentPhase = BlueGreenPhaseType.NOT_CREATED;
                    Logger.LogTrace(Resources.BlueGreenStatusMonitor_CollectStatus_StatusNotAvailable, this.role, BlueGreenPhaseType.NOT_CREATED);
                }
                else
                {
                    this.connection = null;
                    this.panicMode = true;
                    this.currentPhase = null;
                }

                return;
            }

            await using var command = this.connection.CreateCommand();
            command.CommandTimeout = DefaultTimeoutSec;
            command.CommandText = BlueGreenConnectionDialectHelper.GetBlueGreenStatusQuery(this.currentDialect);
            await using var reader = await command.ExecuteReaderAsync();

            var statusEntries = new List<StatusInfo>();
            while (await reader.ReadAsync())
            {
                var ver = reader.GetString(reader.GetOrdinal("version"));
                if (!KnownVersions.Contains(ver))
                {
                    ver = LatestKnownVersion;
                }

                var endpoint = reader.GetString(reader.GetOrdinal("endpoint"));
                var p = reader.GetInt32(reader.GetOrdinal("port"));
                var r = BlueGreenRole.ParseRole(reader.GetString(reader.GetOrdinal("role")), ver);
                var phase = BlueGreenPhase.ParsePhase(reader.GetString(reader.GetOrdinal("status")), ver);

                if (this.role != r)
                {
                    continue;
                }

                statusEntries.Add(new StatusInfo(ver, endpoint, p, phase, r));
            }

            // Check if there's a cluster writer endpoint.
            var statusInfo = statusEntries
                .FirstOrDefault(x => RdsUtils.IsWriterClusterDns(x.Endpoint) && this.blueGreenConnectionUtils.IsNotOldInstance(x.Endpoint));

            if (statusInfo != null)
            {
                // Cluster writer endpoint found.
                // Add cluster reader endpoint as well.
                this.hostNames.Add(statusInfo.Endpoint.ToLower().Replace(".cluster-", ".cluster-ro-"));
            }

            if (statusInfo == null)
            {
                // maybe it's an instance endpoint?
                statusInfo = statusEntries
                    .FirstOrDefault(x => RdsUtils.IsRdsInstance(x.Endpoint) && this.blueGreenConnectionUtils.IsNotOldInstance(x.Endpoint));
            }

            if (statusInfo == null)
            {
                if (!statusEntries.Any())
                {
                    // It's normal to expect that the status table has no entries after BGD is completed.
                    // Old1 cluster/instance has been separated and no longer receives
                    // updates from related green cluster/instance.
                    // Metadata at new blue cluster/instance can be removed after switchover, and it's also expected to get
                    // no records.
                    if (this.role != BlueGreenRoleType.SOURCE)
                    {
                        Logger.LogTrace(Resources.BlueGreenStatusMonitor_CollectStatus_NoEntriesInStatusTable, this.role);
                    }

                    this.currentPhase = null;
                }
            }
            else
            {
                this.currentPhase = statusInfo.Phase;
                this.version = statusInfo.Version;
                this.port = statusInfo.Port;
            }

            if (this.collectTopology)
            {
                var hostNamesToAdd = statusEntries
                    .Where(x => this.blueGreenConnectionUtils.IsNotOldInstance(x.Endpoint))
                    .Select(x => x.Endpoint.ToLower())
                    .ToHashSet();

                foreach (var host in hostNamesToAdd)
                {
                    this.hostNames.Add(host);
                }
            }

            if (!this.connectionHostSpecCorrect && statusInfo != null)
            {
                // We connected to an initialHostSpec that might be not the desired Blue or Green cluster.
                // We need to reconnect to a correct one.

                var statusInfoHostIpAddress = this.GetIpAddress(statusInfo.Endpoint);
                var connectedIpAddressCopy = this.connectedIpAddress;

                if (connectedIpAddressCopy != null && connectedIpAddressCopy != statusInfoHostIpAddress)
                {
                    // Found endpoint confirms that we're connected to a different node, and we need to reconnect.
                    this.connectionHostSpec = new HostSpec(statusInfo.Endpoint, statusInfo.Port, HostRole.Unknown, HostAvailability.Available);
                    this.connectionHostSpecCorrect = true;
                    this.CloseConnection();
                    this.panicMode = true;
                }
                else
                {
                    // We're already connected to a correct node.
                    this.connectionHostSpecCorrect = true;
                    this.panicMode = false;
                }
            }

            if (this.connectionHostSpecCorrect && this.hostListProvider == null)
            {
                // A connection to a correct cluster (blue or green) is established.
                // Let's initialize HostListProvider
                this.InitHostListProvider();
            }
        }
        catch (DbException e) when (e.Message?.Contains("syntax", StringComparison.OrdinalIgnoreCase) == true)
        {
            this.currentPhase = BlueGreenPhaseType.NOT_CREATED;
            Logger.LogWarning(e, Resources.BlueGreenStatusMonitor_CollectStatus_Exception, this.role, BlueGreenPhaseType.NOT_CREATED);
        }
        catch (DbException e)
        {
            if (this.connection?.State != System.Data.ConnectionState.Closed)
            {
                // It's normal to get connection closed during BGD switchover.
                // If connection isn't closed but there's an exception then let's log it.

                // For PG databases
                if (e.Message?.Contains("An error occured while retrieving the blue/green fast switchover metadata") == true)
                {
                    this.currentPhase = BlueGreenPhaseType.NOT_CREATED;
                    return;
                }

                Logger.LogTrace(e, Resources.BlueGreenStatusMonitor_CollectStatus_UnhandledSqlException, this.role);
            }
            else
            {
                Logger.LogDebug("[CollectStatus] Connection closed during switchover for role: {Role}", this.role);
            }
            this.CloseConnection();
            this.panicMode = true;
        }
        catch (Exception e)
        {
            Logger.LogTrace(e, Resources.BlueGreenStatusMonitor_CollectStatus_UnhandledException, this.role);
        }
    }

    protected bool IsConnectionClosed(DbConnection? conn)
    {
        try
        {
            return conn == null 
                || conn.State == System.Data.ConnectionState.Closed 
                || conn.State == System.Data.ConnectionState.Broken;
        }
        catch (DbException)
        {
            // do nothing
        }

        return true;
    }


    protected async Task OpenConnection()
    {
        var conn = this.connection;
        if (!this.IsConnectionClosed(conn))
        {
            return;
        }

        if (this.openConnectionTask != null)
        {
            if (this.openConnectionTask.IsCompleted)
            {
                if (!this.panicMode)
                {
                    return; // Connection should be open by now.
                }
            }
            else if (!this.openConnectionTask.IsCanceled)
            {
                // Opening a new connection is in progress. Let's wait.
                await this.openConnectionTask;
                return;
            }
            else
            {
                this.openConnectionTask = null;
            }
        }

        this.connection = null;
        this.panicMode = true;

        this.openConnectionTask = Task.Run(async () =>
        {
            Logger.LogDebug("[OpenConnection] Task started for role: {Role}", this.role);

            if (this.connectionHostSpec == null)
            {
                this.connectionHostSpec = this.initialHostSpec;
                this.connectedIpAddress = null;
                this.connectionHostSpecCorrect = false;
            }

            var connectionHostSpecCopy = this.connectionHostSpec;
            var connectedIpAddressCopy = this.connectedIpAddress;

            try
            {
                if (this.useIpAddress && connectedIpAddressCopy != null)
                {
                    var connectionWithIpHostSpec = new HostSpec(
                        connectedIpAddressCopy,
                        connectionHostSpecCopy.Port,
                        connectionHostSpecCopy.Role,
                        connectionHostSpecCopy.Availability);

                    var connectWithIpProperties = new Dictionary<string, string>(this.props)
                    {
                        [PropertyDefinition.IamHost.Name] = connectionHostSpecCopy.Host,
                    };

                    Logger.LogTrace(Resources.BlueGreenStatusMonitor_OpenConnection_OpeningConnectionWithIp, this.role, connectionWithIpHostSpec.Host);

                    this.connection = await this.pluginService.ForceOpenConnection(connectionWithIpHostSpec, connectWithIpProperties, null, false);

                    Logger.LogTrace(Resources.BlueGreenStatusMonitor_OpenConnection_OpenedConnectionWithIp, this.role, connectionWithIpHostSpec.Host);
                }
                else
                {
                    Logger.LogTrace(Resources.BlueGreenStatusMonitor_OpenConnection_OpeningConnection, this.role, connectionHostSpecCopy.Host);

                    connectedIpAddressCopy = this.GetIpAddress(connectionHostSpecCopy.Host);
                    this.connection = await this.pluginService.ForceOpenConnection(connectionHostSpecCopy, this.props, null, false);
                    this.connectedIpAddress = connectedIpAddressCopy;

                    Logger.LogTrace(Resources.BlueGreenStatusMonitor_OpenConnection_OpenedConnection, this.role, connectionHostSpecCopy.Host);
                }

                Logger.LogDebug("[OpenConnection] Connection opened successfully for role: {Role}", this.role);
                this.panicMode = false;
                this.NotifyChanges();
            }
            catch (DbException ex)
            {
                Logger.LogWarning(ex, "[OpenConnection] Failed to open connection for role: {Role}", this.role);
                this.connection = null;
                this.panicMode = true;
                this.NotifyChanges();
            }
        });

        await this.openConnectionTask;
    }

    protected void InitHostListProvider()
    {
        if (this.hostListProvider != null || !this.connectionHostSpecCorrect)
        {
            return;
        }

        var hostListProperties = new Dictionary<string, string>(this.props)
        {
            // Need to instantiate a separate HostListProvider with
            // a special unique clusterId to avoid interference with other HostListProviders opened for this cluster.
            // Blue and Green clusters are expected to have different clusterId.
            ["ClusterId"] = $"{this.bgdId}::{this.role}::{BgClusterId}",
        };

        Logger.LogTrace(
            Resources.BlueGreenStatusMonitor_InitHostListProvider_CreateHostListProvider,
            this.role,
            hostListProperties["ClusterId"]);

        var connectionHostSpecCopy = this.connectionHostSpec;

        if (connectionHostSpecCopy != null)
        {
            this.props[PropertyDefinition.Host.Name] = connectionHostSpecCopy.Host;
            this.props[PropertyDefinition.Port.Name] = connectionHostSpecCopy.Port.ToString();
            this.hostListProvider = this.pluginService.Dialect.HostListProviderSupplier(this.props, (PluginService)this.pluginService, this.pluginService);
        }
        else
        {
            Logger.LogWarning(Resources.BlueGreenStatusMonitor_InitHostListProvider_HostSpecNull);
        }
    }


    protected void NotifyChanges()
    {
        sleepWaitObj.Release();
    }

    private class StatusInfo
    {
        public string Version { get; }
        public string Endpoint { get; }
        public int Port { get; }
        public BlueGreenPhaseType Phase { get; }
        public BlueGreenRoleType Role { get; }

        public StatusInfo(string version, string endpoint, int port, BlueGreenPhaseType phase, BlueGreenRoleType role)
        {
            Version = version;
            Endpoint = endpoint;
            Port = port;
            Phase = phase;
            Role = role;
        }
    }
}
