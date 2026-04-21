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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;

public class BlueGreenStatusMonitor
{
    private const string BgClusterId = "941d00a8-8238-4f7d-bf59-771bff783a8e";
    private const string LatestKnownVersion = "1.0";
    private const int DefaultTimeoutSec = 10;

    private static readonly ILogger<BlueGreenStatusMonitor> Logger = LoggerUtils.GetLogger<BlueGreenStatusMonitor>();
    private static readonly long DefaultCheckIntervalMs = (long)TimeSpan.FromMinutes(5).TotalMilliseconds;
    private static readonly HashSet<string> KnownVersions = [LatestKnownVersion];

    private readonly IBlueGreenDialect currentDialect;
    private readonly IPluginService pluginService;
    private readonly string bgdId;
    private readonly Dictionary<string, string> props;
    private readonly BlueGreenRoleType role;
    private readonly OnBlueGreenStatusChange onBlueGreenStatusChange;
    private readonly Dictionary<BlueGreenIntervalRate, long> statusCheckIntervalMap;
    private readonly HostSpec initialHostSpec;
    private readonly ConcurrentDictionary<string, string?> startIpAddressesByHostMap = new();
    private readonly ConcurrentDictionary<string, string?> currentIpAddressesByHostMap = new();

    private CancellationTokenSource cancellationTokenSource = new();
    private TaskCompletionSource<bool> stateChangedTcs = new();
    private Task? monitoringTask;
    private Task? openConnectionTask;

    private bool collectIpAddresses = true;
    private bool collectTopology = true;
    private BlueGreenIntervalRate intervalRate = BlueGreenIntervalRate.BASELINE;
    private bool useIpAddress;

    private IHostListProvider? hostListProvider;
    private IList<HostSpec> startTopology = new List<HostSpec>();
    private IList<HostSpec> currentTopology = new List<HostSpec>();
    private ConcurrentBag<string> hostNames = [];

    private bool allStartTopologyIpChanged;
    private bool allStartTopologyEndpointsRemoved;
    private bool allTopologyChanged;
    private BlueGreenPhaseType? currentPhase = BlueGreenPhaseType.NOT_CREATED;

    private string version = "1.0";
    private int port = -1;

    private DbConnection? connection;
    private HostSpec? connectionHostSpec;
    private string? connectedIpAddress;
    private bool connectionHostSpecCorrect;
    private bool panicMode = true;

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
        IDialect dialect = pluginService.Dialect;
        if (DialectUtils.IsBlueGreenConnectionDialect(dialect))
        {
            this.currentDialect = (IBlueGreenDialect)dialect;
        }
        else
        {
            Logger.LogWarning(Resources.BlueGreenStatusProvider_UnsupportedDialect, this.bgdId, dialect.GetType().Name);
        }
    }

    public void Start()
    {
        if (this.currentDialect == null)
        {
            throw new InvalidOperationException(Resources.BlueGreenStatusProvider_UnsupportedDialect);
        }

        this.cancellationTokenSource?.Cancel();
        this.monitoringTask?.Wait();

        this.cancellationTokenSource = new CancellationTokenSource();
        this.monitoringTask = Task.Run(() =>
            this.RunMonitoringLoop(this.cancellationTokenSource.Token));
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

                    Logger.LogTrace($"Phase RunMonitoringLoop [{this.currentPhase}] [{oldPhase}]");
                    if (this.currentPhase != null && (oldPhase == null || oldPhase != this.currentPhase))
                    {
                        Logger.LogTrace(Resources.BlueGreenStatusMonitor_RunMonitoringLoop_StatusChanged, this.role, oldPhase);
                    }

                    this.onBlueGreenStatusChange?.Invoke(this.role,
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

                    long delayMs = this.statusCheckIntervalMap.GetValueOrDefault(
                        this.panicMode ? BlueGreenIntervalRate.HIGH : this.intervalRate,
                        DefaultCheckIntervalMs);

                    await this.Delay(delayMs, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    await this.cancellationTokenSource.CancelAsync();
                    Logger.LogTrace(Resources.BlueGreenStatusMonitor_RunMonitoringLoop_Interrupted, this.role);
                    return;
                }
                catch (Exception ex)
                {
                    Logger.LogTrace(ex, "[bgdId: {0}] Exception in RunMonitoringLoop for role: {1}", this.bgdId, this.role);
                    Logger.LogTrace($"ERROR: {ex}");  // Also print to console
                }
            }
        }
        finally
        {
            await this.CloseConnection();
        }
    }

    protected async Task Delay(long delayMs, CancellationToken cancellationToken)
    {
        Stopwatch sw = Stopwatch.StartNew();
        var currentIntervalRate = this.intervalRate;
        var currentPanic = this.panicMode;
        var minDelay = Math.Min(delayMs, 50);

        do
        {
            var delayTask = Task.Delay(TimeSpan.FromMilliseconds(minDelay), cancellationToken);
            var stateTask = this.stateChangedTcs.Task;
            await Task.WhenAny(delayTask, stateTask);
        }
        while (this.intervalRate == currentIntervalRate
               && sw.ElapsedMilliseconds < delayMs
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
            this.cancellationTokenSource.Cancel();
        }

        this.NotifyChanges();
    }

    public void ResetCollectedData()
    {
        this.startIpAddressesByHostMap.Clear();
        this.startTopology = new List<HostSpec>();
        this.hostNames = [];
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
            var addresses = Dns.GetHostAddresses(host);
            return addresses.FirstOrDefault()?.ToString();
        }
        catch (SocketException)
        {
            return null;
        }
    }

    protected async Task CollectTopology()
    {
        if (this.hostListProvider == null || this.connection == null)
        {
            return;
        }

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

    protected async Task CloseConnection()
    {
        var conn = this.connection;
        this.connection = null;
        await conn?.CloseAsync()!;
        conn.Dispose();
    }

    protected async Task CollectStatus()
    {
        if (this.connection is null or { State: ConnectionState.Closed } or { State: ConnectionState.Broken })
        {
            return;
        }

        try
        {
            Logger.LogTrace($"[{this.role}] CollectStatus Checking BlueGreenStatusAvailable");
            if (!await this.currentDialect.IsBlueGreenStatusAvailable(this.connection))
            {
                Logger.LogTrace($"CollectStatus Connection State {this.connection.State}");
                if (this.connection.State != ConnectionState.Closed && this.connection.State != ConnectionState.Broken)
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
            command.CommandText = this.currentDialect.GetBlueGreenStatusQuery();
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

                Logger.LogTrace($"version {ver}, endpoint {endpoint}, port {p} phase {phase} role {r}");
                statusEntries.Add(new StatusInfo(ver, endpoint, p, phase, r));
            }

            Logger.LogTrace("Status entries:\n  {0}",
                string.Join("\n  ", statusEntries.Select((s, i) => $"[{i}] {s}")));

            // Check if there's a cluster writer endpoint.
            var statusInfo = statusEntries
                .FirstOrDefault(x => RdsUtils.IsWriterClusterDns(x.Endpoint) && RdsUtils.IsNotOldInstance(x.Endpoint));

            Logger.LogTrace($"StatusInfo CollectStatus {statusInfo == null}");

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
                    .FirstOrDefault(x => RdsUtils.IsRdsInstance(x.Endpoint) && RdsUtils.IsNotOldInstance(x.Endpoint));
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
                    .Where(x => RdsUtils.IsNotOldInstance(x.Endpoint))
                    .Select(x => x.Endpoint.ToLower())
                    .ToHashSet();

                foreach (var host in hostNamesToAdd)
                {
                    this.hostNames.Add(host);
                }
            }

            Logger.LogTrace($"connectionHostSpecCorrect: {this.connectionHostSpecCorrect}, statusInfo: {statusInfo}");

            if (!this.connectionHostSpecCorrect && statusInfo != null)
            {
                // We connected to an initialHostSpec that might be not the desired Blue or Green cluster.
                // We need to reconnect to a correct one.

                var statusInfoHostIpAddress = this.GetIpAddress(statusInfo.Endpoint);
                var connectedIpAddressCopy = this.connectedIpAddress;

                Logger.LogTrace($"statusInfoHostIpAddress: {statusInfoHostIpAddress}, connectedIpAddressCopy: {connectedIpAddressCopy}");

                if (connectedIpAddressCopy != null && connectedIpAddressCopy != statusInfoHostIpAddress)
                {
                    // Found endpoint confirms that we're connected to a different node, and we need to reconnect.
                    this.connectionHostSpec = new HostSpec(statusInfo.Endpoint, statusInfo.Port, HostRole.Unknown, HostAvailability.Available);
                    this.connectionHostSpecCorrect = true;
                    await this.CloseConnection();
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
        catch (DbException e)
        {
            if (this.connection?.State != System.Data.ConnectionState.Closed && this.connection?.State != ConnectionState.Broken)
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

            await this.CloseConnection();
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
            Logger.LogTrace("IsConnectionClosed Connection State {ConnectionState}", conn?.State);
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
        Logger.LogTrace("[{Role}] OpenConnection Connection State {ConnectionState}", this.role, conn?.State);
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

            var connectionHostSpecCopy = this.connectionHostSpec.Clone();
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
            [PropertyDefinition.ClusterId.Name] = $"{this.bgdId}::{this.role}::{BgClusterId}",
        };

        Logger.LogTrace(
            Resources.BlueGreenStatusMonitor_InitHostListProvider_CreateHostListProvider,
            this.role,
            hostListProperties[PropertyDefinition.ClusterId.Name]);

        if (this.connectionHostSpec != null)
        {
            hostListProperties[PropertyDefinition.Host.Name] = this.connectionHostSpec.Host;
            hostListProperties[PropertyDefinition.Port.Name] = this.connectionHostSpec.Port.ToString();
            this.hostListProvider = this.pluginService.Dialect.HostListProviderSupplier(hostListProperties, (IHostListProviderService)this.pluginService, this.pluginService);
        }
        else
        {
            Logger.LogWarning(Resources.BlueGreenStatusMonitor_InitHostListProvider_HostSpecNull);
        }
    }

    private void NotifyChanges()
    {
        var oldTcs = Interlocked.Exchange(ref this.stateChangedTcs, new TaskCompletionSource<bool>());
        oldTcs.TrySetResult(true);
    }
}

public delegate void OnBlueGreenStatusChange(BlueGreenRoleType role, BlueGreenInterimStatus interimStatus);
