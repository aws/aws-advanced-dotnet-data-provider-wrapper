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
using System.Globalization;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// Implementation of cluster topology monitoring with background refresh capabilities.
/// </summary>
public class ClusterTopologyMonitor : IClusterTopologyMonitor
{
    private static readonly ILogger<ClusterTopologyMonitor> Logger = LoggerUtils.GetLogger<ClusterTopologyMonitor>();
    protected const int DefaultTopologyQueryTimeoutSec = 1;

    // Keep monitoring topology with a high rate for 30s after failover
    protected static readonly TimeSpan HighRefreshPeriodAfterPanic = TimeSpan.FromSeconds(30);
    protected static readonly TimeSpan IgnoreTopologyRequestDuration = TimeSpan.FromSeconds(10);

    protected readonly TimeSpan refreshRate;
    protected readonly TimeSpan highRefreshRate;
    protected readonly TimeSpan topologyCacheExpiration;
    protected readonly Dictionary<string, string> properties;
    protected readonly IPluginService pluginService;
    protected readonly HostSpec initialHostSpec;
    protected readonly MemoryCache topologyMap;
    protected readonly string topologyQuery;
    protected readonly string nodeIdQuery;
    protected readonly string writerTopologyQuery;
    protected readonly IHostListProviderService hostListProviderService;
    protected readonly HostSpec clusterInstanceTemplate;
    protected readonly CancellationTokenSource ctsTopologyMonitoring = new();
    protected readonly object topologyUpdatedLock = new();
    protected readonly ConcurrentDictionary<string, Lazy<Task>> nodeThreads = new();
    protected readonly Task monitoringTask;
    protected readonly object disposeLock = new();
    protected readonly SemaphoreSlim monitoringConnectionSemaphore = new(1, 1);

    protected CancellationTokenSource ctsNodeMonitoring;
    protected string clusterId;
    protected HostSpec? writerHostSpec;
    protected DbConnection? monitoringConnection;
    protected bool isVerifiedWriterConnection;
    protected DateTime highRefreshRateEndTime = DateTime.MinValue;
    protected volatile bool requestToUpdateTopology;
    protected long ignoreNewTopologyRequestsEndTime = -1;
    protected volatile bool nodeThreadsStop;
    protected DbConnection? nodeThreadsWriterConnection;
    protected HostSpec? nodeThreadsWriterHostSpec;
    protected DbConnection? nodeThreadsReaderConnection;
    protected IList<HostSpec>? nodeThreadsLatestTopology;
    protected bool disposed = false;

    public ClusterTopologyMonitor(
        string clusterId,
        MemoryCache topologyMap,
        HostSpec initialHostSpec,
        Dictionary<string, string> properties,
        IPluginService pluginService,
        IHostListProviderService hostListProviderService,
        HostSpec clusterInstanceTemplate,
        TimeSpan refreshRate,
        TimeSpan highRefreshRate,
        TimeSpan topologyCacheExpiration,
        string topologyQuery,
        string writerTopologyQuery,
        string nodeIdQuery)
    {
        this.clusterId = clusterId;
        this.topologyMap = topologyMap;
        this.initialHostSpec = initialHostSpec;
        this.pluginService = pluginService;
        this.hostListProviderService = hostListProviderService;
        this.clusterInstanceTemplate = clusterInstanceTemplate;
        this.refreshRate = refreshRate;
        this.highRefreshRate = highRefreshRate;
        this.topologyCacheExpiration = topologyCacheExpiration;
        this.topologyQuery = topologyQuery;
        this.writerTopologyQuery = writerTopologyQuery;
        this.nodeIdQuery = nodeIdQuery;

        // Use original properties directly
        this.properties = properties;

        // Make sure node monitoring tasks are cancelled once topology monitoring task is cancelled
        this.ctsNodeMonitoring = CancellationTokenSource.CreateLinkedTokenSource(this.ctsTopologyMonitoring.Token);

        this.monitoringTask = Task.Run(this.RunMonitoringLoop, this.ctsTopologyMonitoring.Token);
    }

    public bool CanDispose => true;

    public void SetClusterId(string clusterId)
    {
        this.clusterId = clusterId;
    }

    public async Task RunMonitoringLoop()
    {
        try
        {
            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_StartMonitoringThread, this.initialHostSpec.Host));

            while (!this.ctsTopologyMonitoring.Token.IsCancellationRequested)
            {
                if (this.IsInPanicMode())
                {
                    if (this.nodeThreads.IsEmpty)
                    {
                        Logger.LogTrace(Resources.ClusterTopologyMonitor_StartingNodeMonitoringThreads);

                        // Start node threads
                        this.nodeThreadsStop = false;
                        this.nodeThreadsWriterConnection = null;
                        this.nodeThreadsReaderConnection = null;
                        this.nodeThreadsWriterHostSpec = null;
                        this.nodeThreadsLatestTopology = null;

                        var hosts = this.topologyMap.Get<IList<HostSpec>>(this.clusterId) ??
                                    await this.OpenAnyConnectionAndUpdateTopologyAsync();

                        this.ctsNodeMonitoring.Cancel();
                        Logger.LogTrace("Restarting cancellation token for node monitoring tasks");
                        this.ctsNodeMonitoring = CancellationTokenSource.CreateLinkedTokenSource(this.ctsTopologyMonitoring.Token);

                        if (hosts != null && !this.isVerifiedWriterConnection)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                NodeMonitoringTask nodeMonitoringTask = new(this, hostSpec, this.writerHostSpec);

                                // Run new task only if not existed
                                var lazyTask = this.nodeThreads.GetOrAdd(
                                    hostSpec.Host,
                                    _ => new Lazy<Task>(
                                        () =>
                                        {
                                            var task = Task.Run(() => nodeMonitoringTask.RunNodeMonitoringAsync(this.ctsNodeMonitoring.Token), this.ctsNodeMonitoring.Token);
                                            Logger.LogTrace($"Node Monitoring Task@{RuntimeHelpers.GetHashCode(nodeMonitoringTask)} created for host {hostSpec}");
                                            return task;
                                        },
                                        LazyThreadSafetyMode.ExecutionAndPublication));
                                _ = lazyTask.Value;
                            }
                        }
                    }
                    else
                    {
                        // Check if writer is already detected
                        if (this.nodeThreadsWriterConnection != null && this.nodeThreadsWriterHostSpec != null)
                        {
                            Logger.LogTrace(string.Format(
                                Resources.ClusterTopologyMonitor_WriterPickedUpFromNodeMonitors,
                                this.nodeThreadsWriterHostSpec.Host));

                            await this.DisposeConnectionAsync(this.monitoringConnection);
                            Interlocked.Exchange(ref this.monitoringConnection, this.nodeThreadsWriterConnection);
                            this.writerHostSpec = this.nodeThreadsWriterHostSpec;
                            this.isVerifiedWriterConnection = true;
                            this.highRefreshRateEndTime = DateTime.UtcNow.Add(HighRefreshPeriodAfterPanic);

                            // We verify the writer on initial connection and on failover, but we only want to ignore new topology
                            // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
                            // time to 0. Any future writer verifications will set it to a positive value.
                            if (Interlocked.CompareExchange(ref this.ignoreNewTopologyRequestsEndTime, 0, -1) != -1)
                            {
                                Interlocked.Exchange(ref this.ignoreNewTopologyRequestsEndTime, DateTime.UtcNow.Add(IgnoreTopologyRequestDuration).Ticks);
                                Logger.LogTrace("Ignoring topology request until {utcTime}", new DateTime(this.ignoreNewTopologyRequestsEndTime, DateTimeKind.Utc));
                            }

                            this.nodeThreadsStop = true;
                            this.ctsNodeMonitoring.Cancel();
                            Logger.LogTrace($"Clearing node threads...");
                            this.nodeThreads.Clear();
                            continue;
                        }

                        // Update node threads with new nodes in the topology
                        var hosts = this.nodeThreadsLatestTopology;
                        if (hosts != null && !this.nodeThreadsStop)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                NodeMonitoringTask nodeMonitoringTask = new(this, hostSpec, this.writerHostSpec);

                                // Run new task only if not existed
                                var lazyTask = this.nodeThreads.GetOrAdd(
                                    hostSpec.Host,
                                    _ => new Lazy<Task>(
                                        () =>
                                        {
                                            var task = Task.Run(() => nodeMonitoringTask.RunNodeMonitoringAsync(this.ctsNodeMonitoring.Token), this.ctsNodeMonitoring.Token);
                                            Logger.LogTrace($"Node Monitoring Task@{RuntimeHelpers.GetHashCode(task)} created for host {hostSpec}");
                                            return task;
                                        },
                                        LazyThreadSafetyMode.ExecutionAndPublication));
                                _ = lazyTask.Value;
                            }
                        }
                    }

                    await this.DelayAsync(true);
                }
                else
                {
                    // Regular mode (not panic mode)
                    if (!this.nodeThreads.IsEmpty)
                    {
                        this.ctsNodeMonitoring.Cancel();
                        Logger.LogTrace($"Clearing node threads...");
                        this.nodeThreads.Clear();
                    }

                    var hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);
                    if (hosts == null)
                    {
                        // Can't get topology, switch to panic mode
                        Logger.LogTrace("Monitoring connection@{Id} is set to null", RuntimeHelpers.GetHashCode(this.monitoringConnection));
                        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);

                        this.isVerifiedWriterConnection = false;
                        await this.DisposeConnectionAsync(conn);
                        continue;
                    }

                    if (this.highRefreshRateEndTime > DateTime.MinValue && this.highRefreshRateEndTime < DateTime.UtcNow)
                    {
                        this.highRefreshRateEndTime = DateTime.MinValue;
                    }

                    if (this.highRefreshRateEndTime == DateTime.MinValue)
                    {
                        Logger.LogTrace(LoggerUtils.LogTopology(hosts, $"Running Monitoring Loop. Found {hosts.Count} hosts"));
                    }

                    await this.DelayAsync(false);
                }

                long endTime = Interlocked.Read(ref this.ignoreNewTopologyRequestsEndTime);
                if (endTime > 0 && endTime < DateTime.UtcNow.Ticks)
                {
                    Interlocked.Exchange(ref this.ignoreNewTopologyRequestsEndTime, 0);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Suppress Error.
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, string.Format(Resources.ClusterTopologyMonitor_ExceptionDuringMonitoringStop, this.initialHostSpec.Host));
        }
        finally
        {
            Logger.LogTrace("Cancelling all node monitoring tasks from topology monitor connection@{Id}", RuntimeHelpers.GetHashCode(this.monitoringConnection));
            this.ctsNodeMonitoring.Cancel();

            try
            {
                var tasks = this.nodeThreads.Values.Select(lz => lz.Value).ToArray();
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "One or more node monitoring tasks failed during shutdown.");
            }

            Logger.LogTrace("Monitoring connection@{Id} is set to null", RuntimeHelpers.GetHashCode(this.monitoringConnection));
            var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
            await this.DisposeConnectionAsync(conn);

            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_StopMonitoringThread, this.initialHostSpec.Host));
        }
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        if (Interlocked.Read(ref this.ignoreNewTopologyRequestsEndTime) > DateTime.UtcNow.Ticks)
        {
            // Previous failover has just completed. We can use results of it without triggering a new topology update.
            if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts) && currentHosts != null)
            {
                Logger.LogTrace(LoggerUtils.LogTopology(currentHosts, Resources.ClusterTopologyMonitor_IgnoringTopologyRequest));
                return currentHosts;
            }
        }

        if (shouldVerifyWriter)
        {
            Logger.LogTrace("Monitoring connection@{Id} is set to null", RuntimeHelpers.GetHashCode(this.monitoringConnection));
            var connectionToClose = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.isVerifiedWriterConnection = false;
            await this.DisposeConnectionAsync(connectionToClose);
        }

        return this.WaitTillTopologyGetsUpdated(timeoutMs);
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(DbConnection? connection, long timeoutMs)
    {
        if (this.isVerifiedWriterConnection)
        {
            // Push monitoring thread to refresh topology with a verified connection
            return this.WaitTillTopologyGetsUpdated(timeoutMs);
        }

        // Otherwise use provided unverified connection to update topology
        return await this.FetchTopologyAndUpdateCacheAsync(connection) ?? [];
    }

    protected IList<HostSpec> WaitTillTopologyGetsUpdated(long timeoutMs)
    {
        this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts);
        Logger.LogTrace(LoggerUtils.LogTopology(currentHosts, $"Wait until current topology is updated:"));

        lock (this.topologyUpdatedLock)
        {
            this.requestToUpdateTopology = true;
        }

        if (timeoutMs == 0)
        {
            Logger.LogTrace(LoggerUtils.LogTopology(currentHosts, Resources.ClusterTopologyMonitor_TimeoutSetToZero));
            return currentHosts ?? [];
        }

        DateTime endTime = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        IList<HostSpec>? latestHosts = [];
        lock (this.topologyUpdatedLock)
        {
            while (this.topologyMap.TryGetValue(this.clusterId, out latestHosts) &&
                ReferenceEquals(currentHosts, latestHosts) && DateTime.UtcNow < endTime)
            {
                Logger.LogTrace("Waiting on topologyUpdatedLock...");
                if (Monitor.Wait(this.topologyUpdatedLock, 1000))
                {
                    Logger.LogTrace("Woke up from Monitor. Wait due to Pulse, rechecking condition...");
                    if (this.topologyMap.TryGetValue(this.clusterId, out latestHosts) &&
                        !ReferenceEquals(currentHosts, latestHosts))
                    {
                        Logger.LogTrace(LoggerUtils.LogTopology(latestHosts, $"Topology is updated:"));
                        return latestHosts ?? [];
                    }
                    else
                    {
                        Logger.LogTrace(LoggerUtils.LogTopology(latestHosts, $"Topology is still not updated:"));
                    }
                }
                else
                {
                    Logger.LogTrace("Woke up from Monitor. Wait due to timeout, rechecking condition...");
                }
            }
        }

        if (DateTime.UtcNow > endTime)
        {
            throw new TimeoutException(string.Format(Resources.ClusterTopologyMonitor_TopologyNotUpdated, timeoutMs));
        }

        Logger.LogTrace(LoggerUtils.LogTopology(latestHosts, $"Topology is updated:"));
        return latestHosts ?? [];
    }

    protected bool IsInPanicMode()
    {
        return this.monitoringConnection == null || !this.isVerifiedWriterConnection;
    }

    protected async Task<IList<HostSpec>?> OpenAnyConnectionAndUpdateTopologyAsync()
    {
        bool writerVerifiedByThisThread = false;
        if (this.monitoringConnection == null)
        {
            DbConnection? newConnection = null;

            try
            {
                newConnection = await this.pluginService.ForceOpenConnection(this.initialHostSpec, this.properties, null, true);

                if (Interlocked.CompareExchange(ref this.monitoringConnection, newConnection, null) == null)
                {
                    Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_OpenedMonitoringConnection, RuntimeHelpers.GetHashCode(this.monitoringConnection), this.initialHostSpec.Host));

                    if (!string.IsNullOrEmpty(await this.GetWriterNodeIdAsync(this.monitoringConnection)))
                    {
                        this.isVerifiedWriterConnection = true;
                        writerVerifiedByThisThread = true;
                        if (RdsUtils.IsRdsInstance(this.initialHostSpec.Host))
                        {
                            Logger.LogTrace("{host} IS a rds instance", this.initialHostSpec.Host);
                            this.writerHostSpec = this.initialHostSpec;
                            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_WriterMonitoringConnection, this.writerHostSpec));
                        }
                        else
                        {
                            Logger.LogTrace("{host} IS NOT a rds instance", this.initialHostSpec.Host);
                            string? nodeId = await this.GetNodeIdAsync(this.monitoringConnection);
                            if (!string.IsNullOrEmpty(nodeId))
                            {
                                this.writerHostSpec = this.CreateHost(nodeId, nodeId, true, 0, null);
                                Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_WriterMonitoringConnection, this.writerHostSpec));
                            }
                        }
                    }
                }

                // Don't close this connection - it's now the monitoring connection
                newConnection = null;
            }
            catch (DbException ex)
            {
                // Suppress connection errors and continue
                Logger.LogWarning(ex, "DbException thrown during finding a monitoring connection, and ignored.");
            }
            finally
            {
                // Clean up if this thread lost the race or if there was an exception
                if (newConnection != null)
                {
                    try
                    {
                        await this.DisposeConnectionAsync(newConnection);
                    }
                    catch
                    {
                        // Ignore cleanup errors
                    }
                }
            }
        }

        IList<HostSpec>? hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);

        // TODO: Generate correct writer host spec with node name query
        // Workaround: Update the real host for multiaz cluster
        Logger.LogTrace("Writer host before update: {writerHostSpec}", this.writerHostSpec);
        if (this.writerHostSpec is not null)
        {
            var writerHostSpecWithCorrectHost = this.topologyMap.Get<IList<HostSpec>>(this.clusterId)?.FirstOrDefault(h => h.HostId == this.writerHostSpec.HostId) ?? this.writerHostSpec;
            this.writerHostSpec = this.hostListProviderService.HostSpecBuilder
                .CopyFrom(this.writerHostSpec)
                .WithHost(writerHostSpecWithCorrectHost.Host)
                .Build();
        }

        Logger.LogTrace("Writer host after update: {writerHostSpec}", this.writerHostSpec);

        if (writerVerifiedByThisThread)
        {
            // We verify the writer on initial connection and on failover, but we only want to ignore new topology
            // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
            // time to 0. Any future writer verifications will set it to a positive value.
            if (Interlocked.CompareExchange(ref this.ignoreNewTopologyRequestsEndTime, 0, -1) != -1)
            {
                Interlocked.Exchange(ref this.ignoreNewTopologyRequestsEndTime, DateTime.UtcNow.Add(IgnoreTopologyRequestDuration).Ticks);
                Logger.LogTrace("Ignoring topology request until {utcTime}", new DateTime(this.ignoreNewTopologyRequestsEndTime, DateTimeKind.Utc));
            }
        }

        if (hosts == null)
        {
            Logger.LogTrace("Monitoring connection@{Id} is set to null", RuntimeHelpers.GetHashCode(this.monitoringConnection));
            var connToDispose = Interlocked.Exchange(ref this.monitoringConnection, null);

            this.isVerifiedWriterConnection = false;
            await this.DisposeConnectionAsync(connToDispose);
        }

        return hosts;
    }

    protected virtual HostSpec CreateHost(DbDataReader reader, string? suggestedWriterNodeId)
    {
        string hostName = reader.GetString(0);
        bool isWriter = reader.GetBoolean(1);
        double cpuUtilization = reader.GetDouble(2);
        double nodeLag = reader.GetDouble(3);
        DateTime lastUpdateTime = reader.IsDBNull(4)
            ? DateTime.UtcNow
            : reader.GetDateTime(4);

        long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));

        return this.CreateHost(hostName, hostName, isWriter, weight, lastUpdateTime);
    }

    protected HostSpec CreateHost(
        string nodeName,
        string nodeId,
        bool isWriter,
        long weight,
        DateTime? lastUpdateTime)
    {
        string endpoint = this.clusterInstanceTemplate.Host.Replace("?", nodeName);
        int port = this.clusterInstanceTemplate.IsPortSpecified
            ? this.clusterInstanceTemplate.Port
            : this.initialHostSpec.Port;

        HostSpec host = this.hostListProviderService.HostSpecBuilder
            .WithHost(endpoint)
            .WithHostId(nodeId)
            .WithPort(port)
            .WithRole(isWriter ? HostRole.Writer : HostRole.Reader)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithLastUpdateTime(lastUpdateTime)
            .Build();

        host.AddAlias(nodeName);
        return host;
    }

    protected async Task<string?> GetNodeIdAsync(DbConnection connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = this.nodeIdQuery;
            await using var reader = await command.ExecuteReaderAsync(this.ctsTopologyMonitoring.Token);
            if (await reader.ReadAsync(this.ctsTopologyMonitoring.Token))
            {
                return await reader.IsDBNullAsync(0)
                    ? null
                    : Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture);
            }
        }
        catch (Exception ex)
        {
            // Ignore
            Logger.LogWarning(ex, "Exception thrown during getting the node Id for topology monitoring connection@{id}, and ignored.", RuntimeHelpers.GetHashCode(connection));
            throw;
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }

        return null;
    }

    protected virtual async Task<string?> GetWriterNodeIdAsync(DbConnection connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = this.writerTopologyQuery;
            await using var reader = await command.ExecuteReaderAsync(this.ctsTopologyMonitoring.Token);
            if (await reader.ReadAsync(this.ctsTopologyMonitoring.Token))
            {
                return reader.GetString(0);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Exception thrown during getting the writer node Id for topology monitoring connection@{id}", RuntimeHelpers.GetHashCode(connection));
            throw;
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }

        return null;
    }

    protected virtual Task<string?> GetSuggestedWriterNodeIdAsync(DbConnection connection) => Task.FromResult<string?>(null);

    protected async Task DisposeConnectionAsync(DbConnection? connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            if (connection != null)
            {
                Logger.LogTrace("Connection {Type}@{Id} is being disposed.", connection.GetType().FullName, RuntimeHelpers.GetHashCode(connection));
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }
    }

    protected async Task DelayAsync(bool useHighRefreshRate)
    {
        if (this.highRefreshRateEndTime > DateTime.UtcNow && this.highRefreshRateEndTime < DateTime.UtcNow)
        {
            useHighRefreshRate = true;
        }

        if (this.requestToUpdateTopology)
        {
            useHighRefreshRate = true;
        }

        var delay = useHighRefreshRate ? this.highRefreshRate : this.refreshRate;
        var start = DateTime.UtcNow;
        var end = start.Add(delay);

        do
        {
            await Task.Delay(50, this.ctsTopologyMonitoring.Token);
        }
        while (!this.requestToUpdateTopology && DateTime.UtcNow < end && !this.ctsTopologyMonitoring.Token.IsCancellationRequested);
    }

    protected async Task<IList<HostSpec>?> FetchTopologyAndUpdateCacheAsync(DbConnection? connection)
    {
        if (connection == null)
        {
            return null;
        }

        try
        {
            var hosts = await this.QueryForTopologyAsync(connection);
            Logger.LogTrace(LoggerUtils.LogTopology(hosts, $"UpdateTopologyCache:"));
            if (hosts?.Count > 0)
            {
                this.UpdateTopologyCache(hosts);
            }

            return hosts;
        }
        catch (DbException ex)
        {
            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_ErrorFetchingTopology, ex));
            return null;
        }
    }

    protected void UpdateTopologyCache(IList<HostSpec> hosts)
    {
        lock (this.topologyUpdatedLock)
        {
            this.topologyMap.Set(this.clusterId, hosts, this.topologyCacheExpiration);
            this.requestToUpdateTopology = false;
            Logger.LogTrace("Notifying topologyUpdatedLock...");
            Monitor.PulseAll(this.topologyUpdatedLock);
        }
    }

    protected async Task<IList<HostSpec>?> QueryForTopologyAsync(DbConnection connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            string? suggestedWriterNodeId = await this.GetSuggestedWriterNodeIdAsync(connection);

            await using var command = connection.CreateCommand();
            if (command.CommandTimeout == 0)
            {
                command.CommandTimeout = DefaultTopologyQueryTimeoutSec;
            }

            Logger.LogTrace("Command timeout for querying topology: {time} seconds", command.CommandTimeout);

            command.CommandText = this.topologyQuery;
            await using var reader = await command.ExecuteReaderAsync(this.ctsTopologyMonitoring.Token);

            var hosts = new List<HostSpec>();
            var writers = new List<HostSpec>();

            while (await reader.ReadAsync(this.ctsTopologyMonitoring.Token))
            {
                try
                {
                    HostSpec hostSpec = this.CreateHost(reader, suggestedWriterNodeId);
                    (hostSpec.Role == HostRole.Writer ? writers : hosts).Add(hostSpec);
                }
                catch (Exception ex)
                {
                    Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_ErrorProcessingQueryResults, ex));
                    return null;
                }
            }

            if (writers.Count == 0)
            {
                Logger.LogWarning(Resources.ClusterTopologyMonitor_InvalidTopology);
                hosts.Clear();
            }
            else if (writers.Count == 1)
            {
                hosts.Add(writers[0]);
            }
            else
            {
                // Take the latest updated writer node as the current writer
                hosts.Add(writers.MaxBy(x => x.LastUpdateTime)!);
            }

            return hosts;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, string.Format(Resources.ClusterTopologyMonitor_ErrorProcessingQueryResults, ex));
            return null;
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }
    }

    public void Dispose()
    {
        Logger.LogTrace("Disposing cluster topology monitor for host {host}", this.initialHostSpec.Host);
        lock (this.disposeLock)
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
        }

        this.nodeThreadsStop = true;
        this.ctsTopologyMonitoring.Cancel();

        try
        {
            this.monitoringTask.Wait(TimeSpan.FromSeconds(30));
        }
        catch (Exception ex)
        {
            Logger.LogWarning("Error occourred when waiting on node monitoring thread to finish: {message}", ex.Message);
        }

        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
        conn?.Dispose();

        var readerConn = Interlocked.Exchange(ref this.nodeThreadsReaderConnection, null);
        readerConn?.Dispose();

        var writerConn = Interlocked.Exchange(ref this.nodeThreadsWriterConnection, null);
        writerConn?.Dispose();

        this.ctsTopologyMonitoring.Dispose();
    }

    private class NodeMonitoringTask(
        ClusterTopologyMonitor monitor,
        HostSpec hostSpec,
        HostSpec? writerHostSpec)
    {
        private static readonly ILogger<NodeMonitoringTask> NodeMonitorLogger = LoggerUtils.GetLogger<NodeMonitoringTask>();

        private bool writerChanged = false;

        public async Task RunNodeMonitoringAsync(CancellationToken token)
        {
            DbConnection? connection = null;
            bool updateTopology = false;
            DateTime start = DateTime.UtcNow;

            LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, "Start running node monitoring task");
            try
            {
                while (!monitor.nodeThreadsStop && !token.IsCancellationRequested)
                {
                    if (connection == null)
                    {
                        try
                        {
                            connection = await monitor.pluginService.ForceOpenConnection(hostSpec, monitor.properties, null, true);
                            monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Available);
                        }
                        catch (DbException)
                        {
                            monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Unavailable);
                            await monitor.DisposeConnectionAsync(connection);
                            connection = null;
                        }
                    }

                    if (connection != null)
                    {
                        string? writerId = null;
                        try
                        {
                            writerId = await monitor.GetWriterNodeIdAsync(connection);
                        }
                        catch (DbException)
                        {
                            await monitor.DisposeConnectionAsync(connection);
                            connection = null;
                        }

                        if (!string.IsNullOrEmpty(writerId))
                        {
                            if (Interlocked.CompareExchange(ref monitor.nodeThreadsWriterConnection, connection, null) == null)
                            {
                                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Information, string.Format(Resources.NodeMonitoringTask_DetectedWriter, writerId));
                                await monitor.FetchTopologyAndUpdateCacheAsync(connection);
                                monitor.nodeThreadsWriterHostSpec = hostSpec;
                                monitor.nodeThreadsStop = true;
                            }
                            else
                            {
                                await monitor.DisposeConnectionAsync(connection);
                            }

                            connection = null; // Prevent disposal
                            return;
                        }
                        else if (connection != null)
                        {
                            // Reader connection
                            if (monitor.nodeThreadsWriterConnection == null)
                            {
                                if (updateTopology)
                                {
                                    await this.ReaderThreadFetchTopologyAsync(connection);
                                }
                                else if (Interlocked.CompareExchange(ref monitor.nodeThreadsReaderConnection, connection, null) == null)
                                {
                                    updateTopology = true;
                                    await this.ReaderThreadFetchTopologyAsync(connection);
                                }
                            }
                        }
                    }

                    await Task.Delay(100, token);
                }
            }
            catch (OperationCanceledException ex)
            {
                // Expected
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, ex, "Operation canncelled: {message}", ex.Message);
            }
            catch (Exception ex)
            {
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Warning, ex, "Unknown exception thrown: {message}", ex.Message);
            }
            finally
            {
                await monitor.DisposeConnectionAsync(connection);
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, string.Format(Resources.NodeMonitoringTask_ThreadCompleted, RuntimeHelpers.GetHashCode(this), (DateTime.UtcNow - start).TotalMilliseconds));
            }
        }

        protected async Task ReaderThreadFetchTopologyAsync(DbConnection connection)
        {
            try
            {
                var hosts = await monitor.QueryForTopologyAsync(connection);
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, LoggerUtils.LogTopology(hosts, $"UpdateTopologyCache:"));
                if (hosts == null)
                {
                    return;
                }

                if (this.writerChanged)
                {
                    monitor.UpdateTopologyCache(hosts);
                    return;
                }

                monitor.nodeThreadsLatestTopology = hosts;
                var latestWriterHostSpec = hosts.FirstOrDefault(x => x.Role == HostRole.Writer);

                if (latestWriterHostSpec != null && writerHostSpec != null &&
                    latestWriterHostSpec.GetHostAndPort() != writerHostSpec.GetHostAndPort())
                {
                    LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, string.Format(
                        Resources.NodeMonitoringTask_WriterNodeChanged,
                        writerHostSpec.Host,
                        latestWriterHostSpec.Host));

                    this.writerChanged = true;
                    monitor.UpdateTopologyCache(hosts);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, ex, "Exception caught but ignored: {message}", ex.Message);
            }
        }
    }
}
