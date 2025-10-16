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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitor : IHostMonitor
{
    private static readonly ILogger<HostMonitor> Logger = LoggerUtils.GetLogger<HostMonitor>();
    private static readonly int ThreadSleepMs = 100;
    private static readonly string MonitoringPropertyPrefix = "monitoring-";

    private readonly ConcurrentQueue<WeakReference<HostMonitorConnectionContext>> activeContexts = new();
    private readonly MemoryCache newContexts = new(new MemoryCacheOptions());
    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> properties;
    private readonly HostSpec hostSpec;
    private readonly int failureDetectionTimeMs;
    private readonly int failureDetectionIntervalMs;
    private readonly int failureDetectionCount;
    private readonly CancellationTokenSource cancellationTokenSource = new();
    private readonly Task runTask;
    private readonly Task newContextRunTask;

    private readonly object monitorLock = new();
    private DateTime? invalidNodeStartTime = null;
    private volatile int failureCount = 0;
    private volatile bool nodeUnhealthy = false;
    private DbConnection? monitoringConn = null;

    internal volatile bool TestUnhealthyCluster = false;

    public int FailureCount { get => this.failureCount; }

    /// <summary>
    /// Initializes a new instance of the <see cref="HostMonitor"/> class.
    /// </summary>
    /// <param name="pluginService">A service for creating new connections.</param>
    /// <param name="hostSpec">The HostSpec of the server this HostMonitorImpl instance is monitoring.</param>
    /// <param name="properties">The Properties containing additional monitoring configuration.</param>
    /// <param name="failureDetectionTimeMs">A failure detection time in millis.</param>
    /// <param name="failureDetectionIntervalMs">A failure detection interval in millis.</param>
    /// <param name="failureDetectionCount">A failure detection count.</param>
    public HostMonitor(
        IPluginService pluginService,
        HostSpec hostSpec,
        Dictionary<string, string> properties,
        int failureDetectionTimeMs,
        int failureDetectionIntervalMs,
        int failureDetectionCount)
    {
        this.pluginService = pluginService;
        this.hostSpec = hostSpec;
        this.properties = properties;
        this.failureDetectionTimeMs = failureDetectionTimeMs;
        this.failureDetectionIntervalMs = failureDetectionIntervalMs;
        this.failureDetectionCount = failureDetectionCount;

        this.newContextRunTask = Task.Run(() => this.NewContextRun(this.cancellationTokenSource.Token));
        this.runTask = Task.Run(() => this.Run(this.cancellationTokenSource.Token));
    }

    public void StartMonitoring(HostMonitorConnectionContext context)
    {
        if (this.cancellationTokenSource.Token.IsCancellationRequested)
        {
            Logger.LogWarning(Resources.EfmHostMonitor_StartMonitoringWhenStopped);
            return;
        }

        DateTime startMonitoringTime = DateTime.UtcNow.AddMilliseconds(this.failureDetectionTimeMs);

        DateTime truncated = new(
            startMonitoringTime.Year,
            startMonitoringTime.Month,
            startMonitoringTime.Day,
            startMonitoringTime.Hour,
            startMonitoringTime.Minute,
            startMonitoringTime.Second,
            startMonitoringTime.Kind);

        ConcurrentQueue<WeakReference<HostMonitorConnectionContext>>? queue = this.newContexts.GetOrCreate(truncated,
            (key) => new ConcurrentQueue<WeakReference<HostMonitorConnectionContext>>());

        queue!.Enqueue(new WeakReference<HostMonitorConnectionContext>(context));
    }

    public bool CanDispose()
    {
        return this.activeContexts.IsEmpty && this.newContexts.Count == 0;
    }

    public void Dispose()
    {
        this.cancellationTokenSource.Cancel();

        Task.WaitAll([this.newContextRunTask, this.runTask], TimeSpan.FromSeconds(30));
        this.newContextRunTask.Dispose();
        this.runTask.Dispose();

        if (this.newContexts.Count > 0)
        {
            this.newContexts.Clear();
        }

        if (!this.activeContexts.IsEmpty)
        {
            this.activeContexts.Clear();
        }

        Logger.LogTrace(string.Format(Resources.EfmHostMonitor_StoppedMonitoring, this.hostSpec.Host));
    }

    public async void NewContextRun(CancellationToken token)
    {
        Logger.LogTrace(string.Format(Resources.EfmHostMonitor_StartedPollingNewContexts, this.hostSpec.Host));

        try
        {
            while (!token.IsCancellationRequested)
            {
                DateTime currentTime = DateTime.UtcNow;

                List<DateTime> processedStartTimes = [];

                foreach (DateTime startTime in this.newContexts.Keys.Cast<DateTime>().Where(startTime => startTime < currentTime))
                {
                    if (this.newContexts.TryGetValue(startTime, out ConcurrentQueue<WeakReference<HostMonitorConnectionContext>>? queue) && queue != null)
                    {
                        processedStartTimes.Add(startTime);

                        while (queue.TryDequeue(out WeakReference<HostMonitorConnectionContext>? contextRef))
                        {
                            if (contextRef != null
                                && contextRef.TryGetTarget(out HostMonitorConnectionContext? context)
                                && context != null
                                && context.IsActive())
                            {
                                Logger.LogTrace("Adding active monitoring context to poll");
                                this.activeContexts.Enqueue(contextRef);
                            }
                        }
                    }
                }

                processedStartTimes.ForEach(key => this.newContexts.Remove(key));

                // sleep for one second before polling new contexts
                await Task.Delay(1000, token);
            }
        }
        catch (OperationCanceledException)
        {
            // pass
        }
        catch (Exception ex)
        {
            Logger.LogWarning(string.Format(Resources.EfmHostMonitor_NewContextsException, this.hostSpec.Host, ex.Message, ex.StackTrace));
        }

        Logger.LogTrace(string.Format(Resources.EfmHostMonitor_StoppedPollingNewContexts, this.hostSpec.Host));
    }

    public async void Run(CancellationToken token)
    {
        Logger.LogTrace(string.Format(Resources.EfmHostMonitor_StartedMonitoringActiveContexts, this.hostSpec.Host));

        try
        {
            while (!token.IsCancellationRequested)
            {
                bool isNodeUnhealthy;

                lock (this.monitorLock)
                {
                    isNodeUnhealthy = this.nodeUnhealthy;
                }

                if (this.activeContexts.IsEmpty && !isNodeUnhealthy)
                {
                    await Task.Delay(ThreadSleepMs, token);
                    Logger.LogTrace("No active contexts and node is healthy, skipping status check");
                    continue;
                }

                Logger.LogTrace("Current active contexts count: {count}", this.activeContexts.Count);
                DateTime statusCheckStartTime = DateTime.UtcNow;
                bool isValid = await this.CheckConnectionStatus();
                DateTime statusCheckEndTime = DateTime.UtcNow;

                this.UpdateNodeHealthStatus(isValid, statusCheckStartTime, statusCheckEndTime);

                List<WeakReference<HostMonitorConnectionContext>> tmpActiveContexts = [];

                lock (this.monitorLock)
                {
                    while (this.activeContexts.TryDequeue(out WeakReference<HostMonitorConnectionContext>? monitorContextRef))
                    {
                        Logger.LogTrace("Dequeued a context from activeContexts");
                        if (token.IsCancellationRequested)
                        {
                            break;
                        }

                        if (!monitorContextRef.TryGetTarget(out HostMonitorConnectionContext? monitorContext) || monitorContext == null)
                        {
                            continue;
                        }

                        if (this.nodeUnhealthy)
                        {
                            monitorContext.NodeUnhealthy = true;
                            DbConnection? connectionToAbort = monitorContext.GetConnection();
                            monitorContext.SetInactive();

                            if (connectionToAbort != null)
                            {
                                this.AbortConnection(connectionToAbort);
                            }
                        }
                        else if (monitorContext.IsActive())
                        {
                            Logger.LogTrace("Adding context to tmpActiveContexts");
                            tmpActiveContexts.Add(monitorContextRef);
                        }
                    }

                    // this.activeContexts is now empty, and tmpActiveContexts contains all still active contexts
                    // add those back into this.activeContexts
                    foreach (WeakReference<HostMonitorConnectionContext> contextRef in tmpActiveContexts)
                    {
                        Logger.LogTrace("Adding context back to activeContexts");
                        this.activeContexts.Enqueue(contextRef);
                    }
                }

                int delayMs = this.failureDetectionIntervalMs - (int)(statusCheckEndTime - statusCheckStartTime).TotalMilliseconds;
                if (delayMs < ThreadSleepMs)
                {
                    delayMs = ThreadSleepMs;
                }

                await Task.Delay(delayMs, token);
            }
        }
        catch (OperationCanceledException)
        {
            // ignore
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, string.Format(Resources.EfmHostMonitor_ActiveContextsException, this.hostSpec.Host, ex.Message, ex.StackTrace));
        }
        finally
        {
            lock (this.monitorLock)
            {
                if (!this.cancellationTokenSource.Token.IsCancellationRequested)
                {
                    this.cancellationTokenSource.Cancel();
                }

                if (this.monitoringConn != null)
                {
                    try
                    {
                        this.monitoringConn.Close();
                    }
                    catch
                    {
                        // ignore
                    }
                }
            }
        }

        Logger.LogTrace(string.Format(Resources.EfmHostMonitor_StoppedMonitoringActiveContexts, this.hostSpec.Host));
    }

    private async Task<bool> CheckConnectionStatus()
    {
        try
        {
            DbConnection? conn;

            lock (this.monitorLock)
            {
                conn = this.monitoringConn;
            }

            if (conn == null || conn.State == System.Data.ConnectionState.Closed)
            {
                Dictionary<string, string> monitoringConnProperties = new(this.properties);

                foreach (string key in this.properties.Keys)
                {
                    if (key.StartsWith(MonitoringPropertyPrefix))
                    {
                        monitoringConnProperties[key[MonitoringPropertyPrefix.Length..]] = this.properties[key];
                        monitoringConnProperties.Remove(key);
                    }
                }

                Logger.LogTrace(string.Format(Resources.EfmHostMonitor_OpeningMonitoringConnection, this.hostSpec.Host));
                conn = await this.pluginService.ForceOpenConnection(this.hostSpec, monitoringConnProperties, null, true);
                Logger.LogTrace(string.Format(Resources.EfmHostMonitor_OpenedMonitoringConnection, this.hostSpec.Host));

                lock (this.monitorLock)
                {
                    this.monitoringConn?.Dispose();
                    this.monitoringConn = conn;
                    conn = null;
                }

                return true;
            }

            using (var validityCheckCommand = this.monitoringConn!.CreateCommand())
            {
                validityCheckCommand.CommandText = "SELECT 1";
                int validTimeoutSeconds = Math.Max((this.failureDetectionIntervalMs - ThreadSleepMs) / 2000, 1);
                Logger.LogTrace($"Command timeout for ping is {validTimeoutSeconds} seconds");
                validityCheckCommand.CommandTimeout = validTimeoutSeconds;
                validityCheckCommand.ExecuteScalar();
            }

            return !this.TestUnhealthyCluster;
        }
        catch (DbException ex)
        {
            Logger.LogWarning(ex, "Disposing invalid monitoring connection");
            this.monitoringConn?.Dispose();
            this.monitoringConn = null;
            return false;
        }
    }

    private void UpdateNodeHealthStatus(bool connectionValid, DateTime statusCheckStartTime, DateTime statusCheckEndTime)
    {
        if (!connectionValid)
        {
            DateTime invalidNodeDurationStart;

            lock (this.monitorLock)
            {
                this.failureCount++;

                if (this.invalidNodeStartTime == null)
                {
                    this.invalidNodeStartTime = statusCheckStartTime;
                }

                invalidNodeDurationStart = this.invalidNodeStartTime ?? statusCheckStartTime;
            }

            TimeSpan invalidNodeDuration = statusCheckEndTime - invalidNodeDurationStart;
            int maxInvalidNodeDurationMs = this.failureDetectionIntervalMs * Math.Max(0, this.failureDetectionCount - 1);

            if (invalidNodeDuration >= TimeSpan.FromMilliseconds(maxInvalidNodeDurationMs))
            {
                Logger.LogTrace(string.Format(Resources.EfmHostMonitor_HostDead, this.hostSpec.Host));
                lock (this.monitorLock)
                {
                    this.nodeUnhealthy = true;
                }
            }
            else
            {
                lock (this.monitorLock)
                {
                    Logger.LogTrace(string.Format(Resources.EfmHostMonitor_HostNotResponding, this.hostSpec.Host, this.failureCount));
                }
            }

            return;
        }

        lock (this.monitorLock)
        {
            if (this.failureCount > 0)
            {
                Logger.LogTrace(string.Format(Resources.EfmHostMonitor_HostAlive, this.hostSpec.Host));
            }

            this.failureCount = 0;
            this.invalidNodeStartTime = null;
            this.nodeUnhealthy = false;
        }
    }

    private void AbortConnection(DbConnection connection)
    {
        Logger.LogTrace("Aborting unhealthy connection.");
        try
        {
            connection.Close();
            Logger.LogTrace("Finished aborting unhealthy connection.");
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, string.Format(Resources.EfmHostMonitor_ExceptionAbortingConnection, ex.Message));
        }
    }
}
