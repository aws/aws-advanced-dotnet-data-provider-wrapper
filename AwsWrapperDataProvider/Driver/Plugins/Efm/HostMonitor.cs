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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitor : IHostMonitor
{
    private static readonly ILogger<HostMonitor> Logger = LoggerUtils.GetLogger<HostMonitor>();
    private static readonly int ThreadSleepMillis = 100;
    private static readonly string MonitoringPropertyPrefix = "monitoring-";

    private readonly ConcurrentQueue<WeakReference<HostMonitorConnectionContext>> activeContexts = new();
    private readonly ConcurrentDictionary<int, ConcurrentQueue<WeakReference<HostMonitorConnectionContext>>> newContexts = new();
    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> properties;
    private readonly HostSpec hostSpec;
    private readonly int failureDetectionTimeMillis;
    private readonly int failureDetectionIntervalMillis;
    private readonly int failureDetectionCount;
    private readonly CancellationTokenSource cts = new();
    private readonly Task runTask;
    private readonly Task newContextRunTask;

    private DateTime? invalidNodeStartTime = null;
    private volatile int failureCount = 0;
    private volatile bool stopped = false;
    private volatile bool nodeUnhealthy = false;
    private DbConnection? monitoringConn = null;

    /// <summary>
    /// Initializes a new instance of the <see cref="HostMonitor"/> class.
    /// </summary>
    /// <param name="pluginService">A service for creating new connections.</param>
    /// <param name="hostSpec">The HostSpec of the server this HostMonitorImpl instance is monitoring.</param>
    /// <param name="properties">The Properties containing additional monitoring configuration.</param>
    /// <param name="failureDetectionTimeMillis">A failure detection time in millis.</param>
    /// <param name="failureDetectionIntervalMillis">A failure detection interval in millis.</param>
    /// <param name="failureDetectionCount">A failure detection count.</param>
    public HostMonitor(
        IPluginService pluginService,
        HostSpec hostSpec,
        Dictionary<string, string> properties,
        int failureDetectionTimeMillis,
        int failureDetectionIntervalMillis,
        int failureDetectionCount)
    {
        this.pluginService = pluginService;
        this.hostSpec = hostSpec;
        this.properties = properties;
        this.failureDetectionTimeMillis = failureDetectionTimeMillis;
        this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
        this.failureDetectionCount = failureDetectionCount;

        this.newContextRunTask = Task.Run(() => this.NewContextRun(this.cts.Token));
        this.runTask = Task.Run(() => this.Run(this.cts.Token));
    }

    public void StartMonitoring(HostMonitorConnectionContext context)
    {
        if (this.stopped)
        {
            Logger.LogWarning("Starting monitoring for a monitor that is stopped.");
        }

        DateTime startMonitoringTime = DateTime.Now + TimeSpan.FromMilliseconds(this.failureDetectionTimeMillis);
        int startMonitoringTimeSeconds = (int)(startMonitoringTime - DateTime.UnixEpoch).TotalSeconds;

        ConcurrentQueue<WeakReference<HostMonitorConnectionContext>> queue = this.newContexts.GetOrAdd(
            startMonitoringTimeSeconds, (_) => new());

        queue.Enqueue(new WeakReference<HostMonitorConnectionContext>(context));
    }

    public bool CanDispose()
    {
        return this.activeContexts.IsEmpty && this.newContexts.IsEmpty;
    }

    public void Close()
    {
        this.stopped = true;
        this.cts.Cancel();

        Task.WaitAll([this.newContextRunTask, this.runTask], (int)TimeSpan.FromSeconds(30).TotalMilliseconds);
        this.newContextRunTask.Dispose();
        this.runTask.Dispose();

        if (!this.newContexts.IsEmpty)
        {
            this.newContexts.Clear();
        }

        if (!this.activeContexts.IsEmpty)
        {
            this.activeContexts.Clear();
        }

        Logger.LogInformation($"Stopped monitoring for {this.hostSpec.Host}.");
    }

    public async void NewContextRun(CancellationToken token)
    {
        Logger.LogInformation($"Started monitoring thread to poll new contexts for {this.hostSpec.Host}.");

        try
        {
            while (!this.stopped)
            {
                token.ThrowIfCancellationRequested();
                DateTime currentTime = DateTime.Now;

                List<int> processedKeys = [];

                foreach (int key in this.newContexts.Keys)
                {
                    if (DateTime.UnixEpoch.AddSeconds(key) < currentTime)
                    {
                        ConcurrentQueue<WeakReference<HostMonitorConnectionContext>> queue = this.newContexts[key];
                        processedKeys.Add(key);

                        while (queue.TryDequeue(out WeakReference<HostMonitorConnectionContext>? contextRef))
                        {
                            if (contextRef != null
                                && contextRef.TryGetTarget(out HostMonitorConnectionContext? context)
                                && context != null
                                && context.IsActive())
                            {
                                this.activeContexts.Enqueue(contextRef);
                            }
                        }
                    }
                }

                foreach (int key in processedKeys)
                {
                    this.newContexts.Remove(key, out _);
                }

                // sleep for one second before polling new contexts
                await Task.Delay(1000);
            }
        }
        catch (OperationCanceledException)
        {
            // pass
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Exception thrown while polling new contexts for {this.hostSpec.Host}: {ex.Message} {ex.StackTrace}");
        }

        Logger.LogInformation($"Stopped monitoring thread to poll new contexts for {this.hostSpec.Host}.");
    }

    public async void Run(CancellationToken token)
    {
        Logger.LogInformation($"Started monitoring thread to monitor active contexts for host {this.hostSpec.Host}.");

        try
        {
            while (!this.stopped)
            {
                token.ThrowIfCancellationRequested();

                if (this.activeContexts.IsEmpty && !this.nodeUnhealthy)
                {
                    await Task.Delay(ThreadSleepMillis);
                    continue;
                }

                DateTime statusCheckStartTime = DateTime.Now;
                bool isValid = this.CheckConnectionStatus();
                DateTime statusCheckEndTime = DateTime.Now;

                this.UpdateNodeHealthStatus(isValid, statusCheckStartTime, statusCheckEndTime);

                WeakReference<HostMonitorConnectionContext>[] tmpActiveContexts = [];

                while (this.activeContexts.TryDequeue(out WeakReference<HostMonitorConnectionContext>? monitorContextRef))
                {
                    token.ThrowIfCancellationRequested();
                    if (this.stopped)
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
                        tmpActiveContexts.Append(monitorContextRef);
                    }
                }

                // this.activeContexts is now empty, and tmpActiveContexts contains all still active contexts
                // add those back into this.activeContexts
                foreach (WeakReference<HostMonitorConnectionContext> contextRef in tmpActiveContexts)
                {
                    this.activeContexts.Enqueue(contextRef);
                }

                int delayMillis = this.failureDetectionIntervalMillis - (int)(statusCheckEndTime - statusCheckStartTime).TotalMilliseconds;
                if (delayMillis < ThreadSleepMillis)
                {
                    delayMillis = ThreadSleepMillis;
                }

                await Task.Delay(delayMillis);
            }
        }
        catch (OperationCanceledException)
        {
            // ignore
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Exception thrown while monitoring active contexts for {this.hostSpec.Host}: {ex.Message} {ex.StackTrace}");
        }
        finally
        {
            this.stopped = true;
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

        Logger.LogInformation($"Stopped monitoring thread to monitor active contexts for {this.hostSpec.Host}.");
    }

    private bool CheckConnectionStatus()
    {
        try
        {
            if (this.monitoringConn == null || this.monitoringConn.State == System.Data.ConnectionState.Closed)
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

                Logger.LogInformation($"Opening a monitoring connection to {this.hostSpec.Host}...");
                this.monitoringConn = this.pluginService.ForceOpenConnection(this.hostSpec, monitoringConnProperties, false);
                Logger.LogInformation($"Opened a monitoring connection to {this.hostSpec.Host}");

                return true;
            }

            try
            {
                DbCommand validityCheckCommand = this.monitoringConn.CreateCommand();
                validityCheckCommand.CommandText = "SELECT 1";

                // JDBC: Some drivers, like MySQL Connector/J, execute isValid() in a double of specified timeout time.
                int validTimeoutSeconds = (this.failureDetectionIntervalMillis - ThreadSleepMillis) / 2000;
                validityCheckCommand.CommandTimeout = validTimeoutSeconds;

                _ = validityCheckCommand.ExecuteScalar();

                // was able to execute command within the timeout - connection is still valid
                return true;
            }
            catch
            {
                return false;
            }
        }
        catch (DbException)
        {
            return false;
        }
    }

    private void UpdateNodeHealthStatus(bool connectionValid, DateTime statusCheckStartTime, DateTime statusCheckEndTime)
    {
        if (!connectionValid)
        {
            this.failureCount++;

            if (this.invalidNodeStartTime == null)
            {
                this.invalidNodeStartTime = statusCheckStartTime;
            }

            TimeSpan invalidNodeDuration = statusCheckEndTime - statusCheckStartTime;
            int maxInvalidNodeDurationMillis = this.failureDetectionIntervalMillis * Math.Max(0, this.failureDetectionCount - 1);

            if (invalidNodeDuration >= TimeSpan.FromMilliseconds(maxInvalidNodeDurationMillis))
            {
                Logger.LogInformation($"Host is dead: {this.hostSpec.Host}");
                this.nodeUnhealthy = true;
            }
            else
            {
                Logger.LogInformation($"Host is not responding: {this.hostSpec.Host}, failure count: {this.failureCount}");
            }

            return;
        }

        if (this.failureCount > 0)
        {
            Logger.LogInformation($"Host is alive: {this.hostSpec.Host}");
        }

        this.failureCount = 0;
        this.invalidNodeStartTime = null;
        this.nodeUnhealthy = false;
    }

    private void AbortConnection(DbConnection connection)
    {
        try
        {
            connection.Dispose();
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Exception thrown while aborting connection: {ex.Message}");
        }
    }
}
