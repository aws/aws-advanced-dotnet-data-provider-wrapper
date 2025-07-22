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

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitor : IHostMonitor
{
    public struct ConnectionStatus
    {
        public bool IsValid { get; set; }
        public TimeSpan ElapsedTime { get; set; }

        public ConnectionStatus(bool isValid, TimeSpan elapsedTime)
        {
            this.IsValid = isValid;
            this.ElapsedTime = elapsedTime;
        }
    }

    private static readonly long ThreadSleepWhenInactiveMillis = 100;
    private static readonly long MinConnectionCheckTimeoutMillis = 100;
    private static readonly string MonitoringPropertyPrefix = "monitoring-";

    public ConcurrentQueue<HostMonitorConnectionContext> ActiveContexts = new();

    private readonly ConcurrentQueue<HostMonitorConnectionContext> newContexts = new();
    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> properties;
    private readonly HostSpec hostSpec;
    private readonly HostMonitorThreadContainer threadContainer;
    private readonly long monitorDisposalTimeMillis;
    private DbConnection? monitoringConn;
    private DateTime contextLastUsed;
    private volatile bool stopped;

    /// <summary>
    /// Initializes a new instance of the <see cref="HostMonitor"/> class.
    /// </summary>
    /// <param name="pluginService">A service for creating new connections.</param>
    /// <param name="hostSpec">The HostSpec of the server this HostMonitorImpl instance is monitoring.</param>
    /// <param name="properties">The Properties containing additional monitoring configuration.</param>
    /// <param name="monitorDisposalTimeMillis">Time in milliseconds before stopping the monitoring thread where there are no active connection to the server this HostMonitor instance is monitoring.</param>
    /// <param name="threadContainer">A reference to the HostMonitorThreadContainer implementation that initialized this class.</param>
    public HostMonitor(
        IPluginService pluginService,
        HostSpec hostSpec,
        Dictionary<string, string> properties,
        long monitorDisposalTimeMillis,
        HostMonitorThreadContainer threadContainer)
    {
        this.pluginService = pluginService;
        this.hostSpec = hostSpec;
        this.properties = properties;
        this.monitorDisposalTimeMillis = monitorDisposalTimeMillis;
        this.threadContainer = threadContainer;

        this.monitoringConn = null;
        this.contextLastUsed = DateTime.Now;
    }

    public void StartMonitoring(HostMonitorConnectionContext context)
    {
        if (this.stopped)
        {
            // warning: monitor is stopped
        }

        DateTime currentTime = DateTime.Now;
        context.SetStartMonitorTime(currentTime);
        this.contextLastUsed = currentTime;
        this.newContexts.Enqueue(context);
    }

    public void StopMonitoring(HostMonitorConnectionContext context)
    {
        context.SetInactive();
        this.contextLastUsed = DateTime.Now;
    }

    public void ClearContexts()
    {
        this.newContexts.Clear();
        this.ActiveContexts.Clear();
    }

    public bool IsStopped()
    {
        return this.stopped;
    }

    public async void Run(CancellationToken token)
    {
        try
        {
            this.stopped = false;
            while (true)
            {
                token.ThrowIfCancellationRequested();
                try
                {
                    HostMonitorConnectionContext? firstAddedNewMonitorContext = null;
                    DateTime currentTime = DateTime.Now;

                    // process new contexts and add them to the active contexts when it is past their monitoring start time
                    while (this.newContexts.TryDequeue(out HostMonitorConnectionContext? newMonitorContext))
                    {
                        if (firstAddedNewMonitorContext == newMonitorContext)
                        {
                            // this context has already been processed; add it back and break the loop
                            this.newContexts.Enqueue(newMonitorContext);
                            break;
                        }

                        if (newMonitorContext.IsActiveContext())
                        {
                            if (newMonitorContext.ExpectedActiveMonitoringStartTime > currentTime)
                            {
                                this.newContexts.Enqueue(newMonitorContext);
                                firstAddedNewMonitorContext ??= newMonitorContext;
                            }
                            else
                            {
                                this.ActiveContexts.Enqueue(newMonitorContext);
                            }
                        }
                    }

                    if (!this.ActiveContexts.IsEmpty
                        || this.monitoringConn == null
                        || this.monitoringConn.State == System.Data.ConnectionState.Closed)
                    {
                        DateTime statusCheckStart = DateTime.Now;
                        this.contextLastUsed = statusCheckStart;

                        ConnectionStatus status = this.CheckConnectionStatus();

                        long delayMillis = -1;
                        HostMonitorConnectionContext? firstAddedMonitorContext = null;

                        while (this.ActiveContexts.TryDequeue(out HostMonitorConnectionContext? monitorContext))
                        {
                            monitorContext.Lock.WaitOne();
                            try
                            {
                                if (!monitorContext.IsActiveContext())
                                {
                                    continue;
                                }

                                if (firstAddedMonitorContext == monitorContext)
                                {
                                    this.ActiveContexts.Enqueue(monitorContext);
                                    break;
                                }

                                monitorContext.UpdateConnectionStatus(
                                    this.hostSpec.Host,
                                    statusCheckStart,
                                    statusCheckStart.Add(status.ElapsedTime),
                                    status.IsValid);

                                if (monitorContext.IsActiveContext() && !monitorContext.IsNodeUnhealthy())
                                {
                                    this.ActiveContexts.Enqueue(monitorContext);
                                    firstAddedMonitorContext ??= monitorContext;

                                    if (delayMillis == -1 || delayMillis > monitorContext.FailureDetectionIntervalMillis)
                                    {
                                        delayMillis = monitorContext.FailureDetectionIntervalMillis;
                                    }
                                }
                            }
                            finally
                            {
                                monitorContext.Lock.ReleaseMutex();
                            }
                        }

                        if (delayMillis == -1)
                        {
                            delayMillis = ThreadSleepWhenInactiveMillis;
                        }
                        else
                        {
                            delayMillis -= (long)status.ElapsedTime.TotalMilliseconds;

                            if (delayMillis <= MinConnectionCheckTimeoutMillis)
                            {
                                delayMillis = MinConnectionCheckTimeoutMillis;
                            }
                        }

                        await Task.Delay((int)delayMillis, token);
                    }
                    else
                    {
                        if (DateTime.Now - this.contextLastUsed >= TimeSpan.FromMilliseconds(this.monitorDisposalTimeMillis))
                        {
                            this.threadContainer.ReleaseResource(this);
                            break;
                        }

                        await Task.Delay((int)ThreadSleepWhenInactiveMillis, token);
                    }
                }
                catch
                {
                    // ignore
                }
            }
        }
        finally
        {
            this.threadContainer.ReleaseResource(this);
            this.stopped = true;
            if (this.monitoringConn != null)
            {
                try
                {
                    this.monitoringConn.Close();
                    this.monitoringConn.Dispose();
                }
                catch
                {
                    // ignore
                }
            }
        }

        // stopped monitoring thread
    }

    /// <summary>
    /// Check the status of the monitored server by sending a ping.
    /// </summary>
    /// <returns>
    /// whether the server is still alive and the elapsed time spent checking.
    /// </returns>
    private ConnectionStatus CheckConnectionStatus()
    {
        DateTime startTime = DateTime.Now;
        try
        {
            if (this.monitoringConn == null || this.monitoringConn.State == System.Data.ConnectionState.Closed)
            {
                Dictionary<string, string> monitoringConnProperties = new(this.properties);

                foreach (string propertyName in this.properties.Keys)
                {
                    if (propertyName.StartsWith(MonitoringPropertyPrefix))
                    {
                        string monitoringPropertyName = propertyName[MonitoringPropertyPrefix.Length..];
                        monitoringConnProperties[monitoringPropertyName] = this.properties[propertyName];
                    }
                }

                this.monitoringConn = this.pluginService.OpenConnection(this.hostSpec, monitoringConnProperties, null);
                return new ConnectionStatus(true, DateTime.Now - startTime);
            }

            bool isValid = this.monitoringConn.State != System.Data.ConnectionState.Open;
            return new ConnectionStatus(isValid, DateTime.Now - startTime);
        }
        catch
        {
            return new ConnectionStatus(false, DateTime.Now - startTime);
        }
    }
}
