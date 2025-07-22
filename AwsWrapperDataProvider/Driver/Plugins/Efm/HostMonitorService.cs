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

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitorService : IHostMonitorService
{
    public static readonly long DefaultMonitorDisposalTimeMs = 600000;

    public HostMonitorThreadContainer? ThreadContainer { get; private set; }

    private readonly IPluginService pluginService;
    private readonly WeakReference<IHostMonitor?> cachedMonitor = new(null);
    private HashSet<string> cachedMonitorNodeKeys = new();

    public HostMonitorService(IPluginService pluginService)
    {
        this.pluginService = pluginService;
        this.ThreadContainer = HostMonitorThreadContainer.GetInstance();
    }

    public HostMonitorConnectionContext StartMonitoring(
        DbConnection connectionToAbort,
        HashSet<string> nodeKeys,
        HostSpec hostSpec,
        Dictionary<string, string> properties,
        int failureDetectionTimeMillis,
        int failureDetectionIntervalMillis,
        int failureDetectionCount)
    {
        if (nodeKeys.Count == 0)
        {
            throw new Exception("Cannot start monitoring without alias set.");
        }

        IHostMonitor? monitor;
        if (!this.cachedMonitor.TryGetTarget(out monitor))
        {
            monitor = null;
        }

        if (monitor == null
            || monitor.IsStopped()
            || this.cachedMonitorNodeKeys == null
            || !this.cachedMonitorNodeKeys.SetEquals(nodeKeys))
        {
            monitor = this.GetMonitor(nodeKeys, hostSpec, properties);
            this.cachedMonitor.SetTarget(monitor);
            this.cachedMonitorNodeKeys = nodeKeys;
        }

        HostMonitorConnectionContext context = new(monitor, connectionToAbort, failureDetectionTimeMillis, failureDetectionIntervalMillis, failureDetectionCount);
        monitor.StartMonitoring(context);

        return context;
    }

    public void StopMonitoring(HostMonitorConnectionContext context)
    {
        IHostMonitor monitor = context.Monitor;
        monitor.StopMonitoring(context);
    }

    public void StopMonitoringForAllConnections(HashSet<string> nodeKeys)
    {
        foreach (string nodeKey in nodeKeys.ToArray())
        {
            IHostMonitor? monitor = this.ThreadContainer!.GetMonitor(nodeKey);
            if (monitor != null)
            {
                monitor.ClearContexts();
                return;
            }
        }
    }

    public void ReleaseResources()
    {
        this.ThreadContainer = null;
    }

    protected IHostMonitor GetMonitor(
        HashSet<string> nodeKeys,
        HostSpec hostSpec,
        Dictionary<string, string> properties)
    {
        long monitorDisposalTimeMillis = Utils.PropertyDefinition.MonitorDisposalTimeMs.GetLong(properties) ?? DefaultMonitorDisposalTimeMs;
        return this.ThreadContainer!.GetOrCreateMonitor(
            nodeKeys.ToArray(),
            () => new HostMonitor(this.pluginService, hostSpec, properties, monitorDisposalTimeMillis, this.ThreadContainer!));
    }
}
