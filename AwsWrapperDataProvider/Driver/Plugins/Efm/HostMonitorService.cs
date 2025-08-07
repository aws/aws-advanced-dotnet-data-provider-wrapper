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
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitorService : IHostMonitorService
{
    private static readonly ILogger<HostMonitorService> Logger = LoggerUtils.GetLogger<HostMonitorService>();

    protected static readonly int CacheCleanupMs = (int)TimeSpan.FromMinutes(1).TotalMilliseconds;
    protected static readonly MemoryCache Monitors = new(new MemoryCacheOptions());

    private readonly IPluginService pluginService;

    public static readonly int DefaultMonitorDisposalTimeMs = 600000;

    public HostMonitorService(IPluginService pluginService)
    {
        this.pluginService = pluginService;
    }

    public static void CloseAllMonitors()
    {
        foreach (object key in Monitors.Keys)
        {
            if (Monitors.TryGetValue(key, out IHostMonitor? monitor) && monitor != null)
            {
                try
                {
                    monitor.Close();
                }
                catch
                {
                    // ignore
                }
            }

            Monitors.Clear();
        }
    }

    public HostMonitorConnectionContext StartMonitoring(
        DbConnection connectionToAbort,
        HostSpec hostSpec,
        Dictionary<string, string> properties,
        int failureDetectionTimeMs,
        int failureDetectionIntervalMs,
        int failureDetectionCount)
    {
        IHostMonitor monitor = this.GetMonitor(
            hostSpec,
            properties,
            failureDetectionTimeMs,
            failureDetectionIntervalMs,
            failureDetectionCount);

        HostMonitorConnectionContext context = new HostMonitorConnectionContext(connectionToAbort);
        monitor.StartMonitoring(context);

        return context;
    }

    public void StopMonitoring(HostMonitorConnectionContext context, DbConnection connectionToAbort)
    {
        if (context.ShouldAbort())
        {
            context.SetInactive();
            try
            {
                connectionToAbort.Dispose();
            }
            catch
            {
                // ignore
            }
        }
        else
        {
            context.SetInactive();
        }
    }

    public void ReleaseResources()
    {
        // do nothing
    }

    protected IHostMonitor GetMonitor(
        HostSpec hostSpec,
        Dictionary<string, string> properties,
        int failureDetectionTimeMs,
        int failureDetectionIntervalMs,
        int failureDetectionCount)
    {
        string monitorKey = $"{failureDetectionTimeMs}:{failureDetectionIntervalMs}:{failureDetectionCount}:{hostSpec.Host}";
        int cacheExpirationMs = PropertyDefinition.MonitorDisposalTimeMs.GetInt(properties) ?? DefaultMonitorDisposalTimeMs;

        if (!Monitors.TryGetValue(monitorKey, out IHostMonitor? monitor))
        {
            monitor = new HostMonitor(
                this.pluginService,
                hostSpec,
                properties,
                failureDetectionTimeMs,
                failureDetectionIntervalMs,
                failureDetectionCount);

            Monitors.Set(monitorKey, monitor, TimeSpan.FromMilliseconds(cacheExpirationMs));
        }

        return monitor ?? throw new Exception("Could not create or get monitor.");
    }
}
