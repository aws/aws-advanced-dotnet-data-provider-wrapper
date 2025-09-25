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
    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly TimeSpan cacheExpiration;

    protected static readonly int CacheCleanupMs = (int)TimeSpan.FromMinutes(1).TotalMilliseconds;

    public static readonly int DefaultMonitorDisposalTimeMs = 600000;

    internal static readonly MemoryCache Monitors = new(new MemoryCacheOptions { SizeLimit = 100 });

    public static string GetMonitorKey(int failureDetectionTimeMs, int failureDetectionIntervalMs, int failureDetectionCount, string host)
    {
        return $"{failureDetectionTimeMs}:{failureDetectionIntervalMs}:{failureDetectionCount}:{host}";
    }

    public HostMonitorService(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.cacheExpiration = TimeSpan.FromMilliseconds(PropertyDefinition.MonitorDisposalTimeMs.GetInt(this.props) ?? DefaultMonitorDisposalTimeMs);
    }

    public static void CloseAllMonitors()
    {
        Monitors.Clear();
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

        HostMonitorConnectionContext context = new(connectionToAbort);
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
                connectionToAbort.Close();
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
        string monitorKey = GetMonitorKey(failureDetectionTimeMs, failureDetectionIntervalMs, failureDetectionCount, hostSpec.Host);

        if (!Monitors.TryGetValue(monitorKey, out IHostMonitor? monitor))
        {
            monitor = new HostMonitor(
                this.pluginService,
                hostSpec,
                properties,
                failureDetectionTimeMs,
                failureDetectionIntervalMs,
                failureDetectionCount);

            Monitors.Set(monitorKey, monitor, this.CreateCacheEntryOptions());
        }

        return monitor ?? throw new Exception("Could not create or get monitor.");
    }

    private void OnMonitorEvicted(object key, object? value, EvictionReason reason, object? state)
    {
        if (value is IHostMonitor evictedMonitor)
        {
            try
            {
                Logger.LogTrace("Disposing host monitor for monitor key: {key} due to eviction reason: {reason}", key, reason);
                evictedMonitor.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning("Error disposing host monitor: {message} ", ex.Message);
            }
        }
    }

    private MemoryCacheEntryOptions CreateCacheEntryOptions()
    {
        return new MemoryCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = this.cacheExpiration,
            Size = 1,
            PostEvictionCallbacks =
            {
                new PostEvictionCallbackRegistration
                {
                    EvictionCallback = this.OnMonitorEvicted,
                },
            },
        };
    }
}
