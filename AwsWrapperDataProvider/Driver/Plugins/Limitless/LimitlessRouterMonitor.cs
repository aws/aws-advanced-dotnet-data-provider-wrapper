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

using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Limitless;

public class LimitlessRouterMonitor : IDisposable
{
    private const string MonitoringPropertyPrefix = "limitless-router-monitor-";
    private static readonly ILogger<LimitlessRouterMonitor> Logger = LoggerUtils.GetLogger<LimitlessRouterMonitor>();

    private static readonly ILogger<LimitlessRouterMonitor> Logger = LoggerUtils.GetLogger<LimitlessRouterMonitor>();

    private readonly IPluginService _pluginService;
    private readonly HostSpec _hostSpec;
    private readonly MemoryCache _limitlessRouterCache;
    private readonly string _limitlessRouterCacheKey;
    private readonly Dictionary<string, string> _props;
    private readonly int _intervalMs;
    private readonly LimitlessQueryHelper _queryHelper;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task _monitoringTask;
    private DbConnection? _monitoringConn;

    public LimitlessRouterMonitor(
        IPluginService pluginService,
        HostSpec hostSpec,
        MemoryCache limitlessRouterCache,
        string limitlessRouterCacheKey,
        Dictionary<string, string> properties,
        int intervalMs)
    {
        this._pluginService = pluginService;
        this._hostSpec = hostSpec;
        this._limitlessRouterCache = limitlessRouterCache;
        this._limitlessRouterCacheKey = limitlessRouterCacheKey;
        this._intervalMs = intervalMs;
        this._queryHelper = new LimitlessQueryHelper(pluginService);
        this._cancellationTokenSource = new CancellationTokenSource();

        // Copy properties and process monitoring-specific properties
        this._props = new Dictionary<string, string>(properties);
        var keysToRemove = new List<string>();
        var keysToAdd = new Dictionary<string, string>();

        foreach (var kvp in this._props)
        {
            if (kvp.Key.StartsWith(MonitoringPropertyPrefix, StringComparison.Ordinal))
            {
                var newKey = kvp.Key.Substring(MonitoringPropertyPrefix.Length);
                keysToAdd[newKey] = kvp.Value;
                keysToRemove.Add(kvp.Key);
            }
        }

        foreach (var key in keysToRemove)
        {
            this._props.Remove(key);
        }

        foreach (var kvp in keysToAdd)
        {
            this._props[kvp.Key] = kvp.Value;
        }

        this._props[PropertyDefinition.LimitlessWaitForRouterInfo.Name] = "false";

        this._monitoringTask = Task.Run(() => this.Run(this._cancellationTokenSource.Token));
    }

    private async Task Run(CancellationToken cancellationToken)
    {
        Logger.LogTrace(
            "Limitless Router Monitor thread running on node {0}.",
            this._hostSpec.Host);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await this.OpenConnection();

                    if (this._monitoringConn == null || this._monitoringConn.State == ConnectionState.Closed)
                    {
                        continue;
                    }

                    IList<HostSpec> newLimitlessRouters = await this._queryHelper.QueryForLimitlessRouters(
                        this._monitoringConn,
                        this._hostSpec.Port);

                    int monitorDisposalTimeMs = PropertyDefinition.LimitlessMonitorDisposalTimeMs.GetInt(this._props) ?? 600000;
                    MemoryCacheEntryOptions cacheOptions = new()
                    {
                        SlidingExpiration = TimeSpan.FromMilliseconds(monitorDisposalTimeMs),
                    };

                    this._limitlessRouterCache.Set(
                        this._limitlessRouterCacheKey,
                        newLimitlessRouters,
                        cacheOptions);

                    Logger.LogTrace(
                        LoggerUtils.LogTopology(newLimitlessRouters, "[limitlessRouterMonitor] Topology:"));

                    // Sleep between monitoring iterations (do not include this in telemetry)
                    await Task.Delay(this._intervalMs, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogTrace(
                "Limitless Router Monitoring thread for node {0} was interrupted.",
                this._hostSpec.Host);
        }
        catch (Exception ex)
        {
            // This should not be reached; log and exit thread
            Logger.LogTrace(
                ex,
                "Stopping monitoring after unhandled exception was thrown in Limitless Router Monitoring thread for node {0}.",
                this._hostSpec.Host);
        }
        finally
        {
            this.CloseMonitoringConnection();
        }
    }

    private async Task OpenConnection()
    {
        try
        {
            if (this._monitoringConn == null || this._monitoringConn.State == ConnectionState.Closed)
            {
                Logger.LogTrace(
                    "Opening Limitless Router Monitor connection to ''{0}''.",
                    this._hostSpec.GetHostAndPort());

                this._monitoringConn = await this._pluginService.ForceOpenConnection(
                    this._hostSpec,
                    this._props,
                    null,
                    true);

                Logger.LogTrace(
                    "Opened Limitless Router Monitor connection: {0}.",
                    this._monitoringConn);
            }
        }
        catch (DbException)
        {
            this.CloseMonitoringConnection();
            throw;
        }
    }

    private void CloseMonitoringConnection()
    {
        this._cancellationTokenSource.Cancel();

        if (this._monitoringConn != null && this._monitoringConn.State != ConnectionState.Closed)
        {
            try
            {
                this._monitoringConn.Close();
            }
            catch (Exception)
            {
                // Ignore errors during close
            }
        }

        if (this._monitoringConn != null)
        {
            this._monitoringConn.Dispose();
            this._monitoringConn = null;
        }
    }

    public void Dispose()
    {
        try
        {
            if (this._monitoringConn is not null && this._monitoringConn.State != ConnectionState.Closed)
            {
                this.CloseMonitoringConnection();
            }
        }
        catch (DbException e)
        {
            Logger.LogTrace(e, "Error waiting for monitoring task to complete");
        }
        finally
        {
            this._monitoringConn = null;
        }

        this._cancellationTokenSource.Cancel();

        // Waiting for 5s gives a thread enough time to exit monitoring loop and close database connection.
        if (!this._monitoringTask.Wait(TimeSpan.FromSeconds(5)))
        {
            Logger.LogWarning(
                "LimitlessRouterMonitor did not stop within 5 seconds for host {Host}",
                this._hostSpec.Host);
        }

        this._cancellationTokenSource.Dispose();

        Logger.LogTrace(
            "Limitless Router Monitor thread stopped on node {0}.",
            this._hostSpec.Host);
    }
}
