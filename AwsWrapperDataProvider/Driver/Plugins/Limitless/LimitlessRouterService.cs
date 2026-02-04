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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Limitless;

public class LimitlessRouterService : ILimitlessRouterService
{
    private static readonly ILogger<LimitlessRouterService> Logger = LoggerUtils.GetLogger<LimitlessRouterService>();

    private static readonly ConcurrentDictionary<string, object> ForceGetLimitlessRoutersLockMap = new();
    private static readonly MemoryCache LimitlessRouterCache = new(new MemoryCacheOptions());
    private static readonly MemoryCache MonitorCache = new(new MemoryCacheOptions());
    private static readonly TimeSpan CacheExpiration = TimeSpan.FromMinutes(1);

    private readonly IPluginService _pluginService;
    private readonly LimitlessQueryHelper _queryHelper;
    private readonly LimitlessRouterMonitorInitializer _limitlessRouterMonitorInitializer;

    public static void ClearCache()
    {
        LimitlessRouterCache.Clear();
        MonitorCache.Clear();
        ForceGetLimitlessRoutersLockMap.Clear();
    }

    public LimitlessRouterService(IPluginService pluginService)
        : this(pluginService,
            new LimitlessQueryHelper(pluginService),
            (hostSpec,
                routerCache,
                routerCacheKey,
                properties,
                intervalMs) => new LimitlessRouterMonitor(
                pluginService,
                hostSpec,
                routerCache,
                routerCacheKey,
                properties,
                intervalMs))
    {
    }

    public LimitlessRouterService(
        IPluginService pluginService,
        LimitlessQueryHelper queryHelper,
        LimitlessRouterMonitorInitializer limitlessRouterMonitorInitializer)
    {
        this._pluginService = pluginService;
        this._queryHelper = queryHelper;
        this._limitlessRouterMonitorInitializer = limitlessRouterMonitorInitializer;
    }

    public async Task EstablishConnection(LimitlessConnectionContext context)
    {
        context.LimitlessRouters = this.GetLimitlessRouters(this._pluginService.HostListProvider!.GetClusterId());

        if (context.LimitlessRouters == null || !context.LimitlessRouters.Any())
        {
            Logger.LogTrace(Resources.LimitlessRouterService_EstablishConnection_RouterCacheEmpty);
            bool waitForRouterInfo = PropertyDefinition.LimitlessWaitForRouterInfo.GetBoolean(context.Props);
            if (waitForRouterInfo)
            {
                await this.SynchronouslyGetLimitlessRoutersWithRetry(context);
            }
            else
            {
                Logger.LogTrace(Resources.LimitlessRouterService_EstablishConnection_UsingProvidedConnectUrl);
                if (context.Connection == null || context.Connection.State == ConnectionState.Closed)
                {
                    context.SetConnection(await context.ConnectFunc());
                }

                return;
            }
        }

        if (context.LimitlessRouters!.Any(r => r.Host == context.HostSpec.Host && r.Port == context.HostSpec.Port))
        {
            Logger.LogTrace(Resources.LimitlessRouterService_EstablishConnection_ConnectingWithHost, context.HostSpec.Host);
            if (context.Connection == null || context.Connection.State == ConnectionState.Closed)
            {
                try
                {
                    context.SetConnection(await context.ConnectFunc());
                }
                catch (DbException e)
                {
                    if (this.IsLoginException(e))
                    {
                        throw;
                    }

                    await this.RetryConnectWithLeastLoadedRouters(context);
                }
            }

            return;
        }

        WeightedRandomHostSelector.SetHostWeightPairsProperty(context.Props, context.LimitlessRouters!);
        HostSpec? selectedHostSpec;
        try
        {
            selectedHostSpec = this._pluginService.GetHostSpecByStrategy(
                context.LimitlessRouters!,
                HostRole.Writer,
                WeightedRandomHostSelector.StrategyName);
            Logger.LogDebug(Resources.LimitlessRouterService_EstablishConnection_WeightedRandomHostWeightPairs, context.Props[PropertyDefinition.WeightedRandomHostWeightPairs.Name]);
            Logger.LogDebug(Resources.LimitlessRouterService_EstablishConnection_SelectedHost, selectedHostSpec?.Host ?? "null");
        }
        catch (Exception e)
        {
            if (this.IsLoginException(e))
            {
                throw;
            }

            await this.RetryConnectWithLeastLoadedRouters(context);
            return;
        }

        if (selectedHostSpec == null)
        {
            await this.RetryConnectWithLeastLoadedRouters(context);
            return;
        }

        try
        {
            context.SetConnection(await this._pluginService.OpenConnection(selectedHostSpec, context.Props, context.Plugin, false));
        }
        catch (Exception e)
        {
            if (this.IsLoginException(e))
            {
                throw;
            }

            selectedHostSpec.Availability = HostAvailability.Unavailable;
            await this.RetryConnectWithLeastLoadedRouters(context);
        }
    }

    private IList<HostSpec>? GetLimitlessRouters(string clusterId)
    {
        return LimitlessRouterCache.Get<IList<HostSpec>>(clusterId);
    }

    private void SetLimitlessRouters(string clusterId, IList<HostSpec> routers)
    {
        LimitlessRouterCache.Set(clusterId, routers, CacheExpiration);
    }

    private async Task RetryConnectWithLeastLoadedRouters(LimitlessConnectionContext context)
    {
        var retryCount = 0;
        var maxRetries = PropertyDefinition.LimitlessMaxRetries.GetInt(context.Props) ?? 5;

        while (retryCount++ < maxRetries)
        {
            if (context.LimitlessRouters == null || !context.LimitlessRouters.Any(h => h.Availability == HostAvailability.Available))
            {
                await this.SynchronouslyGetLimitlessRoutersWithRetry(context);

                if (context.LimitlessRouters == null || !context.LimitlessRouters.Any(h => h.Availability == HostAvailability.Available))
                {
                    Logger.LogWarning(Resources.LimitlessRouterService_RetryConnect_NoRoutersAvailable);
                    if (context.Connection != null && context.Connection.State != ConnectionState.Closed)
                    {
                        return;
                    }

                    try
                    {
                        context.SetConnection(await context.ConnectFunc());
                        return;
                    }
                    catch (DbException e)
                    {
                        if (this.IsLoginException(e))
                        {
                            throw;
                        }

                        throw new AwsWrapperDbException(string.Format(Resources.LimitlessRouterService_Error_UnableToConnectNoRoutersAvailable, context.HostSpec.Host), e);
                    }
                }
            }

            HostSpec? selectedHostSpec;
            try
            {
                // Select healthiest router for best chance of connection over load-balancing with round-robin
                selectedHostSpec = this._pluginService.GetHostSpecByStrategy(
                    context.LimitlessRouters!,
                    HostRole.Writer,
                    HighestWeightHostSelector.StrategyName);
                Logger.LogDebug(Resources.LimitlessRouterService_RetryConnect_SelectedHostForRetry, selectedHostSpec?.Host ?? "null");
                if (selectedHostSpec == null)
                {
                    continue;
                }
            }
            catch (NotSupportedException)
            {
                Logger.LogError(Resources.LimitlessRouterService_RetryConnect_IncorrectConfiguration);
                throw;
            }
            catch (Exception)
            {
                // error from host selector
                continue;
            }

            try
            {
                context.SetConnection(await this._pluginService.OpenConnection(selectedHostSpec, context.Props, context.Plugin, false));
                if (context.Connection != null)
                {
                    return;
                }
            }
            catch (Exception e)
            {
                if (this.IsLoginException(e))
                {
                    throw;
                }

                selectedHostSpec.Availability = HostAvailability.Unavailable;
                Logger.LogTrace(Resources.LimitlessRouterService_RetryConnect_FailedToConnectToHost, selectedHostSpec.Host);
            }
        }

        throw new InvalidOperationException(Resources.Error_MaxRetriesExceeded);
    }

    private async Task SynchronouslyGetLimitlessRoutersWithRetry(LimitlessConnectionContext context)
    {
        var retryCount = -1;
        var maxRetries = PropertyDefinition.LimitlessGetRouterMaxRetries.GetInt(context.Props) ?? 5;
        var retryIntervalMs = PropertyDefinition.LimitlessGetRouterRetryIntervalMs.GetInt(context.Props) ?? 300;

        do
        {
            try
            {
                this.SynchronouslyGetLimitlessRouters(context);
                if (context.LimitlessRouters != null && context.LimitlessRouters.Any())
                {
                    return;
                }

                await Task.Delay(retryIntervalMs);
            }
            catch (DbException e)
            {
                if (this.IsLoginException(e))
                {
                    throw;
                }

                Logger.LogDebug(Resources.LimitlessRouterService_SynchronouslyGetLimitlessRouters_ExceptionGettingRouters, e);
            }
            finally
            {
                retryCount++;
            }
        }
        while (retryCount < maxRetries);

        throw new AwsWrapperDbException(Resources.LimitlessRouterService_Error_NoRoutersAvailable);
    }

    private void SynchronouslyGetLimitlessRouters(LimitlessConnectionContext context)
    {
        string lockKey = this._pluginService.HostListProvider!.GetClusterId();
        object lockObj = ForceGetLimitlessRoutersLockMap.GetOrAdd(lockKey, _ => new object());

        lock (lockObj)
        {
            IList<HostSpec>? limitlessRouters = this.GetLimitlessRouters(lockKey);
            if (limitlessRouters != null && limitlessRouters.Any())
            {
                context.LimitlessRouters = limitlessRouters;
                return;
            }

            if (context.Connection == null || context.Connection.State == ConnectionState.Closed)
            {
                context.SetConnection(context.ConnectFunc().GetAwaiter().GetResult());
            }

            var newRouterList = this._queryHelper.QueryForLimitlessRouters(context.Connection!, context.HostSpec.Port).GetAwaiter().GetResult();

            if (newRouterList.Any())
            {
                context.LimitlessRouters = newRouterList;
                this.SetLimitlessRouters(
                    this._pluginService.HostListProvider!.GetClusterId(),
                    newRouterList);
            }
            else
            {
                throw new AwsWrapperDbException(Resources.LimitlessRouterService_Error_FetchedEmptyRouterList);
            }
        }
    }

    private bool IsLoginException(Exception exception)
    {
        return this._pluginService.IsLoginException(exception);
    }

    public void StartMonitoring(HostSpec hostSpec, Dictionary<string, string> props, int intervalMs)
    {
        var monitorKey = this._pluginService.HostListProvider!.GetClusterId();

        try
        {
            MonitorCache.GetOrCreate(monitorKey, entry =>
            {
                entry.SlidingExpiration =
                    TimeSpan.FromMilliseconds(PropertyDefinition.LimitlessMonitorDisposalTimeMs.GetInt(props) ?? 600000);
                return this._limitlessRouterMonitorInitializer(
                    hostSpec,
                    LimitlessRouterCache,
                    monitorKey,
                    props,
                    intervalMs);
            });
        }
        catch (DbException e)
        {
            Logger.LogWarning(Resources.LimitlessRouterService_StartMonitoring_ErrorGettingRouters, e);
            throw;
        }
    }
}

public delegate LimitlessRouterMonitor LimitlessRouterMonitorInitializer(
    HostSpec hostSpec,
    MemoryCache limitlessRouterCache,
    string limitlessRouterCacheKey,
    Dictionary<string, string> props,
    int intervalMs);
