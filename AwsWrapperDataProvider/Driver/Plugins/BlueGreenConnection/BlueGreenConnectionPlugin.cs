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
using System.Diagnostics;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;

public class BlueGreenConnectionPlugin : AbstractConnectionPlugin
{
    public static readonly AwsWrapperProperty BgSkipRoutingInForceConnect = new(
        "3a864d24-568f-4b55-a227-6f649ae3021a",
        "true",
        "Flag to skip Bg when force connecting");

    private static readonly ILogger<BlueGreenConnectionPlugin> Logger = LoggerUtils.GetLogger<BlueGreenConnectionPlugin>();

    private static readonly ConcurrentDictionary<string, BlueGreenStatusProvider> Provider = new();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly BlueGreenProviderSupplier providerSupplier;
    private readonly string bgdId;
    private readonly AtomicLong endTime;

    private Stopwatch? stopwatch;
    private BlueGreenStatus? bgStatus;
    private string? clusterId;

    public override IReadOnlySet<string> SubscribedMethods => PluginMethods.NetworkBoundMethods;

    public BlueGreenConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props)
        : this(pluginService, props, (container, p, bgdIdParam, clusterIdParam) => new BlueGreenStatusProvider(container, p, bgdIdParam, clusterIdParam))
    {
    }

    private BlueGreenConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props,
        BlueGreenProviderSupplier providerSupplier)
    {
        this.endTime = new AtomicLong(-1);
        this.pluginService = pluginService;
        this.props = props;
        this.providerSupplier = providerSupplier;
        this.bgdId = PropertyDefinition.BgdId.GetString(this.props) ?? PropertyDefinition.BgdId.DefaultValue!;
    }

    public override async Task<DbConnection> ForceOpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> connectFunc,
        bool async)
    {
        if (!props.ContainsKey(BgSkipRoutingInForceConnect.Name))
        {
            return await this.ConnectInternal(hostSpec, props, isInitialConnection, true, connectFunc);
        }

        props.Remove(BgSkipRoutingInForceConnect.Name);
        return await connectFunc();
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> connectFunc,
        bool async)
    {
        return await this.ConnectInternal(hostSpec, props, isInitialConnection, false, connectFunc);
    }

    private async Task<DbConnection> ConnectInternal(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool useForceConnect,
        ADONetDelegate<DbConnection> connectFunc)
    {
        this.ResetRoutingTimeNano();

        try
        {
            this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);

            if (this.bgStatus == null)
            {
                return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
            }

            if (hostSpec == null)
            {
                return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
            }

            BlueGreenRoleType? hostRole = this.bgStatus.GetRole(hostSpec);

            if (hostRole == null)
            {
                return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
            }

            var routing = this.bgStatus.ConnectRouting
                .FirstOrDefault(r => r.IsMatch(hostSpec, hostRole.Value));

            if (routing == null)
            {
                return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
            }

            this.stopwatch = Stopwatch.StartNew();

            DbConnection? conn = null;
            while (routing != null && conn == null)
            {
                conn = await routing.Apply(
                    this,
                    hostSpec,
                    props,
                    isInitialConnection,
                    useForceConnect,
                    connectFunc,
                    this.pluginService);

                if (conn != null)
                {
                    continue;
                }

                this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
                if (this.bgStatus == null)
                {
                    this.endTime.Set(this.stopwatch?.ElapsedMilliseconds ?? 0);
                    return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
                }

                routing = this.bgStatus.ConnectRouting
                    .FirstOrDefault(r => r.IsMatch(hostSpec, hostRole.Value));
            }

            this.endTime.Set(this.stopwatch?.ElapsedMilliseconds ?? 0);

            if (conn == null)
            {
                conn = await connectFunc();
            }

            if (isInitialConnection && !useForceConnect)
            {
                this.InitProvider();
            }

            return conn;
        }
        finally
        {
            if (this.stopwatch != null)
            {
                this.endTime.CompareAndSet(0, this.stopwatch.ElapsedMilliseconds);
            }
        }
    }

    private async Task<DbConnection> RegularOpenConnection(
        ADONetDelegate<DbConnection> connectFunc,
        bool isInitialConnection,
        bool useForceConnect)
    {
        var conn = await connectFunc();

        if (isInitialConnection && !useForceConnect)
        {
            this.InitProvider();
        }

        return conn;
    }

    public override async Task<T> Execute<T>(
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        this.ResetRoutingTimeNano();
        try
        {
            this.InitProvider();

            this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
            Logger.LogTrace($"[BlueGreenConnectionPlugin] bgStatus: {this.bgStatus}");

            if (this.bgStatus == null)
            {
                return await methodFunc();
            }

            HostSpec? currentHostSpec = this.pluginService.CurrentHostSpec;
            BlueGreenRoleType? hostRole = this.bgStatus.GetRole(currentHostSpec);

            if (hostRole == null || currentHostSpec == null)
            {
                return await methodFunc();
            }

            Logger.LogTrace($"[BlueGreenConnectionPlugin] ExecutingRoutingCount: {this.bgStatus.ExecuteRouting.Count}");
            T? result = default;
            var routing = this.bgStatus.ExecuteRouting
                .FirstOrDefault(r => r.IsMatch(currentHostSpec, hostRole.Value));

            if (routing == null)
            {
                return await methodFunc();
            }

            Logger.LogTrace($"[BlueGreenConnectionPlugin] Routing: {routing}");
            this.stopwatch = Stopwatch.StartNew();

            while (routing != null && result == null)
            {
                result = await routing.Apply(
                    this,
                    methodInvokeOn,
                    methodName,
                    methodFunc,
                    methodArgs,
                    this.pluginService,
                    this.props);

                if (result != null)
                {
                    continue;
                }

                this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
                if (this.bgStatus == null)
                {
                    this.endTime.Set(this.stopwatch.ElapsedMilliseconds);
                    return await methodFunc();
                }

                routing = this.bgStatus.ExecuteRouting
                    .FirstOrDefault(r => r.IsMatch(currentHostSpec, hostRole.Value));
            }

            this.endTime.Set(this.stopwatch.ElapsedMilliseconds);

            if (result != null)
            {
                return result;
            }

            return await methodFunc();
        }
        finally
        {
            if (this.stopwatch != null)
            {
                this.endTime.CompareAndSet(0, this.stopwatch.ElapsedMilliseconds);
            }
        }
    }

    protected void InitProvider()
    {
        try
        {
            this.clusterId = this.pluginService.HostListProvider?.GetClusterId();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to get cluster ID");
            throw new InvalidOperationException("Failed to get cluster ID", ex);
        }

        // Plugins are constructed with only an IPluginService (IConnectionPluginFactory.GetInstance),
        // so the container is derived here once and passed down the provider/monitor chain.
        FullServicesContainer servicesContainer = ServiceUtility.FromPluginService(this.pluginService)
            ?? throw new InvalidOperationException(
                string.Format(Resources.Error_FullServicesContainerSlotNotInitialized, nameof(FullServicesContainer)));
        Provider.GetOrAdd(this.bgdId,
            key => this.providerSupplier(servicesContainer, this.props, this.bgdId, this.clusterId));
    }

    public long GetHoldTimeMs()
    {
        if (this.stopwatch == null)
        {
            return 0;
        }

        return this.endTime.Get() <= 0 ? this.stopwatch.ElapsedMilliseconds : this.endTime.Get();
    }

    public void ResetRoutingTimeNano()
    {
        this.stopwatch = null;
        this.endTime.Set(-1);
    }
}
