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
    private readonly AtomicLong startTimeNano = new();
    private readonly AtomicLong endTimeNano = new();

    private BlueGreenStatus? bgStatus;
    private string? clusterId;

    public override IReadOnlySet<string> SubscribedMethods => PluginMethods.NetworkBoundMethods;

    public BlueGreenConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props)
        : this(pluginService, props, (svc, p, bgdIdParam, clusterIdParam) => new BlueGreenStatusProvider(svc, p, bgdIdParam, clusterIdParam))
    {
    }

    private BlueGreenConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props,
        BlueGreenProviderSupplier providerSupplier)
    {
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

            this.startTimeNano.Set(this.GetNanoTime());

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
                    this.endTimeNano.Set(this.GetNanoTime());
                    return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
                }

                routing = this.bgStatus.ConnectRouting
                    .FirstOrDefault(r => r.IsMatch(hostSpec, hostRole.Value));
            }

            this.endTimeNano.Set(this.GetNanoTime());

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
            if (this.startTimeNano.Get() > 0)
            {
                this.endTimeNano.CompareAndSet(0, this.GetNanoTime());
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

            T? result = default;
            var routing = this.bgStatus.ExecuteRouting
                .FirstOrDefault(r => r.IsMatch(currentHostSpec, hostRole.Value));

            if (routing == null)
            {
                return await methodFunc();
            }

            this.startTimeNano.Set(this.GetNanoTime());

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
                    this.endTimeNano.Set(this.GetNanoTime());
                    return await methodFunc();
                }

                routing = this.bgStatus.ExecuteRouting
                    .FirstOrDefault(r => r.IsMatch(currentHostSpec, hostRole.Value));
            }

            this.endTimeNano.Set(this.GetNanoTime());

            if (result != null)
            {
                return result;
            }

            return await methodFunc();
        }
        finally
        {
            if (this.startTimeNano.Get() > 0)
            {
                this.endTimeNano.CompareAndSet(0, this.GetNanoTime());
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
            throw new InvalidOperationException("Failed to get cluster ID", ex);
        }

        Provider.GetOrAdd(this.bgdId,
            key => this.providerSupplier(this.pluginService, this.props, this.bgdId, this.clusterId));
    }

    protected virtual long GetNanoTime()
    {
        return Stopwatch.GetTimestamp();
    }

    public void ResetRoutingTimeNano()
    {
        this.startTimeNano.Set(0);
        this.endTimeNano.Set(0);
    }
}
