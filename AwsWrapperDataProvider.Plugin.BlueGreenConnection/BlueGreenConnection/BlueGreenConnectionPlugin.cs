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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection;

public class BlueGreenConnectionPlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<BlueGreenConnectionPlugin> Logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<BlueGreenConnectionPlugin>();

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>()
    {
        // Network-bound methods that might fail and trigger failover
        "DbConnection.Open",
        "DbConnection.OpenAsync",
        "DbConnection.BeginDbTransaction",
        "DbConnection.BeginDbTransactionAsync",

        "DbCommand.ExecuteNonQuery",
        "DbCommand.ExecuteNonQueryAsync",
        "DbCommand.ExecuteReader",
        "DbCommand.ExecuteReaderAsync",
        "DbCommand.ExecuteScalar",
        "DbCommand.ExecuteScalarAsync",

        "DbBatch.ExecuteNonQuery",
        "DbBatch.ExecuteNonQueryAsync",
        "DbBatch.ExecuteReaderA",
        "DbBatch.ExecuteReaderAsync",
        "DbBatch.ExecuteScalar",
        "DbBatch.ExecuteScalarAsync",

        "DbDataReader.Read",
        "DbDataReader.ReadAsync",
        "DbDataReader.NextResult",
        "DbDataReader.NextResultAsync",

        "DbTransaction.Commit",
        "DbTransaction.CommitAsync",
        "DbTransaction.Rollback",
        "DbTransaction.RollbackAsync",
    };

    private static readonly HashSet<string> ClosingMethodNames = new()
    {
        "DbConnection.Close",
        "DbConnection.CloseAsync",
        "DbConnection.Dispose",
        "DbConnection.DisposeAsync",
    };


    public static readonly AwsWrapperProperty BgConnectTimeout = new(
        "bgConnectTimeoutMs",
        "30000",
        "Connect timeout (in msec) during Blue/Green Deployment switchover.");

    protected static readonly ConcurrentDictionary<string, BlueGreenStatusProvider> Provider = new();

    public const string BgSkipRoutingInForceConnect = "3a864d24-568f-4b55-a227-6f649ae3021a";

    protected readonly IPluginService pluginService;
    protected readonly Dictionary<string, string> props;
    protected BlueGreenProviderSupplier providerSupplier;

    protected BlueGreenStatus? bgStatus = null;
    protected string? bgdId;
    protected string? clusterId;

    protected long startTimeNano = 0;
    protected long endTimeNano = 0;

    public BlueGreenConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props)
        : this(pluginService, props, (svc, p, bgdIdParam, clusterIdParam) => new BlueGreenStatusProvider(svc, p, bgdIdParam, clusterIdParam))
    {
    }

    public BlueGreenConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props,
        BlueGreenProviderSupplier providerSupplier)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.providerSupplier = providerSupplier;
        this.bgdId = PropertyDefinition.BgdId.GetString(this.props);
    }

    public override async Task<DbConnection> ForceOpenConnection(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> connectFunc,
        bool async)
    {
        if (props.ContainsKey(BgSkipRoutingInForceConnect))
        {
            return await connectFunc();
        }

        return await this.ConnectInternal(hostSpec, props, isInitialConnection, true, connectFunc);
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> connectFunc,
        bool async)
    {
        return await this.ConnectInternal(hostSpec, props, isInitialConnection, false, connectFunc);
    }

    protected async Task<DbConnection> ConnectInternal(
        HostSpec hostSpec,
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

            var hostRole = this.bgStatus.GetRole(hostSpec);

            if (hostRole == null)
            {
                return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
            }

            DbConnection? conn = null;
            var routing = this.bgStatus.ConnectRouting
                .FirstOrDefault(r => r.IsMatch(hostSpec, hostRole.Value));

            if (routing == null)
            {
                return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
            }

            Interlocked.Exchange(ref this.startTimeNano, this.GetNanoTime());

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

                if (conn == null)
                {
                    this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
                    if (this.bgStatus == null)
                    {
                        Interlocked.Exchange(ref this.endTimeNano, this.GetNanoTime());
                        return await this.RegularOpenConnection(connectFunc, isInitialConnection, useForceConnect);
                    }

                    routing = this.bgStatus.ConnectRouting
                        .FirstOrDefault(r => r.IsMatch(hostSpec, hostRole.Value));
                }
            }

            Interlocked.Exchange(ref this.endTimeNano, this.GetNanoTime());

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
            if (Interlocked.Read(ref this.startTimeNano) > 0)
            {
                Interlocked.CompareExchange(ref this.endTimeNano, this.GetNanoTime(), 0);
            }
        }
    }

    protected async Task<DbConnection> RegularOpenConnection(
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

            if (ClosingMethodNames.Contains(methodName))
            {
                return await methodFunc();
            }

            this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);

            if (this.bgStatus == null)
            {
                return await methodFunc();
            }

            var currentHostSpec = this.pluginService.CurrentHostSpec;
            var hostRole = this.bgStatus.GetRole(currentHostSpec);

            if (hostRole == null)
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

            Interlocked.Exchange(ref this.startTimeNano, this.GetNanoTime());

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

                if (result == null)
                {
                    this.bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
                    if (this.bgStatus == null)
                    {
                        Interlocked.Exchange(ref this.endTimeNano, this.GetNanoTime());
                        return await methodFunc();
                    }

                    routing = this.bgStatus.ExecuteRouting
                        .FirstOrDefault(r => r.IsMatch(currentHostSpec, hostRole.Value));
                }
            }

            Interlocked.Exchange(ref this.endTimeNano, this.GetNanoTime());

            if (result != null)
            {
                return result;
            }

            return await methodFunc();
        }
        finally
        {
            if (Interlocked.Read(ref this.startTimeNano) > 0)
            {
                Interlocked.CompareExchange(ref this.endTimeNano, this.GetNanoTime(), 0);
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

        var provider = Provider.GetOrAdd(this.bgdId,
            key => this.providerSupplier(this.pluginService, this.props, this.bgdId, this.clusterId));
        
        Logger.LogTrace($"[bgdId: {this.bgdId}] Provider initialized. Cluster ID: {this.clusterId}");
    }

    protected virtual long GetNanoTime()
    {
        return DateTime.UtcNow.Ticks * 100;
    }

    public long GetHoldTimeNano()
    {
        long start = Interlocked.Read(ref this.startTimeNano);
        long end = Interlocked.Read(ref this.endTimeNano);
        return start == 0 ? 0 : (end == 0 ? (this.GetNanoTime() - start) : (end - start));
    }

    public void ResetRoutingTimeNano()
    {
        Interlocked.Exchange(ref this.startTimeNano, 0);
        Interlocked.Exchange(ref this.endTimeNano, 0);
    }
}
