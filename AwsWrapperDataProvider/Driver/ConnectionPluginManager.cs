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
using System.Diagnostics;
using System.Reflection;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver;

public class ConnectionPluginManager
{
    private readonly Dictionary<string, Delegate> pluginChainDelegates = [];
    protected IList<IConnectionPlugin> plugins = [];
    protected string[] activePluginCodes = [];
    protected IConnectionProvider defaultConnProvider;
    protected IConnectionProvider? effectiveConnProvider;
    protected ConfigurationProfile? configurationProfile;

    protected AwsWrapperConnection ConnectionWrapper { get; }

    protected IPluginService? pluginService;

    /// <summary>
    /// Gets the telemetry factory associated with this plugin manager.
    /// </summary>
    internal ITelemetryFactory TelemetryFactory
        => this.pluginService?.TelemetryFactory ?? NullTelemetryFactory.Instance;

    private const string AllMethods = "*";
    private const string GetHostSpecByStrategyMethod = "GetHostSpecByStrategy";
    private const string ConnectMethod = "DbConnection.Open";
    private const string ForceConnectMethod = "DbConnection.ForceOpen";
    private const string InitHostMethod = "initHostProvider";

    /// <summary>
    /// Pretty-name map used to produce human-readable nested span names for the
    /// per-plugin telemetry wrap in <see cref="InvokePluginWithNestedTrace{T}"/>.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> PluginNameByType =
        new Dictionary<string, string>
        {
            // Core plugins in AwsWrapperDataProvider
            ["FailoverPlugin"] = "plugin:failover",
            ["HostMonitoringPlugin"] = "plugin:efm",
            ["AuroraConnectionTrackerPlugin"] = "plugin:auroraConnectionTracker",
            ["AuroraInitialConnectionStrategyPlugin"] = "plugin:auroraInitialConnectionStrategy",
            ["BlueGreenConnectionPlugin"] = "plugin:blueGreen",
            ["ConnectTimePlugin"] = "plugin:connectTime",
            ["DefaultConnectionPlugin"] = "plugin:targetDriver",
            ["ExecutionTimePlugin"] = "plugin:executionTime",
            ["LimitlessConnectionPlugin"] = "plugin:limitless",
            ["ReadWriteSplittingPlugin"] = "plugin:readWriteSplitting",

            // Plugins in separate projects — referenced by simple class name
            ["IamAuthPlugin"] = "plugin:iam",
            ["FederatedAuthPlugin"] = "plugin:federatedAuth",
            ["OktaAuthPlugin"] = "plugin:okta",
            ["SecretsManagerAuthPlugin"] = "plugin:awsSecretsManager",
            ["CustomEndpointPlugin"] = "plugin:customEndpoint",
        };

    private delegate Task<T> PluginPipelineDelegate<T>(IConnectionPlugin plugin, ADONetDelegate<T> methodFunc);

    private delegate Task<T> PluginChainADONetDelegate<T>(PluginPipelineDelegate<T> pipelineDelegate, ADONetDelegate<T> methodFunc, IConnectionPlugin? pluginToSkip);

    public ConnectionPluginManager(
        IConnectionProvider defaultConnProvider,
        IConnectionProvider? effectiveConnProvider,
        AwsWrapperConnection connection) : this(
        defaultConnProvider,
        effectiveConnProvider,
        connection,
        null)
    { }

    public ConnectionPluginManager(
        IConnectionProvider defaultConnectionProvider,
        IConnectionProvider? effectiveConnectionProvider,
        AwsWrapperConnection connection,
        ConfigurationProfile? configurationProfile)
    {
        this.defaultConnProvider = defaultConnectionProvider;
        this.effectiveConnProvider = effectiveConnectionProvider;
        this.configurationProfile = configurationProfile;
        this.ConnectionWrapper = connection;
    }

    // for testing purpose only
    public ConnectionPluginManager(
        IConnectionProvider defaultConnectionProvider,
        IConnectionProvider? effectiveConnectionProvider,
        IList<IConnectionPlugin> plugins,
        AwsWrapperConnection connection)
    {
        this.defaultConnProvider = defaultConnectionProvider;
        this.effectiveConnProvider = effectiveConnectionProvider;
        this.plugins = plugins;
        this.ConnectionWrapper = connection;
    }

    public void InitConnectionPluginChain(
        IPluginService pluginService,
        Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        ConnectionPluginChainBuilder pluginChainBuilder = new();
        this.plugins = pluginChainBuilder.GetPlugins(
            this.pluginService,
            this.defaultConnProvider,
            this.effectiveConnProvider,
            props,
            this.configurationProfile);
        this.activePluginCodes = pluginChainBuilder.GetPluginCodes(this.pluginService, props);
    }

    private async Task<T> ExecuteWithSubscribedPlugins<T>(
        string methodName,
        PluginPipelineDelegate<T> pluginPipelineDelegate,
        ADONetDelegate<T> methodFunc,
        IConnectionPlugin? pluginToSkip)
    {
        ArgumentNullException.ThrowIfNull(pluginPipelineDelegate);
        ArgumentNullException.ThrowIfNull(methodFunc);

        if (!this.pluginChainDelegates.TryGetValue(methodName, out Delegate? del))
        {
            del = this.MakePluginChainDelegate<T>(methodName);
            this.pluginChainDelegates[methodName] = del;
        }

        if (del is not PluginChainADONetDelegate<T> pluginChainDelegate)
        {
            throw new Exception(Properties.Resources.Error_ProcessingAdoNetCall);
        }

        try
        {
            return await pluginChainDelegate(pluginPipelineDelegate, methodFunc, pluginToSkip);
        }
        catch (TargetInvocationException exception)
        {
            if (exception.InnerException != null)
            {
                throw exception.InnerException;
            }
        }

        throw new UnreachableException(Resources.Error_ShouldNotGetHere);
    }

    private PluginChainADONetDelegate<T> MakePluginChainDelegate<T>(string methodName)
    {
        PluginChainADONetDelegate<T>? pluginChainDelegate = null;
        for (int i = this.plugins.Count - 1; i >= 0; i--)
        {
            IConnectionPlugin plugin = this.plugins[i];
            IReadOnlySet<string> subscribedMethods = plugin.SubscribedMethods;
            bool isSubscribed = subscribedMethods.Contains(AllMethods) || subscribedMethods.Contains(methodName);

            if (isSubscribed)
            {
                if (pluginChainDelegate == null)
                {
                    // DefaultConnectionPlugin always terminates the list of plugins.
                    // Route the actual plugin invocation through InvokePluginWithNestedTrace
                    // so each plugin step gets its own nested telemetry span.
                    pluginChainDelegate = (pipelineDelegate, methodFunc, pluginToSkip) =>
                        this.InvokePluginWithNestedTrace(plugin, () => pipelineDelegate(plugin, methodFunc));
                }
                else
                {
                    PluginChainADONetDelegate<T> finalDelegate = pluginChainDelegate;
                    pluginChainDelegate = (pipelineDelegate, methodFunc, pluginToSkip) =>
                    {
                        if (plugin == pluginToSkip)
                        {
                            // Skipped plugin isn't actually invoked — no nested span is opened
                            // for it. The chain forwards directly to the next plugin.
                            return finalDelegate(pipelineDelegate, methodFunc, pluginToSkip);
                        }

                        return this.InvokePluginWithNestedTrace(
                            plugin,
                            () => pipelineDelegate(plugin, () => finalDelegate(pipelineDelegate, methodFunc, pluginToSkip)));
                    };
                }
            }
        }

        return pluginChainDelegate!;
    }

    /// <summary>
    /// Wraps a single plugin invocation in a <see cref="TelemetryTraceLevel.Nested"/>
    /// telemetry context so the plugin's step in the chain surfaces as its own span
    /// in the trace.
    /// </summary>
    private async Task<T> InvokePluginWithNestedTrace<T>(
        IConnectionPlugin plugin,
        Func<Task<T>> invocation)
    {
        // Resolve the span name from the PluginNameByType map so operators see
        // readable names (e.g., "plugin:failover") in trace UIs. Plugins that
        // aren't in the map fall back to their .NET type name unchanged.
        string typeName = plugin.GetType().Name;
        string spanName = PluginNameByType.TryGetValue(typeName, out string? prettyName)
            ? prettyName
            : typeName;

        ITelemetryContext nestedContext = this.TelemetryFactory.OpenTelemetryContext(
            spanName, TelemetryTraceLevel.Nested);
        try
        {
            T result = await invocation();
            nestedContext.SetSuccess(true);
            return result;
        }
        catch (Exception ex)
        {
            nestedContext.SetException(ex);
            nestedContext.SetSuccess(false);
            throw;
        }
        finally
        {
            nestedContext.CloseContext();
        }
    }

    public virtual Task<T> Execute<T>(
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        return this.ExecuteWithSubscribedPlugins(
            methodName,
            (plugin, methodFunc) => plugin.Execute(methodInvokeOn, methodName, methodFunc, methodArgs),
            methodFunc,
            null);
    }

    public virtual async Task<DbConnection> Open(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        IConnectionPlugin? pluginToSkip,
        bool async)
    {
        ITelemetryContext middleContext = this.TelemetryFactory.OpenTelemetryContext(
            "ConnectionPluginManager.Open", TelemetryTraceLevel.Nested);
        try
        {
            // Execute the plugin chain and return the connection
            DbConnection result = await this.ExecuteWithSubscribedPlugins<DbConnection>(
                ConnectMethod,
                (plugin, methodFunc) => plugin.OpenConnection(hostSpec, props, isInitialConnection, methodFunc, async),
                () => throw new UnreachableException(Resources.Error_FunctionShouldNotBeCalled),
                pluginToSkip);
            middleContext.SetSuccess(true);
            return result;
        }
        catch (Exception ex)
        {
            middleContext.SetException(ex);
            middleContext.SetSuccess(false);
            throw;
        }
        finally
        {
            middleContext.CloseContext();
        }
    }

    public virtual Task<DbConnection> ForceOpen(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        IConnectionPlugin? pluginToSkip,
        bool async)
    {
        // Execute the plugin chain and return the connection
        return this.ExecuteWithSubscribedPlugins<DbConnection>(
            ForceConnectMethod,
            (plugin, methodFunc) => plugin.ForceOpenConnection(hostSpec, props, isInitialConnection, methodFunc, async),
            () => throw new UnreachableException(Resources.Error_ShouldNotBeCalled),
            pluginToSkip);
    }

    public virtual async Task InitHostProvider(
        string initialConnectionString,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService)
    {
        ITelemetryContext middleContext = this.TelemetryFactory.OpenTelemetryContext(
            "ConnectionPluginManager.InitHostProvider", TelemetryTraceLevel.Nested);
        try
        {
            await this.ExecuteWithSubscribedPlugins<object>(
                InitHostMethod,
                async (plugin, methodFunc) =>
                {
                    await plugin.InitHostProvider(
                        initialConnectionString,
                        props,
                        hostListProviderService,
                        () => methodFunc());
                    return default!;
                },
                () => throw new InvalidOperationException(Resources.Error_ShouldNotBeCalled),
                null);
            middleContext.SetSuccess(true);
        }
        catch (Exception ex)
        {
            middleContext.SetException(ex);
            middleContext.SetSuccess(false);
            throw;
        }
        finally
        {
            middleContext.CloseContext();
        }
    }

    public HostSpec GetHostSpecByStrategy(HostRole hostRole, string strategy, Dictionary<string, string> props)
    {
        return this.GetHostSpecByStrategy(null, hostRole, strategy, props);
    }

    public HostSpec GetHostSpecByStrategy(IList<HostSpec>? hosts, HostRole hostRole, string strategy, Dictionary<string, string> props)
    {
        var targeHosts = hosts ?? this.pluginService!.GetHosts();

        if (this.defaultConnProvider.GetHostSpecByStrategy(
                targeHosts,
                hostRole,
                strategy,
                props) is { } hostSpec)
        {
            return hostSpec;
        }

        throw new NotSupportedException(string.Format(Resources.Error_DriverDoesNotSupportRequestedHostSelectionStrategy, strategy));
    }

    public virtual bool AcceptsStrategy(string strategy)
    {
        return this.defaultConnProvider.AcceptsStrategy(strategy);
    }

    public bool IsPluginActive(string pluginCode)
    {
        return this.activePluginCodes.Contains(pluginCode);
    }
}
