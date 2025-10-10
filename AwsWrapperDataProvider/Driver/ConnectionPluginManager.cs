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
using System.Threading.Tasks;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Driver;

public class ConnectionPluginManager
{
    private readonly Dictionary<string, Delegate> pluginChainDelegates = [];
    protected IList<IConnectionPlugin> plugins = [];
    protected IConnectionProvider defaultConnProvider;
    protected IConnectionProvider? effectiveConnProvider;
    protected ConfigurationProfile? configurationProfile;
    protected AwsWrapperConnection ConnectionWrapper { get; }
    protected IPluginService? pluginService;
    private const string AllMethods = "*";
    private const string GetHostSpecByStrategyMethod = "GetHostSpecByStrategy";
    private const string ConnectMethod = "DbConnection.Open";
    private const string ForceConnectMethod = "DbConnection.ForceOpen";
    private const string InitHostMethod = "initHostProvider";

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
            this.pluginChainDelegates.Add(methodName, del);
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

        throw new UnreachableException("Should not get here.");
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
                    // DefaultConnectionPlugin always terminates the list of plugins
                    pluginChainDelegate = async (pipelineDelegate, methodFunc, pluginToSkip) => await pipelineDelegate(plugin, methodFunc);
                }
                else
                {
                    PluginChainADONetDelegate<T> finalDelegate = pluginChainDelegate;
                    pluginChainDelegate = async (pipelineDelegate, methodFunc, pluginToSkip) =>
                    {
                        return plugin == pluginToSkip
                            ? await finalDelegate(pipelineDelegate, methodFunc, pluginToSkip)
                            : await pipelineDelegate(plugin, async () => await finalDelegate(pipelineDelegate, methodFunc, pluginToSkip));
                    };
                }
            }
        }

        return pluginChainDelegate!;
    }

    public virtual async Task<T> Execute<T>(
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        return await this.ExecuteWithSubscribedPlugins(
            methodName,
            async (plugin, methodFunc) => await plugin.Execute(methodInvokeOn, methodName, methodFunc, methodArgs),
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
        // Execute the plugin chain and return the connection
        return await this.ExecuteWithSubscribedPlugins<DbConnection>(
            ConnectMethod,
            async (plugin, methodFunc) => await plugin.OpenConnection(hostSpec, props, isInitialConnection, () => methodFunc(), async),
            () => throw new UnreachableException("Function should not be called."),
            pluginToSkip);
    }

    public virtual async Task<DbConnection> ForceOpen(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        IConnectionPlugin? pluginToSkip,
        bool async)
    {
        // Execute the plugin chain and return the connection
        return await this.ExecuteWithSubscribedPlugins<DbConnection>(
            ForceConnectMethod,
            async (plugin, methodFunc) => await plugin.ForceOpenConnection(hostSpec, props, isInitialConnection, () => methodFunc(), async),
            () => throw new UnreachableException("Function should not be called."),
            pluginToSkip);
    }

    public virtual async Task InitHostProvider(
        string initialConnectionString,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService)
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
            () => throw new InvalidOperationException("Should not be called"),
            null);
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

        throw new NotSupportedException($"The driver does not support the requested host selection strategy: {strategy}");
    }

    public virtual bool AcceptsStrategy(string strategy)
    {
        return this.defaultConnProvider.AcceptsStrategy(strategy);
    }
}
