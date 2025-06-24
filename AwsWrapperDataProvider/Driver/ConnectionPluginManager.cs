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
    private const string InitHostMethod = "initHostProvider";

    private delegate T PluginPipelineDelegate<T>(IConnectionPlugin plugin, ADONetDelegate<T> methodFunc);
    private delegate T PluginChainADONetDelegate<T>(PluginPipelineDelegate<T> pipelineDelegate, ADONetDelegate<T> methodFunc, IConnectionPlugin pluginToSkip);

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

    private T ExecuteWithSubscribedPlugins<T>(
        string methodName,
        PluginPipelineDelegate<T> pluginPipelineDelegate,
        ADONetDelegate<T> methodFunc,
        IConnectionPlugin? pluginToSkip)
    {
        ArgumentNullException.ThrowIfNull(pluginPipelineDelegate);
        ArgumentNullException.ThrowIfNull(methodFunc);

        if (!this.pluginChainDelegates.TryGetValue(methodName, out Delegate? pluginChainDelegate))
        {
            pluginChainDelegate = this.MakePluginChainDelegate<T>(methodName);
            this.pluginChainDelegates.Add(methodName, pluginChainDelegate!);
        }

        if (pluginChainDelegate == null)
        {
            throw new Exception(Properties.Resources.Error_ProcessingAdoNetCall);
        }

        return (T)pluginChainDelegate.DynamicInvoke(
            pluginPipelineDelegate,
            methodFunc,
            pluginToSkip)!;
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
                    pluginChainDelegate = (pipelineDelegate, methodFunc, pluginToSkip) => pipelineDelegate(plugin, methodFunc);
                }
                else
                {
                    PluginChainADONetDelegate<T> finalDelegate = pluginChainDelegate;
                    pluginChainDelegate = (pipelineDelegate, methodFunc, pluginToSkip) =>
                    {
                        return plugin == pluginToSkip
                            ? finalDelegate(pipelineDelegate, methodFunc, pluginToSkip)
                            : pipelineDelegate(plugin, () => finalDelegate(pipelineDelegate, methodFunc, pluginToSkip));
                    };
                }
            }
        }

        return pluginChainDelegate!;
    }

    public virtual T Execute<T>(
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        return this.ExecuteWithSubscribedPlugins(
            methodName,
            (plugin, methodFunc) => plugin.Execute(methodInvokeOn, methodName, methodFunc, methodArgs),
            methodFunc,
            null)!;
    }

    public virtual void Open(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        IConnectionPlugin? pluginToSkip,
        ADONetDelegate openFunc)
    {
        // Type object does not mean anything
        this.ExecuteWithSubscribedPlugins<object>(
            ConnectMethod,
            (plugin, methodFunc) =>
            {
                plugin.OpenConnection(hostSpec, props, isInitialConnection, () => methodFunc());
                return default!;
            },
            () =>
            {
                openFunc();
                return default!;
            },
            pluginToSkip);
    }

    public virtual void InitHostProvider(
        string initialConnectionString,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService)
    {
        this.ExecuteWithSubscribedPlugins<object>(
            InitHostMethod,
            (plugin, methodFunc) =>
            {
                plugin.InitHostProvider(
                    initialConnectionString,
                    props,
                    hostListProviderService,
                    () => methodFunc());
                return default;
            },
            () => throw new InvalidOperationException("Should not be called"),
            null);
    }

    public HostSpec GetHostSpecByStrategy(HostRole hostRole, string strategy, Dictionary<string, string> props)
    {
        if (this.defaultConnProvider.GetHostSpecByStrategy(
                this.pluginService!.GetHosts(),
                hostRole,
                strategy,
                props) is { } hostSpec)
        {
            return hostSpec;
        }

        throw new InvalidOperationException($"The driver does not support the requested host selection strategy: {strategy}");
    }

    public virtual bool AcceptsStrategy(string strategy)
    {
        return this.defaultConnProvider.AcceptsStrategy(strategy);
    }
}
