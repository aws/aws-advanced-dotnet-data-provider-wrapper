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
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Driver;

public class ConnectionPluginManager
{
    protected Dictionary<string, string> props = [];
    private readonly Dictionary<string, Delegate> pluginChainDelegates = [];
    protected IList<IConnectionPlugin> plugins = [];
    protected IConnectionProvider defaultConnProvider;
    protected IConnectionProvider? effectiveConnProvider;
    protected AwsWrapperConnection ConnectionWrapper { get; }
    protected IPluginService? pluginService;
    private const string AllMethods = "*";
    private const string ConnectMethod = "DbConnection.Open";

    private delegate T PluginPipelineDelegate<T>(IConnectionPlugin plugin, ADONetDelegate<T> methodFunc);
    private delegate T PluginChainADONetDelegate<T>(PluginPipelineDelegate<T> pipelineDelegate, ADONetDelegate<T> methodFunc, IConnectionPlugin pluginToSkip);

    public ConnectionPluginManager(
        IConnectionProvider defaultConnectionProvider,
        IConnectionProvider? effectiveConnectionProvider,
        AwsWrapperConnection connection)
    {
        this.defaultConnProvider = defaultConnectionProvider;
        this.effectiveConnProvider = effectiveConnectionProvider;
        this.ConnectionWrapper = connection;
    }

    // for testing purpose only
    public ConnectionPluginManager(
        IConnectionProvider defaultConnectionProvider,
        IConnectionProvider? effectiveConnectionProvider,
        Dictionary<string, string> props,
        IList<IConnectionPlugin> plugins,
        AwsWrapperConnection connection)
    {
        this.defaultConnProvider = defaultConnectionProvider;
        this.effectiveConnProvider = effectiveConnectionProvider;
        this.props = props;
        this.plugins = plugins;
        this.ConnectionWrapper = connection;
    }

    public void InitConnectionPluginChain(
        IPluginService pluginService,
        Dictionary<string, string> props)
    {
        this.props = props;
        this.pluginService = pluginService;
        ConnectionPluginChainBuilder pluginChainBuilder = new();
        this.plugins = pluginChainBuilder.GetPlugins(
            this.pluginService,
            this.defaultConnProvider,
            this.effectiveConnProvider,
            this.props);
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
            throw new Exception("Error processing this ADO.NET call.");
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
            ISet<string> subscribedMethods = plugin.GetSubscribeMethods();
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
                    pluginChainDelegate = (pluginPipelineDelegate, methodFunc, pluginToSkip) =>
                    {
                        return plugin == pluginToSkip
                            ? pluginChainDelegate(pluginPipelineDelegate, methodFunc, pluginToSkip)
                            : pluginPipelineDelegate(plugin, () => pluginChainDelegate(pluginPipelineDelegate, methodFunc, pluginToSkip));
                    };
                }
            }
        }

        return pluginChainDelegate!;
    }

    public T Execute<T>(
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        object[] methodArgs)
    {
        return this.ExecuteWithSubscribedPlugins(
            methodName,
            (plugin, methodFunc) => plugin.Execute(methodInvokeOn, methodName, methodFunc, methodArgs),
            methodFunc,
            null)!;
    }

    public void Open(
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

    public void InitHostProvider(
        string initialConnectionString,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService)
    {
        // TODO: stub method, implement method
        return;
    }
}
