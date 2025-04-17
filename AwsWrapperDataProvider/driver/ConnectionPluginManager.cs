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
using AwsWrapperDataProvider.driver.connectionProviders;
using AwsWrapperDataProvider.driver.hostInfo;
using AwsWrapperDataProvider.driver.plugins;

namespace AwsWrapperDataProvider.driver;

public class ConnectionPluginManager(
    IConnectionProvider defaultConnectionProvider,
    IConnectionProvider? effectiveConnectionProvider,
    AwsWrapperConnection connection)
{
    protected Dictionary<string, string> Props = new Dictionary<string, string>();
    protected IList<IConnectionPlugin> Plugins = new List<IConnectionPlugin>();
    protected IConnectionProvider DefaultConnProvider = defaultConnectionProvider;
    protected IConnectionProvider? EffectiveConnProvider = effectiveConnectionProvider;
    protected AwsWrapperConnection ConnectionWrapper { get; } = connection;
    protected IPluginService? PluginService;

    public void Init(
        IPluginService pluginService,
        Dictionary<string, string> props
    )
    {
        Props = props;
        PluginService = pluginService;
        Plugins = ConnectionPluginChainBuilder.GetPlugins(
            PluginService,
            DefaultConnProvider,
            EffectiveConnProvider,
            Props);
    }

    public T Execute<T>(
        object methodInvokeOn,
        string methodName,
        JdbcCallable<T> jdbcMethodFunc,
        object[] jdbcMethodArgs)
    {
        // TODO: assume that Plugins only contains DefaultConnectionPlugin, implement actual use of plugin chain.
        return Plugins.First().Execute<T>(methodInvokeOn, methodName, jdbcMethodFunc, jdbcMethodArgs);
    }

    
    // TODO: investigate if there is better solution to void return types
    public void Execute(
        object methodInvokeOn,
        string methodName,
        JdbcCallable jdbcMethodFunc,
        object[] jdbcMethodArgs)
    {
        // TODO: assume that Plugins only contains DefaultConnectionPlugin, implement actual use of plugin chain.
        Plugins.First().Execute(methodInvokeOn, methodName, jdbcMethodFunc, jdbcMethodArgs);
    }

    public DbConnection Connect(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        IConnectionPlugin? pluginToSkip)
    {
        // TODO: assume that Plugins only contains DefaultConnectionPlugin, implement actual use of plugin chain.
        return Plugins.First().Connect(hostSpec, props, isInitialConnection, (args) => throw new NotImplementedException("should not be called"));
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