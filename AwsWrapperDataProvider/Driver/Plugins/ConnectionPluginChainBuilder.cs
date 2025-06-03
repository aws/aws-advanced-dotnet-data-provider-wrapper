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

using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.Plugins;

public class ConnectionPluginChainBuilder
{
    private const string DefaultPluginCode = "efm,failover";

    private static readonly Dictionary<string, Type> PluginFactoryTypesByCode = new()
    {
            { "failover", typeof(FailoverPluginFactory) },
            { "efm", typeof(HostMonitoringPluginFactory) },
            { "iam", typeof(IamAuthPluginFactory) },
    };

    private static readonly Dictionary<Type, int> PluginWeightByPluginFactoryType = new()
    {
            { typeof(FailoverPluginFactory), 700 },
            { typeof(HostMonitoringPluginFactory), 800 },
            { typeof(IamAuthPluginFactory), 1000 },
    };

    public IList<IConnectionPlugin> GetPlugins(
        IPluginService pluginService,
        IConnectionProvider defaultConnectionProvider,
        IConnectionProvider? effectiveConnectionProvider,
        Dictionary<string, string> props)
    {
        string pluginsCodes = PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCode;
        string[] pluginsCodesArray = [.. pluginsCodes.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)];

        List<IConnectionPluginFactory> pluginFactories = new(pluginsCodesArray.Length);

        foreach (string pluginCode in pluginsCodesArray)
        {
            if (!PluginFactoryTypesByCode.TryGetValue(pluginCode, out Type? pluginFactoryType))
            {
                throw new Exception($"ConnectionPluginManager.unknownPluginCode: {pluginCode}");
            }

            IConnectionPluginFactory? factoryInstance = (IConnectionPluginFactory?)Activator.CreateInstance(pluginFactoryType);
            if (factoryInstance == null)
            {
                throw new Exception($"ConnectionPluginManager.unableToLoadPlugin: {pluginCode}");
            }

            pluginFactories.Add(factoryInstance);
        }

        if (pluginFactories.Count > 1 && PropertyDefinition.AutoSortPluginOrder.GetBoolean(props))
        {
            pluginFactories = [.. pluginFactories.OrderBy(pluginFactory => PluginWeightByPluginFactoryType[pluginFactory.GetType()])];
        }

        List<IConnectionPlugin> plugins = new(pluginFactories.Count + 1);
        foreach (IConnectionPluginFactory pluginFactory in pluginFactories)
        {
            IConnectionPlugin pluginInstance = pluginFactory.GetInstance(pluginService, props);
            plugins.Add(pluginInstance);
        }

        IConnectionPlugin defaultConnectionPlugin = new DefaultConnectionPlugin(
            pluginService,
            defaultConnectionProvider,
            effectiveConnectionProvider);

        plugins.Add(defaultConnectionPlugin);

        return plugins;
    }
}
