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
using AwsWrapperDataProvider.Driver.Plugins.AuroraInitialConnectionStrategy;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Plugins.ExecutionTime;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Plugins.SecretsManager;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.Plugins;

public class ConnectionPluginChainBuilder
{
    private const int WeightRelativeToPriorPlugin = -1;
    private const string DefaultPluginCode = "efm,failover";

    private static readonly Dictionary<string, Type> PluginFactoryTypesByCode = new()
    {
            { "executionTime", typeof(ExecutionTimePlugin) },
            { "failover", typeof(FailoverPluginFactory) },
            { "efm", typeof(HostMonitoringPluginFactory) },
            { "iam", typeof(IamAuthPluginFactory) },
            { "awsSecretsManager", typeof(SecretsManagerAuthPluginFactory) },
            { "initialConnection", typeof(AuroraInitialConnectionStrategyPluginFactory) },
            { "federatedAuth", typeof(FederatedAuthPluginFactory) },
    };

    private static readonly Dictionary<Type, int> PluginWeightByPluginFactoryType = new()
    {
            { typeof(AuroraInitialConnectionStrategyPluginFactory), 390 },
            { typeof(FailoverPluginFactory), 700 },
            { typeof(HostMonitoringPluginFactory), 800 },
            { typeof(IamAuthPluginFactory), 1000 },
            { typeof(SecretsManagerAuthPluginFactory), 1100 },
            { typeof(FederatedAuthPluginFactory), 1200 },
            { typeof(ExecutionTimePlugin), WeightRelativeToPriorPlugin },
    };

    public IList<IConnectionPlugin> GetPlugins(
        IPluginService pluginService,
        IConnectionProvider defaultConnectionProvider,
        IConnectionProvider? effectiveConnectionProvider,
        Dictionary<string, string> props,
        ConfigurationProfile? configurationProfile)
    {
        List<IConnectionPluginFactory> pluginFactories;

        if (configurationProfile is { PluginFactories: not null })
        {
            pluginFactories = configurationProfile.PluginFactories;
        }
        else
        {
            string pluginsCodes = PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCode;
            string[] pluginsCodesArray = [.. pluginsCodes.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)];
            Console.WriteLine("Current Plugins: " + string.Join(",", pluginsCodesArray));

            pluginFactories = new(pluginsCodesArray.Length);

            foreach (string pluginCode in pluginsCodesArray)
            {
                if (!PluginFactoryTypesByCode.TryGetValue(pluginCode, out Type? pluginFactoryType))
                {
                    throw new Exception(string.Format(Properties.Resources.Error_UnknownPluginCode, pluginCode));
                }

                IConnectionPluginFactory? factoryInstance = (IConnectionPluginFactory?)Activator.CreateInstance(pluginFactoryType);
                if (factoryInstance == null)
                {
                    throw new Exception(string.Format(Properties.Resources.Error_UnableToLoadPlugin, pluginCode));
                }

                pluginFactories.Add(factoryInstance);
            }

            if (pluginFactories.Count > 1 && PropertyDefinition.AutoSortPluginOrder.GetBoolean(props))
            {
                pluginFactories = this.SortPluginFactories(pluginFactories);
            }
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

    private List<IConnectionPluginFactory> SortPluginFactories(List<IConnectionPluginFactory> pluginFactories)
    {
        int lastWeight = 0;
        return [.. pluginFactories.OrderBy(pluginFactory =>
            {
                int pluginWeight = PluginWeightByPluginFactoryType[pluginFactory.GetType()];
                if (pluginWeight == WeightRelativeToPriorPlugin)
                {
                    lastWeight++;
                    return lastWeight;
                }

                lastWeight = pluginWeight;
                return pluginWeight;
            })];
    }
}
