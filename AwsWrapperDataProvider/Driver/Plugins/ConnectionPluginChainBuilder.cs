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

using System.Linq;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins.AuroraInitialConnectionStrategy;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Plugins.ExecutionTime;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins;

public class ConnectionPluginChainBuilder
{
    private const int WeightRelativeToPriorPlugin = -1;
    private const string DefaultPluginCode = "efm,failover";

    private static readonly ILogger<ConnectionPluginChainBuilder> Logger = LoggerUtils.GetLogger<ConnectionPluginChainBuilder>();

    private static readonly Dictionary<string, Type> PluginFactoryTypesByCode = new()
    {
            { PluginCodes.ExecutionTime, typeof(ExecutionTimePlugin) },
            { PluginCodes.Failover, typeof(FailoverPluginFactory) },
            { PluginCodes.HostMonitoring, typeof(HostMonitoringPluginFactory) },
            { PluginCodes.InitialConnection, typeof(AuroraInitialConnectionStrategyPluginFactory) },
    };

    private static readonly Dictionary<string, int> PluginWeightByPluginFactoryType = new()
    {
            { PluginCodes.InitialConnection, 390 },
            { PluginCodes.Failover, 700 },
            { PluginCodes.HostMonitoring, 800 },
            { PluginCodes.Iam, 1000 },
            { PluginCodes.SecretsManager, 1100 },
            { PluginCodes.FederatedAuth, 1200 },
            { PluginCodes.Okta, 1300 },
            { PluginCodes.ExecutionTime, WeightRelativeToPriorPlugin },
    };

    private static readonly List<string> AWSDependentPlugins =
    [
        PluginCodes.Iam,
        PluginCodes.SecretsManager,
        PluginCodes.FederatedAuth,
        PluginCodes.Okta,
    ];

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
            Logger.LogDebug("Current Plugins: " + string.Join(",", pluginsCodesArray));

            Dictionary<int, IConnectionPluginFactory> pluginFactoriesByWeight = [];

            foreach (string pluginCode in pluginsCodesArray)
            {
                IConnectionPluginFactory factoryInstance = this.GetPluginFactory(pluginCode);
                int factoryWeight = PluginWeightByPluginFactoryType.GetValueOrDefault(pluginCode, WeightRelativeToPriorPlugin);
                pluginFactoriesByWeight.Add(factoryWeight, factoryInstance);
            }

            if (pluginFactoriesByWeight.Count > 1 && PropertyDefinition.AutoSortPluginOrder.GetBoolean(props))
            {
                pluginFactories = this.SortPluginFactories(pluginFactoriesByWeight);
            }
            else
            {
                pluginFactories = pluginFactoriesByWeight.Values.ToList();
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

    private IConnectionPluginFactory GetPluginFactory(string pluginCode)
    {

        if (AWSDependentPlugins.Contains(pluginCode))
        {
            return AwsAuthenticationPluginLoader.LoadAwsFactory(pluginCode);
        }
        else
        {
            if (!PluginFactoryTypesByCode.TryGetValue(pluginCode, out Type? pluginFactoryType))
            {
                throw new Exception(string.Format(Properties.Resources.Error_UnknownPluginCode, pluginCode));
            }

            IConnectionPluginFactory? factoryInstance = (IConnectionPluginFactory?)Activator.CreateInstance(pluginFactoryType)
                ?? throw new Exception(string.Format(Properties.Resources.Error_UnableToLoadPlugin, pluginCode));
            return factoryInstance;
        }
    }

    private List<IConnectionPluginFactory> SortPluginFactories(Dictionary<int, IConnectionPluginFactory> pluginFactoriesByWeight)
    {
        int lastWeight = 0;
        return pluginFactoriesByWeight.OrderBy(pluginWeightFactoryPair =>
            {
                int pluginWeight = pluginWeightFactoryPair.Key;

                if (pluginWeight == WeightRelativeToPriorPlugin)
                {
                    lastWeight++;
                    return lastWeight;
                }

                lastWeight = pluginWeight;
                return pluginWeight;
            })
            .Select(pluginWeightFactoryPair => pluginWeightFactoryPair.Value)
            .ToList();
    }
}
