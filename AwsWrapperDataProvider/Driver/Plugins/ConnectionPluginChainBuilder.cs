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

    private static readonly Dictionary<string, Lazy<IConnectionPluginFactory>?> PluginFactoryTypesByCode = new()
    {
            { PluginCodes.ExecutionTime, new Lazy<IConnectionPluginFactory>(() => new ExecutionTimePluginFactory()) },
            { PluginCodes.Failover, new Lazy<IConnectionPluginFactory>(() => new FailoverPluginFactory()) },
            { PluginCodes.HostMonitoring, new Lazy<IConnectionPluginFactory>(() => new HostMonitoringPluginFactory()) },
            { PluginCodes.InitialConnection, new Lazy<IConnectionPluginFactory>(() => new AuroraInitialConnectionStrategyPluginFactory()) },
            { PluginCodes.Iam, null },
            { PluginCodes.SecretsManager, null },
            { PluginCodes.FederatedAuth, null },
            { PluginCodes.Okta, null },
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
                if (!PluginFactoryTypesByCode.TryGetValue(pluginCode, out Lazy<IConnectionPluginFactory>? factory))
                {
                    throw new Exception(string.Format(Properties.Resources.Error_UnknownPluginCode, pluginCode));
                }

                if (factory == null)
                {
                    throw new Exception(string.Format(Properties.Resources.Error_UnableToLoadPlugin, pluginCode));
                }

                int factoryWeight = PluginWeightByPluginFactoryType.GetValueOrDefault(pluginCode, WeightRelativeToPriorPlugin);
                pluginFactoriesByWeight.Add(factoryWeight, factory.Value);
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

        List<IConnectionPlugin> plugins = [
            ..pluginFactories.Select(factory => factory.GetInstance(pluginService, props)),
            new DefaultConnectionPlugin(pluginService, defaultConnectionProvider, effectiveConnectionProvider)
        ];

        return plugins;
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

    public static void RegisterPluginFactory<T>(string pluginCode)
        where T : IConnectionPluginFactory, new()
    {
        PluginFactoryTypesByCode[pluginCode] = new Lazy<IConnectionPluginFactory>(() => new T());
    }
}
