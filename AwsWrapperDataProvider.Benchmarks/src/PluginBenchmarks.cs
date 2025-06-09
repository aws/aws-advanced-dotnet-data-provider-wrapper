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

using System.Runtime.Versioning;
using AwsWrapperDataProvider.Benchmarks.Mocks;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.ExecutionTime;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace AwsWrapperDataProvider.Benchmarks
{
    [SimpleJob(RunStrategy.Monitoring, warmupCount: 3)]
    [MemoryDiagnoser]
    public class PluginBenchmarks
    {
        private const int OperationsPerInvoke = 500000;
        private const int PluginChainLength = 10;

        private ConfigurationProfile? _configurationProfileWithPlugins;
        private ConfigurationProfile? _configurationProfileWithNoPlugins;
        private ConfigurationProfile? _configurationProfileWithExecutionTimePlugin;

        [IterationSetup]
        public void IterationSetup()
        {
            // Setup mocks
            // Create a plugin chain with 10 custom test plugins
            var pluginFactories = new List<IConnectionPluginFactory>();
            for (int i = 0; i < PluginChainLength; i++)
            {
                pluginFactories.Add(new BenchmarkPluginFactory());
            }

            var pluginFactoriesWithExecutionTime = new List<IConnectionPluginFactory>();
            pluginFactoriesWithExecutionTime.Add(new ExecutionTimePluginFactory());
            for (int i = 0; i < PluginChainLength; i++)
            {
                pluginFactoriesWithExecutionTime.Add(new BenchmarkPluginFactory());
            }

            this._configurationProfileWithPlugins = ConfigurationProfileBuilder.Get()
                .WithName("benchmark-with-plugins")
                .WithDialect(new MockDialect())
                .WithTargetConnectionDialect(new MockConnectionDialect())
                .WithConnectionProvider(new DbConnectionProvider())
                .WithPluginFactories(pluginFactories)
                .Build();

            this._configurationProfileWithNoPlugins = ConfigurationProfileBuilder.Get()
                .WithName("benchmark-with-no-plugins")
                .WithDialect(new MockDialect())
                .WithTargetConnectionDialect(new MockConnectionDialect())
                .WithConnectionProvider(new DbConnectionProvider())
                .WithPluginFactories([])
                .Build();

            this._configurationProfileWithExecutionTimePlugin = ConfigurationProfileBuilder.Get()
                .WithName("benchmark-with-plugins")
                .WithDialect(new MockDialect())
                .WithTargetConnectionDialect(new MockConnectionDialect())
                .WithConnectionProvider(new DbConnectionProvider())
                .WithPluginFactories(pluginFactoriesWithExecutionTime)
                .Build();
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void ConnectionPluginManager_InitAndRelease_WithPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                using var connection = new AwsWrapperConnection(new MockConnection(), this._configurationProfileWithPlugins);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void ConnectionPluginMananger_InitAndRelease_WithoutPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                using var connection =
                    new AwsWrapperConnection(new MockConnection(), this._configurationProfileWithNoPlugins);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void ExecuteStatement_WithPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                using var connection =
                    new AwsWrapperConnection(new MockConnection(), this._configurationProfileWithPlugins);
                using var command = connection.CreateCommand();
                using var reader = command.ExecuteReader();
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void ExecuteStatement_WithoutPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                using var connection = new AwsWrapperConnection(new MockConnection(), this._configurationProfileWithPlugins);
                using var command = connection.CreateCommand();
                using var reader = command.ExecuteReader();
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void ConnectionPluginManager_InitAndRelease_WithExecutionTimePlugin()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                using var connection = new AwsWrapperConnection(new MockConnection(), this._configurationProfileWithExecutionTimePlugin);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void ExecuteStatement_WithExecutionTimePlugin()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                using var connection =
                    new AwsWrapperConnection(new MockConnection(), this._configurationProfileWithExecutionTimePlugin);
                using var command = connection.CreateCommand();
                using var reader = command.ExecuteReader();
            }
        }
    }
}
