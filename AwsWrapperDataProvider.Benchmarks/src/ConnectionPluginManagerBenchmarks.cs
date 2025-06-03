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

using AwsWrapperDataProvider.Benchmarks.Mocks;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace AwsWrapperDataProvider.Benchmarks
{
    // TODO: Add ExecutionTimePlugin

    [SimpleJob(RunStrategy.Monitoring, warmupCount: 3)]
    [MemoryDiagnoser]
    public class ConnectionPluginManagerBenchmarks
    {
        private const int OperationsPerInvoke = 500000;

        private ConnectionPluginManager _pluginManagerWithNoPlugins;
        private ConnectionPluginManager _pluginManagerWithPlugins;
        private Dictionary<string, string> _propWithPlugins;
        private Dictionary<string, string> _propWithNoPlugins;

        [IterationSetup]
        public void IterationSetup()
        {
            // Setup mocks
            // Create a plugin chain with 10 custom test plugins
            var pluginFactories = new List<IConnectionPluginFactory>();
            for (int i = 0; i < 10; i++)
            {
                pluginFactories.Add(new BenchmarkPluginFactory());
            }

            var configurationProfileWithPlugins = ConfigurationProfileBuilder.Get()
                .WithName("benchmark-with-plugins")
                .WithDialect(new MockDialect())
                .WithTargetConnectionDialect(new MockConnectionDialect())
                .WithConnectionProvider(new DbConnectionProvider())
                .WithPluginFactories(pluginFactories)
                .Build();

            var configurationProfileWithNoPlugins = ConfigurationProfileBuilder.Get()
                .WithName("benchmark-with-no-plugins")
                .WithDialect(new MockDialect())
                .WithTargetConnectionDialect(new MockConnectionDialect())
                .WithConnectionProvider(new DbConnectionProvider())
                .WithPluginFactories([])
                .Build();

            var connectionWrapperWithPlugins = new AwsWrapperConnection(new MockConnection(), configurationProfileWithPlugins);
            var connectionWrapperWithNoPlugins = new AwsWrapperConnection(new MockConnection(), configurationProfileWithNoPlugins);

            this._pluginManagerWithPlugins = connectionWrapperWithPlugins.PluginManager;
            this._propWithPlugins = connectionWrapperWithPlugins.ConnectionProperties;

            this._pluginManagerWithNoPlugins = connectionWrapperWithNoPlugins.PluginManager;
            this._propWithNoPlugins = connectionWrapperWithNoPlugins.ConnectionProperties;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void Open_WithPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                this._pluginManagerWithPlugins.Open(
                    new HostSpecBuilder().WithHost("host").Build(),
                    this._propWithPlugins,
                    true,
                    null,
                    () => { });
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void Open_WithNoPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                this._pluginManagerWithNoPlugins.Open(
                    new HostSpecBuilder().WithHost("host").Build(),
                    this._propWithNoPlugins,
                    true,
                    null,
                    () => { });
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void Execute_WithPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                this._pluginManagerWithPlugins.Execute(
                    new MockCommand().ExecuteNonQuery(),
                    "DbCommand.ExecuteNonQuery",
                    () => 1,
                    Array.Empty<object>());
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public void Execute_WithNoPlugins()
        {
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                this._pluginManagerWithNoPlugins.Execute(
                    new MockCommand().ExecuteNonQuery(),
                    "DbCommand.ExecuteNonQuery",
                    () => 1,
                    Array.Empty<object>());
            }
        }
    }
}
