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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class ConnectionPluginChainBuilderTests
{
    private readonly Mock<IPluginService> pluginServiceMock = new();
    private readonly Mock<IConnectionProvider> connectionProviderMock = new();

    [Fact]
    [Trait("Category", "Unit")]
    public void TestSortPlugins()
    {
        Dictionary<string, string> props = new() { { PropertyDefinition.Plugins.Name, "efm,failover" } };
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        IList<IConnectionPlugin> plugins = pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            props);

        Assert.NotNull(plugins);
        Assert.Equal(3, plugins.Count);
        Assert.IsType<FailoverPlugin>(plugins[0]);
        Assert.IsType<HostMonitoringPlugin>(plugins[1]);
        Assert.IsType<DefaultConnectionPlugin>(plugins[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestPreservePluginOrder()
    {
        Dictionary<string, string> props = new()
        {
            { PropertyDefinition.Plugins.Name, "efm,failover" },
            { PropertyDefinition.AutoSortPluginOrder.Name, "false" },
        };
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        IList<IConnectionPlugin> plugins = pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            props);

        Assert.NotNull(plugins);
        Assert.Equal(3, plugins.Count);
        Assert.IsType<HostMonitoringPlugin>(plugins[0]);
        Assert.IsType<FailoverPlugin>(plugins[1]);
        Assert.IsType<DefaultConnectionPlugin>(plugins[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestPluginCodesWithSpaces()
    {
        Dictionary<string, string> props = new() { { PropertyDefinition.Plugins.Name, " efm ,  failover " } };
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        IList<IConnectionPlugin> plugins = pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            props);

        Assert.NotNull(plugins);
        Assert.Equal(3, plugins.Count);
        Assert.IsType<FailoverPlugin>(plugins[0]);
        Assert.IsType<HostMonitoringPlugin>(plugins[1]);
        Assert.IsType<DefaultConnectionPlugin>(plugins[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestPluginCodesWithCommasAndSpaces()
    {
        Dictionary<string, string> props = new() { { PropertyDefinition.Plugins.Name, " efm , ,,failover" } };
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        IList<IConnectionPlugin> plugins = pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            props);

        Assert.NotNull(plugins);
        Assert.Equal(3, plugins.Count);
        Assert.IsType<FailoverPlugin>(plugins[0]);
        Assert.IsType<HostMonitoringPlugin>(plugins[1]);
        Assert.IsType<DefaultConnectionPlugin>(plugins[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestDefaultPluginCodes()
    {
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        IList<IConnectionPlugin> plugins = pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            []);

        Assert.NotNull(plugins);
        Assert.Equal(3, plugins.Count);
        Assert.IsType<FailoverPlugin>(plugins[0]);
        Assert.IsType<HostMonitoringPlugin>(plugins[1]);
        Assert.IsType<DefaultConnectionPlugin>(plugins[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestEmptyPlugins()
    {
        Dictionary<string, string> props = new() { { PropertyDefinition.Plugins.Name, string.Empty } };
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        IList<IConnectionPlugin> plugins = pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            props);

        Assert.NotNull(plugins);
        Assert.Single(plugins);
        Assert.IsType<DefaultConnectionPlugin>(plugins[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestUnknownPlugin()
    {
        Dictionary<string, string> props = new() { { PropertyDefinition.Plugins.Name, "unknown" } };
        ConnectionPluginChainBuilder pluginChainBuilder = new();

        Assert.Throws<Exception>(() => pluginChainBuilder.GetPlugins(
            this.pluginServiceMock.Object,
            this.connectionProviderMock.Object,
            null,
            props));
    }
}
