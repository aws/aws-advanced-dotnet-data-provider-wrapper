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
using Amazon;
using Amazon.RDS;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.CustomEndpoint;

public class CustomEndpointPluginTests : IDisposable
{
    private const string WriterClusterUrl = "writer.cluster-XYZ.us-east-1.rds.amazonaws.com";
    private const string CustomEndpointUrl = "custom.cluster-custom-XYZ.us-east-1.rds.amazonaws.com";

    private readonly Dictionary<string, string> props = [];
    private readonly Mock<IPluginService> mockPluginService;
    private readonly HostSpec writerClusterHost;
    private readonly HostSpec customEndpointHost;
    private readonly Mock<ADONetDelegate<DbConnection>> mockConnectFunc;
    private readonly Mock<ADONetDelegate<object>> mockJdbcMethodFunc;
    private readonly Mock<DbConnection> mockConnection;
    private readonly Mock<ICustomEndpointMonitor> mockMonitor;

    public CustomEndpointPluginTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.writerClusterHost = new HostSpec(WriterClusterUrl, 5432, "writer", HostRole.Writer, HostAvailability.Available, HostSpec.DefaultWeight, DateTime.UtcNow);
        this.customEndpointHost = new HostSpec(CustomEndpointUrl, 5432, "custom", HostRole.Writer, HostAvailability.Available, HostSpec.DefaultWeight, DateTime.UtcNow);
        this.mockConnectFunc = new Mock<ADONetDelegate<DbConnection>>();
        this.mockJdbcMethodFunc = new Mock<ADONetDelegate<object>>();
        this.mockConnection = new Mock<DbConnection>();
        this.mockMonitor = new Mock<ICustomEndpointMonitor>();

        this.mockConnectFunc.Setup(f => f()).ReturnsAsync(this.mockConnection.Object);
        this.mockJdbcMethodFunc.Setup(f => f()).ReturnsAsync(new object());
        this.mockMonitor.Setup(m => m.HasCustomEndpointInfo()).Returns(true);
    }

    public void Dispose()
    {
        this.props.Clear();
        CustomEndpointPlugin.CloseMonitors();
    }

    /// <summary>
    /// Testable plugin that allows injecting a mock monitor instead of creating a real one.
    /// </summary>
    private sealed class TestableCustomEndpointPlugin : CustomEndpointPlugin
    {
        private readonly ICustomEndpointMonitor? injectMonitor;
        public int CreateMonitorIfAbsentCallCount { get; private set; }

        public TestableCustomEndpointPlugin(
            IPluginService pluginService,
            Dictionary<string, string> props,
            Func<RegionEndpoint, AmazonRDSClient> rdsClientFunc,
            ICustomEndpointMonitor? injectMonitor = null)
            : base(pluginService, props, rdsClientFunc)
        {
            this.injectMonitor = injectMonitor;
        }

        protected override ICustomEndpointMonitor CreateMonitorIfAbsent(Dictionary<string, string> props)
        {
            this.CreateMonitorIfAbsentCallCount++;
            if (this.injectMonitor != null)
            {
                var cacheKey = this.customEndpointHostSpec!.Host;
                var options = new MemoryCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(this.idleMonitorExpirationMs),
                    Size = 1,
                };
                options.RegisterPostEvictionCallback(CustomEndpointPlugin.OnMonitorEvicted);
                Monitors.Set(cacheKey, this.injectMonitor, options);
                return this.injectMonitor;
            }

            return base.CreateMonitorIfAbsent(props);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_NonCustomEndpointHost_DoesNotCreateMonitor()
    {
        var plugin = new TestableCustomEndpointPlugin(
            this.mockPluginService.Object,
            this.props,
            _ => new AmazonRDSClient(RegionEndpoint.USEast1),
            this.mockMonitor.Object);

        _ = await plugin.OpenConnection(this.writerClusterHost, this.props, true, this.mockConnectFunc.Object, true);

        Assert.Equal(0, plugin.CreateMonitorIfAbsentCallCount);
        this.mockConnectFunc.Verify(f => f(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_CustomEndpointHost_CreatesMonitor()
    {
        this.props[PropertyDefinition.CustomEndpointRegion.Name] = "us-east-1";
        var plugin = new TestableCustomEndpointPlugin(
            this.mockPluginService.Object,
            this.props,
            _ => new AmazonRDSClient(RegionEndpoint.USEast1),
            this.mockMonitor.Object);

        _ = await plugin.OpenConnection(this.customEndpointHost, this.props, true, this.mockConnectFunc.Object, true);

        Assert.Equal(1, plugin.CreateMonitorIfAbsentCallCount);
        this.mockConnectFunc.Verify(f => f(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_TimeoutWaitingForInfo_Throws()
    {
        this.props[PropertyDefinition.CustomEndpointRegion.Name] = "us-east-1";
        this.props[PropertyDefinition.WaitForCustomEndpointInfoTimeoutMs.Name] = "1";
        this.mockMonitor.Setup(m => m.HasCustomEndpointInfo()).Returns(false);

        var plugin = new TestableCustomEndpointPlugin(
            this.mockPluginService.Object,
            this.props,
            _ => new AmazonRDSClient(RegionEndpoint.USEast1),
            this.mockMonitor.Object);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            plugin.OpenConnection(this.customEndpointHost, this.props, true, this.mockConnectFunc.Object, true));

        Assert.Contains("Timed out waiting", ex.Message);
        Assert.Equal(1, plugin.CreateMonitorIfAbsentCallCount);
        this.mockConnectFunc.Verify(f => f(), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_NoCustomEndpointHost_DoesNotCreateMonitor()
    {
        var plugin = new TestableCustomEndpointPlugin(
            this.mockPluginService.Object,
            this.props,
            _ => new AmazonRDSClient(RegionEndpoint.USEast1),
            this.mockMonitor.Object);

        _ = await plugin.Execute(
            this.mockConnection.Object,
            "Connection.createStatement",
            this.mockJdbcMethodFunc.Object);

        Assert.Equal(0, plugin.CreateMonitorIfAbsentCallCount);
        this.mockJdbcMethodFunc.Verify(f => f(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_CustomEndpointHost_CreatesMonitor()
    {
        this.props[PropertyDefinition.CustomEndpointRegion.Name] = "us-east-1";
        var plugin = new TestableCustomEndpointPlugin(
            this.mockPluginService.Object,
            this.props,
            _ => new AmazonRDSClient(RegionEndpoint.USEast1),
            this.mockMonitor.Object);

        await plugin.OpenConnection(this.customEndpointHost, this.props, true, this.mockConnectFunc.Object, true);
        Assert.Equal(1, plugin.CreateMonitorIfAbsentCallCount);

        _ = await plugin.Execute(
            this.mockConnection.Object,
            "Connection.createStatement",
            this.mockJdbcMethodFunc.Object);

        Assert.True(plugin.CreateMonitorIfAbsentCallCount >= 1);
        this.mockJdbcMethodFunc.Verify(f => f(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CloseMonitors_DisposesRegisteredMonitors()
    {
        this.props[PropertyDefinition.CustomEndpointRegion.Name] = "us-east-1";
        var plugin = new TestableCustomEndpointPlugin(
            this.mockPluginService.Object,
            this.props,
            _ => new AmazonRDSClient(RegionEndpoint.USEast1),
            this.mockMonitor.Object);

        await plugin.OpenConnection(this.customEndpointHost, this.props, true, this.mockConnectFunc.Object, true);

        CustomEndpointPlugin.CloseMonitors();

        this.mockMonitor.Verify(m => m.Dispose(), Times.AtLeastOnce);
    }
}
