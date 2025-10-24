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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.AuroraInitialConnectionStrategy;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class AuroraInitialConnectionStrategyPluginTests
{
    private readonly Mock<IPluginService> mockPluginService;
    private readonly Mock<IHostListProviderService> mockHostListProviderService;
    private readonly Mock<DbConnection> mockConnection;
    private readonly Dictionary<string, string> defaultProps;
    private readonly AuroraInitialConnectionStrategyPlugin plugin;

    public AuroraInitialConnectionStrategyPluginTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.mockHostListProviderService = new Mock<IHostListProviderService>();
        this.mockConnection = new Mock<DbConnection>();

        this.defaultProps = new Dictionary<string, string>
        {
            { PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, "30000" },
            { PropertyDefinition.OpenConnectionRetryIntervalMs.Name, "1000" },
            { PropertyDefinition.ReaderHostSelectorStrategy.Name, "random" },
        };

        this.plugin = new AuroraInitialConnectionStrategyPlugin(this.mockPluginService.Object, this.defaultProps);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithVerifyOpenedConnectionType_SetsProperty()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.VerifyOpenedConnectionType.Name, "writer" },
        };

        var pluginWithVerify = new AuroraInitialConnectionStrategyPlugin(this.mockPluginService.Object, props);
        Assert.NotNull(pluginWithVerify);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task InitHostProvider_WithStaticProvider_ThrowsException()
    {
        this.mockHostListProviderService.Setup(x => x.IsStaticHostListProvider()).Returns(true);
        var initFunc = new Mock<ADONetDelegate>();

        var exception = await Assert.ThrowsAsync<Exception>(() =>
            this.plugin.InitHostProvider("test-url", this.defaultProps, this.mockHostListProviderService.Object, initFunc.Object));

        Assert.Equal("AuroraInitialConnectionStrategyPlugin requires dynamic provider.", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task InitHostProvider_WithDynamicProvider_CallsInitFunction()
    {
        this.mockHostListProviderService.Setup(x => x.IsStaticHostListProvider()).Returns(false);
        var initFunc = new Mock<ADONetDelegate>();

        await this.plugin.InitHostProvider("test-url", this.defaultProps, this.mockHostListProviderService.Object, initFunc.Object);

        initFunc.Verify(x => x.Invoke(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithRdsWriterCluster_CallsGetVerifiedWriterConnection()
    {
        var hostSpec = new HostSpec("test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var writerHost = new HostSpec("writer-host", 5432, null, HostRole.Writer, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([writerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.GetHostRole(It.IsAny<DbConnection>())).ReturnsAsync(HostRole.Writer);

        await this.plugin.InitHostProvider("test-url", this.defaultProps, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await this.plugin.OpenConnection(hostSpec, this.defaultProps, false, methodFunc.Object, true);

        this.mockPluginService.Verify(x => x.ForceOpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), null, true), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithRdsReaderCluster_CallsGetVerifiedReaderConnection()
    {
        // Arrange
        var hostSpec = new HostSpec("test-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Reader, HostAvailability.Available);
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var readerHost = new HostSpec("reader-host", 5432, null, HostRole.Reader, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([readerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.GetHostRole(It.IsAny<DbConnection>())).ReturnsAsync(HostRole.Reader);
        this.mockPluginService.Setup(x => x.AcceptsStrategy("random")).Returns(true);
        this.mockPluginService.Setup(x => x.GetHostSpecByStrategy(HostRole.Reader, "random")).Returns(readerHost);

        await this.plugin.InitHostProvider("test-url", this.defaultProps, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await this.plugin.OpenConnection(hostSpec, this.defaultProps, false, methodFunc.Object, true);

        this.mockPluginService.Verify(x => x.ForceOpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), null, true), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithNonClusterHost_CallsMethodFunc()
    {
        var hostSpec = new HostSpec("regular-host.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        await this.plugin.OpenConnection(hostSpec, this.defaultProps, false, methodFunc.Object, true);
        methodFunc.Verify(x => x.Invoke(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithVerifyWriterType_CallsGetVerifiedWriterConnection()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.VerifyOpenedConnectionType.Name, "writer" },
            { PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, "30000" },
            { PropertyDefinition.OpenConnectionRetryIntervalMs.Name, "1000" },
            { PropertyDefinition.ReaderHostSelectorStrategy.Name, "random" },
        };
        var pluginWithVerify = new AuroraInitialConnectionStrategyPlugin(this.mockPluginService.Object, props);
        var hostSpec = new HostSpec("regular-host.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var writerHost = new HostSpec("writer-host", 5432, null, HostRole.Writer, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([writerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.GetHostRole(It.IsAny<DbConnection>())).ReturnsAsync(HostRole.Writer);

        await pluginWithVerify.InitHostProvider("test-url", props, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await pluginWithVerify.OpenConnection(hostSpec, props, true, methodFunc.Object, true);

        this.mockPluginService.Verify(x => x.ForceOpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), null, true), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithVerifyReaderType_CallsGetVerifiedReaderConnection()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.VerifyOpenedConnectionType.Name, "reader" },
            { PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, "30000" },
            { PropertyDefinition.OpenConnectionRetryIntervalMs.Name, "1000" },
            { PropertyDefinition.ReaderHostSelectorStrategy.Name, "random" },
        };
        var pluginWithVerify = new AuroraInitialConnectionStrategyPlugin(this.mockPluginService.Object, props);
        var hostSpec = new HostSpec("regular-host.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Reader, HostAvailability.Available);
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var readerHost = new HostSpec("reader-host", 5432, null, HostRole.Reader, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([readerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.GetHostRole(It.IsAny<DbConnection>())).ReturnsAsync(HostRole.Reader);
        this.mockPluginService.Setup(x => x.AcceptsStrategy("random")).Returns(true);
        this.mockPluginService.Setup(x => x.GetHostSpecByStrategy(HostRole.Reader, "random")).Returns(readerHost);

        await pluginWithVerify.InitHostProvider("test-url", props, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await pluginWithVerify.OpenConnection(hostSpec, props, true, methodFunc.Object, true);

        this.mockPluginService.Verify(x => x.GetHostSpecByStrategy(HostRole.Reader, "random"), Times.Once);
        this.mockPluginService.Verify(x => x.ForceOpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), null, true), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetVerifiedWriterConnection_WithClusterDns_RefreshesHostList()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, "5000" },
            { PropertyDefinition.OpenConnectionRetryIntervalMs.Name, "100" },
            { PropertyDefinition.ReaderHostSelectorStrategy.Name, "random" },
        };
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var writerHost = new HostSpec("test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);
        var actualWriterHost = new HostSpec("writer-instance.xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([writerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.IdentifyConnectionAsync(It.IsAny<DbConnection>(), It.IsAny<DbTransaction>())).ReturnsAsync(actualWriterHost);
        this.mockHostListProviderService.Setup(x => x.IsStaticHostListProvider()).Returns(false);

        await this.plugin.InitHostProvider("test-url", props, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await this.plugin.OpenConnection(writerHost, props, true, methodFunc.Object, true);

        this.mockPluginService.Verify(x => x.ForceRefreshHostList(It.IsAny<DbConnection>()), Times.Once);
        this.mockPluginService.Verify(x => x.IdentifyConnectionAsync(It.IsAny<DbConnection>(), It.IsAny<DbTransaction>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetVerifiedReaderConnection_WithNoReaders_AcceptsWriterConnection()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, "5000" },
            { PropertyDefinition.OpenConnectionRetryIntervalMs.Name, "100" },
            { PropertyDefinition.ReaderHostSelectorStrategy.Name, "random" },
        };
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var readerHost = new HostSpec("test-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Reader, HostAvailability.Available);
        var writerHost = new HostSpec("writer-instance.xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);

        // Setup scenario where only writer exists (no readers)
        this.mockPluginService.Setup(x => x.AllHosts).Returns([writerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.IdentifyConnectionAsync(It.IsAny<DbConnection>(), It.IsAny<DbTransaction>())).ReturnsAsync(writerHost);
        this.mockPluginService.Setup(x => x.AcceptsStrategy("random")).Returns(true);
        this.mockPluginService.Setup(x => x.GetHostSpecByStrategy(HostRole.Reader, "random")).Returns((HostSpec?)null!);
        this.mockHostListProviderService.Setup(x => x.IsStaticHostListProvider()).Returns(false);

        await this.plugin.InitHostProvider("test-url", props, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await this.plugin.OpenConnection(readerHost, props, true, methodFunc.Object, true);

        this.mockPluginService.Verify(x => x.ForceRefreshHostList(It.IsAny<DbConnection>()), Times.Once);
        this.mockHostListProviderService.VerifySet(x => x.InitialConnectionHostSpec = It.IsAny<HostSpec>(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetVerifiedReaderConnection_WithInvalidStrategy_ThrowsException()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.ReaderHostSelectorStrategy.Name, "invalid-strategy" },
            { PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, "5000" },
            { PropertyDefinition.OpenConnectionRetryIntervalMs.Name, "100" },
        };
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var readerHost = new HostSpec("test-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Reader, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([readerHost]);
        this.mockPluginService.Setup(x => x.AcceptsStrategy("invalid-strategy")).Returns(false);
        this.mockHostListProviderService.Setup(x => x.IsStaticHostListProvider()).Returns(false);

        await this.plugin.InitHostProvider("test-url", props, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            this.plugin.OpenConnection(readerHost, props, false, methodFunc.Object, true));

        Assert.Equal("Invalid host selection strategy: invalid-strategy", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithInitialConnection_SetsInitialConnectionHostSpec()
    {
        var hostSpec = new HostSpec("test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, null, HostRole.Writer, HostAvailability.Available);
        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        var writerHost = new HostSpec("writer-host", 5432, null, HostRole.Writer, HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns([writerHost]);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.GetHostRole(It.IsAny<DbConnection>())).ReturnsAsync(HostRole.Writer);
        this.mockHostListProviderService.Setup(x => x.IsStaticHostListProvider()).Returns(false);

        await this.plugin.InitHostProvider("test-url", this.defaultProps, this.mockHostListProviderService.Object, () => Task.CompletedTask);

        await this.plugin.OpenConnection(hostSpec, this.defaultProps, true, methodFunc.Object, true);

        this.mockHostListProviderService.VerifySet(x => x.InitialConnectionHostSpec = It.IsAny<HostSpec>(), Times.Once);
    }
}
