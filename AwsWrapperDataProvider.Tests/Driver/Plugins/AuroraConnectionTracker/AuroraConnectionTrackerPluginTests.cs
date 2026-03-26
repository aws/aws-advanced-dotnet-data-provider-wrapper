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
using System.Reflection;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.AuroraConnectionTracker;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests.Driver;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.AuroraConnectionTracker;

public class AuroraConnectionTrackerPluginTests
{
    private readonly Mock<IPluginService> mockPluginService;
    private readonly Mock<IConnectionTracker> mockTracker;
    private readonly Mock<DbConnection> mockConnection;
    private readonly Dictionary<string, string> props;
    private readonly HostSpec writerHostSpec;
    private readonly AuroraConnectionTrackerPlugin plugin;

    public AuroraConnectionTrackerPluginTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.mockTracker = new Mock<IConnectionTracker>();
        this.mockConnection = new Mock<DbConnection>();
        this.props = new Dictionary<string, string>();

        this.writerHostSpec = new HostSpec(
            "writer-instance.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.mockPluginService.Setup(x => x.CurrentHostSpec).Returns(this.writerHostSpec);
        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.AllHosts).Returns(new List<HostSpec> { this.writerHostSpec });

        this.plugin = new AuroraConnectionTrackerPlugin(
            this.mockPluginService.Object,
            this.props,
            this.mockTracker.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithRdsInstanceHost_CallsPopulateAndSkipsFillAliases()
    {
        var hostSpec = new HostSpec(
            "instance1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        var methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        methodFunc.Setup(x => x.Invoke()).ReturnsAsync(this.mockConnection.Object);

        var conn = await this.plugin.OpenConnection(
            hostSpec,
            this.props,
            false,
            methodFunc.Object,
            true);

        Assert.Equal(this.mockConnection.Object, conn);
        this.mockPluginService.Verify(
            x => x.FillAliasesAsync(It.IsAny<DbConnection>(), It.IsAny<HostSpec>(), It.IsAny<DbTransaction>()),
            Times.Never);
        this.mockTracker.Verify(
            x => x.PopulateOpenedConnectionQueue(hostSpec, this.mockConnection.Object),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithClusterHost_CallsFillAliasesAndPopulate()
    {
        var hostSpec = new HostSpec(
            "test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        var mockMethodFunc = new Mock<ADONetDelegate<DbConnection>>();
        mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync(this.mockConnection.Object);

        var conn = await this.plugin.OpenConnection(
            hostSpec,
            this.props,
            false,
            mockMethodFunc.Object,
            true);

        Assert.Equal(this.mockConnection.Object, conn);
        this.mockPluginService.Verify(
            x => x.FillAliasesAsync(this.mockConnection.Object, hostSpec, null),
            Times.Once);
        this.mockTracker.Verify(
            x => x.PopulateOpenedConnectionQueue(hostSpec, this.mockConnection.Object),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_ReturnsNullConnection_SkipsTracking()
    {
        var hostSpec = new HostSpec(
            "test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        var mockMethodFunc = new Mock<ADONetDelegate<DbConnection>>();
        mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync((DbConnection)null!);

        var conn = await this.plugin.OpenConnection(
            hostSpec,
            this.props,
            false,
            mockMethodFunc.Object,
            true);

        Assert.Null(conn);
        this.mockPluginService.Verify(
            x => x.FillAliasesAsync(It.IsAny<DbConnection>(), It.IsAny<HostSpec>(), It.IsAny<DbTransaction>()),
            Times.Never);
        this.mockTracker.Verify(
            x => x.PopulateOpenedConnectionQueue(It.IsAny<HostSpec>(), It.IsAny<DbConnection>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_FailoverException_WriterNotChanged_DoesNotInvalidate()
    {
        try
        {
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                0L);

            var originalHost = new HostSpec(
                "host.xyz.us-east-1.rds.amazonaws.com",
                5432,
                HostRole.Writer,
                HostAvailability.Available);

            this.mockPluginService.Setup(x => x.AllHosts).Returns(new List<HostSpec> { originalHost });

            var mockMethodFunc = new Mock<ADONetDelegate<object>>();
            mockMethodFunc.Setup(x => x.Invoke()).ThrowsAsync(new FailoverSuccessException());

            var ex = await Assert.ThrowsAsync<FailoverSuccessException>(() =>
                this.plugin.Execute(
                    new object(),
                    "DbCommand.ExecuteNonQuery",
                    mockMethodFunc.Object));

            Assert.NotNull(ex);
            this.mockTracker.Verify(
                x => x.InvalidateAllConnections(It.IsAny<HostSpec>()),
                Times.Never);
            this.mockTracker.Verify(
                x => x.RemoveConnectionTracking(It.IsAny<HostSpec>(), It.IsAny<DbConnection>()),
                Times.Never);
        }
        finally
        {
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                0L);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_FailoverException_WriterChanged_InvalidatesOldWriter()
    {
        try
        {
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                0L);

            var originalHost = new HostSpec(
                "original-host.xyz.us-east-1.rds.amazonaws.com",
                5432,
                HostRole.Writer,
                HostAvailability.Available);

            var newHost = new HostSpec(
                "new-host.xyz.us-east-1.rds.amazonaws.com",
                5432,
                HostRole.Writer,
                HostAvailability.Available);

            this.mockPluginService.SetupSequence(x => x.AllHosts)
                .Returns(new List<HostSpec> { originalHost }) // RememberWriter on first Execute
                .Returns(new List<HostSpec> { newHost });        // CheckWriterChangedAsync after failover

            var mockSuccessFunc = new Mock<ADONetDelegate<object>>();
            mockSuccessFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

            var mockFailoverFunc = new Mock<ADONetDelegate<object>>();
            mockFailoverFunc.Setup(x => x.Invoke()).ThrowsAsync(new FailoverSuccessException());

            // First call succeeds — sets currentWriter to originalHost
            await this.plugin.Execute(
                new object(),
                "DbCommand.ExecuteNonQuery",
                mockSuccessFunc.Object);

            // Second call throws FailoverSuccessException — triggers writer change detection
            var ex = await Assert.ThrowsAsync<FailoverSuccessException>(() =>
                this.plugin.Execute(
                    new object(),
                    "DbCommand.ExecuteNonQuery",
                    mockFailoverFunc.Object));

            Assert.NotNull(ex);
            this.mockTracker.Verify(
                x => x.InvalidateAllConnections(originalHost),
                Times.Once);
            this.mockTracker.Verify(
                x => x.RemoveConnectionTracking(It.IsAny<HostSpec>(), It.IsAny<DbConnection>()),
                Times.Never);
        }
        finally
        {
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                0L);
        }
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbConnection.Close")]
    [InlineData("DbConnection.CloseAsync")]
    [InlineData("DbConnection.Dispose")]
    public async Task Execute_CloseMethod_CallsRemoveConnectionTracking(string methodName)
    {
        var mockMethodFunc = new Mock<ADONetDelegate<object>>();
        mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

        await this.plugin.Execute(
            new object(),
            methodName,
            mockMethodFunc.Object);

        this.mockTracker.Verify(
            x => x.RemoveConnectionTracking(this.writerHostSpec, this.mockConnection.Object),
            Times.Once);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbConnection.Close")]
    [InlineData("DbConnection.CloseAsync")]
    [InlineData("DbConnection.Dispose")]
    public async Task Execute_CloseMethod_SkipsWriterChangeCheck(string methodName)
    {
        var mockMethodFunc = new Mock<ADONetDelegate<object>>();
        mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

        await this.plugin.Execute(
            new object(),
            methodName,
            mockMethodFunc.Object);

        this.mockPluginService.Verify(
            x => x.RefreshHostListAsync(),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_NonCloseMethod_RemembersWriter()
    {
        var writer = new HostSpec(
            "writer.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.mockPluginService.Setup(x => x.AllHosts).Returns(new List<HostSpec> { writer });

        var mockMethodFunc = new Mock<ADONetDelegate<object>>();
        mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

        await this.plugin.Execute(
            new object(),
            "DbCommand.ExecuteNonQuery",
            mockMethodFunc.Object);

        var currentWriter = TestUtils.GetNonPublicInstanceField<HostSpec>(this.plugin, "currentWriter");
        Assert.NotNull(currentWriter);
        Assert.Equal(writer.GetHostAndPort(), currentWriter.GetHostAndPort());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_WithinRefreshWindow_CallsRefreshHostList()
    {
        try
        {
            // Set refresh deadline to 5 minutes in the future
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                DateTime.UtcNow.Ticks + TimeSpan.FromMinutes(3).Ticks);

            var mockMethodFunc = new Mock<ADONetDelegate<object>>();
            mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

            await this.plugin.Execute(
                new object(),
                "DbCommand.ExecuteNonQuery",
                mockMethodFunc.Object);

            this.mockPluginService.Verify(
                x => x.RefreshHostListAsync(),
                Times.Once);
        }
        finally
        {
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                0L);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_RefreshWindowExpired_StopsRefreshing()
    {
        try
        {
            // Set refresh deadline to 1 minute in the past
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                DateTime.UtcNow.Ticks - TimeSpan.FromMinutes(1).Ticks);

            var mockMethodFunc = new Mock<ADONetDelegate<object>>();
            mockMethodFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

            await this.plugin.Execute(
                new object(),
                "DbCommand.ExecuteNonQuery",
                mockMethodFunc.Object);

            this.mockPluginService.Verify(
                x => x.RefreshHostListAsync(),
                Times.Never);

            var field = typeof(AuroraConnectionTrackerPlugin)
                .GetField("hostListRefreshEndTimeTicks", BindingFlags.Static | BindingFlags.NonPublic);
            Assert.NotNull(field);
            Assert.Equal(0L, (long)field.GetValue(null)!);
        }
        finally
        {
            TestUtils.SetNonPublicStaticField(
                typeof(AuroraConnectionTrackerPlugin),
                "hostListRefreshEndTimeTicks",
                0L);
        }
    }
}
