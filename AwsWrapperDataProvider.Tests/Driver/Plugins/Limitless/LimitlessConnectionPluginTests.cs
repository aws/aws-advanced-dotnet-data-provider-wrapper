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

using System;
using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Limitless;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.Limitless;

public class LimitlessConnectionPluginTests
{
    private const string ClusterId = "someClusterId";
    private static readonly HostSpec InputHostSpec = new HostSpecBuilder()
        .WithHost("pg.testdb.us-east-2.rds.amazonaws.com")
        .Build();

    private static readonly HostSpec ExpectedSelectedHostSpec = new HostSpecBuilder()
        .WithHost("expected-selected-instance")
        .WithRole(HostRole.Writer)
        .WithWeight(long.MaxValue)
        .Build();
    private static readonly IDialect SupportedDialect = new AuroraPgDialect();
    private static readonly IDialect UnsupportedDialect = new PgDialect();

    private readonly Mock<ADONetDelegate<DbConnection>> mockConnectFuncLambda;
    private readonly Mock<DbConnection> mockConnection;
    private readonly Mock<IPluginService> mockPluginService;
    private readonly Mock<IHostListProvider> mockHostListProvider;
    private readonly Mock<ILimitlessRouterService> mockLimitlessRouterService;
    private readonly Dictionary<string, string> props;
    private readonly LimitlessConnectionPlugin plugin;

    public LimitlessConnectionPluginTests()
    {
        this.mockConnectFuncLambda = new Mock<ADONetDelegate<DbConnection>>();
        this.mockConnection = new Mock<DbConnection>();
        this.mockPluginService = new Mock<IPluginService>();
        this.mockHostListProvider = new Mock<IHostListProvider>();
        this.mockLimitlessRouterService = new Mock<ILimitlessRouterService>();
        this.props = new Dictionary<string, string>();

        this.mockPluginService.Setup(x => x.HostListProvider).Returns(this.mockHostListProvider.Object);
        this.mockPluginService.Setup(x => x.Dialect).Returns(SupportedDialect);
        this.mockHostListProvider.Setup(x => x.GetClusterId()).Returns(ClusterId);
        this.mockConnectFuncLambda.Setup(x => x.Invoke()).ReturnsAsync(this.mockConnection.Object);

        this.plugin = new LimitlessConnectionPlugin(
            this.mockPluginService.Object,
            this.props,
            () => this.mockLimitlessRouterService.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnect()
    {
        this.mockLimitlessRouterService
            .Setup(x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()))
            .Callback<LimitlessConnectionContext>(context => context.Connection = this.mockConnection.Object);

        var expectedConnection = this.mockConnection.Object;
        var actualConnection = await this.plugin.OpenConnection(
            InputHostSpec,
            this.props,
            true,
            this.mockConnectFuncLambda.Object,
            true);

        Assert.Equal(expectedConnection, actualConnection);
        this.mockPluginService.Verify(x => x.Dialect, Times.Once);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Never);
        this.mockLimitlessRouterService.Verify(
            x => x.StartMonitoring(
                InputHostSpec,
                this.props,
                PropertyDefinition.LimitlessIntervalMs.GetInt(this.props) ?? 7500),
            Times.Once);
        this.mockLimitlessRouterService.Verify(
            x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectGivenNullConnection()
    {
        this.mockLimitlessRouterService
            .Setup(x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()))
            .Callback<LimitlessConnectionContext>(context => context.Connection = null);

        await Assert.ThrowsAsync<AwsWrapperDbException>(
            () => this.plugin.OpenConnection(
                InputHostSpec,
                this.props,
                true,
                this.mockConnectFuncLambda.Object,
                true));

        this.mockPluginService.Verify(x => x.Dialect, Times.Once);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Never);
        this.mockLimitlessRouterService.Verify(
            x => x.StartMonitoring(
                InputHostSpec,
                this.props,
                PropertyDefinition.LimitlessIntervalMs.GetInt(this.props) ?? 7500),
            Times.Once);
        this.mockLimitlessRouterService.Verify(
            x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectGivenUnsupportedDialect()
    {
        this.mockPluginService.SetupSequence(x => x.Dialect)
            .Returns(UnsupportedDialect)
            .Returns(UnsupportedDialect);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => this.plugin.OpenConnection(
                InputHostSpec,
                this.props,
                true,
                this.mockConnectFuncLambda.Object,
                true));

        this.mockPluginService.Verify(x => x.Dialect, Times.Exactly(2));
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
        this.mockLimitlessRouterService.Verify(
            x => x.StartMonitoring(
                It.IsAny<HostSpec>(),
                It.IsAny<Dictionary<string, string>>(),
                It.IsAny<int>()),
            Times.Never);
        this.mockLimitlessRouterService.Verify(
            x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectGivenSupportedDialectAfterRefresh()
    {
        this.mockPluginService.SetupSequence(x => x.Dialect)
            .Returns(UnsupportedDialect)
            .Returns(SupportedDialect);
        this.mockLimitlessRouterService
            .Setup(x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()))
            .Callback<LimitlessConnectionContext>(context => context.Connection = this.mockConnection.Object);

        var expectedConnection = this.mockConnection.Object;
        var actualConnection = await this.plugin.OpenConnection(
            InputHostSpec,
            this.props,
            true,
            this.mockConnectFuncLambda.Object,
            true);

        Assert.Equal(expectedConnection, actualConnection);
        this.mockPluginService.Verify(x => x.Dialect, Times.Exactly(2));
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
        this.mockLimitlessRouterService.Verify(
            x => x.StartMonitoring(
                InputHostSpec,
                this.props,
                PropertyDefinition.LimitlessIntervalMs.GetInt(this.props) ?? 7500),
            Times.Once);
        this.mockLimitlessRouterService.Verify(
            x => x.EstablishConnection(It.IsAny<LimitlessConnectionContext>()),
            Times.Once);
    }
}
