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

using System.Data;
using System.Data.Common;
using System.Reflection;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Limitless;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Driver.Dialects;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.Limitless;

public class LimitlessRouterServiceTests : IDisposable
{
    private const string ClusterId = "someClusterId";
    private static readonly HostSpec HostSpec = new HostSpecBuilder()
        .WithHost("some-instance")
        .WithRole(HostRole.Writer)
        .Build();
    private static readonly TimeSpan SomeExpiration = TimeSpan.FromMilliseconds(60000);
    private readonly Dictionary<string, string> props;
    private readonly Mock<IPluginService> mockPluginService;
    private readonly Mock<IHostListProvider> mockHostListProvider;
    private readonly Mock<LimitlessRouterMonitor> mockLimitlessRouterMonitor;
    private readonly Mock<LimitlessQueryHelper> mockQueryHelper;
    private readonly Mock<ADONetDelegate<DbConnection>> mockConnectFuncLambda;
    private readonly Mock<DbConnection> mockConnection;

    public LimitlessRouterServiceTests()
    {
        this.props = new Dictionary<string, string>();
        this.mockPluginService = new Mock<IPluginService>();
        this.mockHostListProvider = new Mock<IHostListProvider>();
        this.mockLimitlessRouterMonitor = new Mock<LimitlessRouterMonitor>(
            Mock.Of<IPluginService>(),
            HostSpec,
            new MemoryCache(new MemoryCacheOptions()),
            "key",
            new Dictionary<string, string>(),
            1000);
        this.mockQueryHelper = new Mock<LimitlessQueryHelper>(Mock.Of<IPluginService>());
        this.mockConnectFuncLambda = new Mock<ADONetDelegate<DbConnection>>();
        this.mockConnection = new Mock<DbConnection>();

        this.mockConnection.SetupGet(x => x.State).Returns(ConnectionState.Open);
        this.mockConnectFuncLambda.Setup(x => x.Invoke()).ReturnsAsync(this.mockConnection.Object);
        this.mockPluginService.Setup(x => x.HostListProvider).Returns(this.mockHostListProvider.Object);
        this.mockHostListProvider.Setup(x => x.GetClusterId()).Returns(ClusterId);
    }

    public void Dispose()
    {
        LimitlessRouterService.ClearCache();
    }

    private static MemoryCache GetLimitlessRouterCache()
    {
        var field = typeof(LimitlessRouterService).GetField(
            "LimitlessRouterCache",
            BindingFlags.NonPublic | BindingFlags.Static);
        return (MemoryCache)field!.GetValue(null)!;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenGetEmptyRouterListAndWaitForRouterInfo_ThenThrow()
    {
        this.mockQueryHelper
            .Setup(x => x.QueryForLimitlessRouters(It.IsAny<DbConnection>(), It.IsAny<int>()))
            .ReturnsAsync(new List<HostSpec>());

        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await Assert.ThrowsAsync<AwsWrapperDbException>(
            () => limitlessRouterService.EstablishConnection(inputContext));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenGetEmptyRouterListAndNoWaitForRouterInfo_ThenCallConnectFunc()
    {
        this.props[PropertyDefinition.LimitlessWaitForRouterInfo.Name] = "false";
        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenHostSpecInRouterCache_ThenCallConnectFunc()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("instance-1").WithRole(HostRole.Writer).WithWeight(-100).Build(),
            new HostSpecBuilder().WithHost("instance-2").WithRole(HostRole.Writer).WithWeight(0).Build(),
            new HostSpecBuilder().WithHost("instance-3").WithRole(HostRole.Writer).WithWeight(100).Build(),
        };
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);

        var inputContext = new LimitlessConnectionContext(
            routerList[1],
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenFetchRouterListAndHostSpecInRouterList_ThenCallConnectFunc()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("some-instance-1").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-2").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-3").WithRole(HostRole.Writer).Build(),
        };

        this.mockQueryHelper
            .Setup(x => x.QueryForLimitlessRouters(It.IsAny<DbConnection>(), It.IsAny<int>()))
            .ReturnsAsync(routerList);

        var inputContext = new LimitlessConnectionContext(
            routerList[1],
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        var cache = GetLimitlessRouterCache();
        Assert.Equal(routerList, cache.Get<IList<HostSpec>>(ClusterId));
        this.mockQueryHelper.Verify(
            x => x.QueryForLimitlessRouters(inputContext.Connection!, inputContext.HostSpec.Port),
            Times.Once);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenRouterCache_ThenSelectsHost()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("instance-1").WithRole(HostRole.Writer).WithWeight(-100).Build(),
            new HostSpecBuilder().WithHost("instance-2").WithRole(HostRole.Writer).WithWeight(0).Build(),
            new HostSpecBuilder().WithHost("instance-3").WithRole(HostRole.Writer).WithWeight(100).Build(),
        };
        var selectedRouter = routerList[2];
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);

        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Returns(selectedRouter);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockConnection.Object);

        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(
                routerList,
                HostRole.Writer,
                WeightedRandomHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.OpenConnection(selectedRouter, inputContext.Props, null, false),
            Times.Once);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenFetchRouterList_ThenSelectsHost()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("some-instance-1").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-2").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-3").WithRole(HostRole.Writer).Build(),
        };
        var selectedRouter = routerList[2];
        this.mockQueryHelper
            .Setup(x => x.QueryForLimitlessRouters(It.IsAny<DbConnection>(), It.IsAny<int>()))
            .ReturnsAsync(routerList);
        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Returns(selectedRouter);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockConnection.Object);

        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        var cache = GetLimitlessRouterCache();
        Assert.Equal(routerList, cache.Get<IList<HostSpec>>(ClusterId));
        this.mockQueryHelper.Verify(
            x => x.QueryForLimitlessRouters(inputContext.Connection!, inputContext.HostSpec.Port),
            Times.Once);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
        this.mockPluginService.Verify(
            x => x.OpenConnection(selectedRouter, inputContext.Props, null, false),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenHostSpecInRouterCacheAndCallConnectFuncThrows_ThenRetry()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("instance-1").WithRole(HostRole.Writer).WithWeight(-100).WithAvailability(HostAvailability.Available).Build(),
            new HostSpecBuilder().WithHost("instance-2").WithRole(HostRole.Writer).WithWeight(0).WithAvailability(HostAvailability.Available).Build(),
            new HostSpecBuilder().WithHost("instance-3").WithRole(HostRole.Writer).WithWeight(100).WithAvailability(HostAvailability.Available).Build(),
        };
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);
        var selectedRouter = routerList[2];
        var inputContext = new LimitlessConnectionContext(
            routerList[1],
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);

        this.mockConnectFuncLambda.Setup(x => x.Invoke()).ThrowsAsync(new MockDbException());
        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Returns(selectedRouter);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockConnection.Object);

        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        Assert.Equal(routerList, cache.Get<IList<HostSpec>>(ClusterId));
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, HighestWeightHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.OpenConnection(selectedRouter, inputContext.Props, null, false),
            Times.Once);
        this.mockConnectFuncLambda.Verify(x => x.Invoke(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenSelectsHostThrows_ThenRetry()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("some-instance-1").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-2").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-3").WithRole(HostRole.Writer).Build(),
        };
        var selectedRouter = routerList[2];
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);
        this.mockPluginService
            .SetupSequence(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Throws(new MockDbException())
            .Returns(selectedRouter);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockConnection.Object);

        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        Assert.Equal(routerList, cache.Get<IList<HostSpec>>(ClusterId));
        this.mockPluginService.Verify(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()), Times.Exactly(2));
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, WeightedRandomHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, HighestWeightHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.OpenConnection(selectedRouter, inputContext.Props, null, false),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenSelectsHostNull_ThenRetry()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("some-instance-1").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-2").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-3").WithRole(HostRole.Writer).Build(),
        };
        var selectedRouter = routerList[2];
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);
        this.mockPluginService
            .SetupSequence(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Returns((HostSpec?)null)
            .Returns(selectedRouter);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockConnection.Object);

        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        Assert.Equal(routerList, cache.Get<IList<HostSpec>>(ClusterId));
        this.mockPluginService.Verify(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()), Times.Exactly(2));
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, WeightedRandomHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, HighestWeightHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.OpenConnection(selectedRouter, inputContext.Props, null, false),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenPluginServiceConnectThrows_ThenRetry()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("some-instance-1").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-2").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("some-instance-3").WithRole(HostRole.Writer).Build(),
        };
        var selectedRouter = routerList[1];
        var selectedRouterForRetry = routerList[2];
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);
        this.mockPluginService
            .SetupSequence(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Returns(selectedRouter)
            .Returns(selectedRouterForRetry);
        this.mockPluginService
            .SetupSequence(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ThrowsAsync(new MockDbException())
            .ReturnsAsync(this.mockConnection.Object);

        var inputContext = new LimitlessConnectionContext(
            HostSpec,
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);
        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await limitlessRouterService.EstablishConnection(inputContext);

        Assert.Equal(this.mockConnection.Object, inputContext.Connection);
        Assert.Equal(routerList, cache.Get<IList<HostSpec>>(ClusterId));
        this.mockPluginService.Verify(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()), Times.Exactly(2));
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, WeightedRandomHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(routerList, HostRole.Writer, HighestWeightHostSelector.StrategyName),
            Times.Once);
        this.mockPluginService.Verify(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()), Times.Exactly(2));
        this.mockPluginService.Verify(x => x.OpenConnection(selectedRouter, inputContext.Props, null, false), Times.Once);
        this.mockPluginService.Verify(x => x.OpenConnection(selectedRouterForRetry, inputContext.Props, null, false), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEstablishConnection_GivenRetryAndMaxRetriesExceeded_thenThrowSqlException()
    {
        var routerList = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("instance-1").WithRole(HostRole.Writer).WithWeight(-100).WithAvailability(HostAvailability.Available).Build(),
            new HostSpecBuilder().WithHost("instance-2").WithRole(HostRole.Writer).WithWeight(0).WithAvailability(HostAvailability.Available).Build(),
            new HostSpecBuilder().WithHost("instance-3").WithRole(HostRole.Writer).WithWeight(100).WithAvailability(HostAvailability.Available).Build(),
        };
        var cache = GetLimitlessRouterCache();
        cache.Set(ClusterId, routerList, SomeExpiration);

        var inputContext = new LimitlessConnectionContext(
            routerList[0],
            this.props,
            null,
            this.mockConnectFuncLambda.Object,
            null,
            null);

        this.mockConnectFuncLambda.Setup(x => x.Invoke()).ThrowsAsync(new MockDbException());
        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()))
            .Returns(routerList[0]);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ThrowsAsync(new MockDbException());

        var limitlessRouterService = new LimitlessRouterService(
            this.mockPluginService.Object,
            this.mockQueryHelper.Object,
            (a, b, c, d, e) => this.mockLimitlessRouterMonitor.Object);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => limitlessRouterService.EstablishConnection(inputContext));

        var maxRetries = PropertyDefinition.LimitlessMaxRetries.GetInt(this.props) ?? 5;
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()),
            Times.Exactly(maxRetries));
        this.mockPluginService.Verify(
            x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), It.IsAny<HostRole>(), It.IsAny<string>()),
            Times.Exactly(maxRetries));
    }
}
