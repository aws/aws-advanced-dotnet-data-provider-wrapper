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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver;

public class PartialPluginServiceTests
{
    private readonly Mock<IDialect> mockDialect = new();
    private readonly Mock<IHostListProvider> mockHostListProvider = new();
    private readonly Mock<ITargetConnectionDialect> mockTargetDialect = new();
    private readonly IConnectionProvider mockConnectionProvider = Mock.Of<IConnectionProvider>();
    private readonly IHostIdCacheService mockHostIdCacheService = Mock.Of<IHostIdCacheService>();

    public PartialPluginServiceTests()
    {
        this.mockDialect
            .Setup(d => d.HostListProviderSupplier)
            .Returns((props, servicesContainer) => this.mockHostListProvider.Object);
        this.mockTargetDialect
            .Setup(d => d.GetPluginCodesOrDefault(It.IsAny<Dictionary<string, string>>()))
            .Returns("executionTime");
    }

    private PartialPluginService CreatePartialPluginService(FullServicesContainer? container = null)
    {
        container ??= new FullServicesContainer(this.mockConnectionProvider, this.mockHostIdCacheService, null)
        {
            ConnectionPluginManager = new ConnectionPluginManager(this.mockConnectionProvider, null),
            TelemetryFactory = NullTelemetryFactory.Instance,
        };

        return new PartialPluginService(container, [], this.mockDialect.Object, this.mockTargetDialect.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestRegistersItselfInContainer()
    {
        FullServicesContainer container = new(this.mockConnectionProvider, this.mockHostIdCacheService, null)
        {
            ConnectionPluginManager = new ConnectionPluginManager(this.mockConnectionProvider, null),
            TelemetryFactory = NullTelemetryFactory.Instance,
        };

        PartialPluginService partialPluginService = this.CreatePartialPluginService(container);

        Assert.Same(partialPluginService, container.PluginService);
        Assert.Same(partialPluginService, container.HostListProviderService);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestDialectIsConfirmedSnapshot()
    {
        PartialPluginService partialPluginService = this.CreatePartialPluginService();

        Assert.True(partialPluginService.IsDialectConfirmed);
        Assert.Same(this.mockDialect.Object, partialPluginService.Dialect);
        Assert.Same(this.mockTargetDialect.Object, partialPluginService.TargetConnectionDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConnectionScopedMembersThrowOrReturnNull()
    {
        PartialPluginService partialPluginService = this.CreatePartialPluginService();

        Assert.Null(partialPluginService.CurrentConnection);
        Assert.Null(partialPluginService.CurrentTransaction);
        Assert.Null(partialPluginService.RoutedHostSpec);
        Assert.Throws<NotSupportedException>(() => partialPluginService.SetCurrentConnection(null, null));
        Assert.Throws<NotSupportedException>(() => partialPluginService.CurrentTransaction = null);
        Assert.Throws<NotSupportedException>(() => partialPluginService.RoutedHostSpec = null);
        Assert.Throws<NotSupportedException>(
            () => partialPluginService.GetHostSpecByStrategy(HostRole.Writer, "random"));
        Assert.Throws<NotSupportedException>(
            () => partialPluginService.GetHostSpecByStrategy([], HostRole.Writer, "random"));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => partialPluginService.UpdateDialectAsync(null!));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestRefreshHostListUpdatesAllHosts()
    {
        HostSpec host = new HostSpecBuilder().WithHost("instance-1").Build();
        this.mockHostListProvider.Setup(p => p.RefreshAsync()).ReturnsAsync([host]);

        PartialPluginService partialPluginService = this.CreatePartialPluginService();
        await partialPluginService.RefreshHostListAsync();

        Assert.Single(partialPluginService.AllHosts);
        Assert.Same(host, partialPluginService.AllHosts[0]);
        Assert.Same(host, partialPluginService.GetHosts()[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestForceRefreshHostListReturnsFalseOnTimeout()
    {
        this.mockHostListProvider
            .Setup(p => p.ForceRefreshAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ThrowsAsync(new TimeoutException());

        PartialPluginService partialPluginService = this.CreatePartialPluginService();

        Assert.False(await partialPluginService.ForceRefreshHostListAsync(true, 100));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestCreateMinimalContainerBuildsIndependentGraph()
    {
        FullServicesContainer source = new(this.mockConnectionProvider, this.mockHostIdCacheService, null)
        {
            ConnectionPluginManager = new ConnectionPluginManager(this.mockConnectionProvider, null),
            TelemetryFactory = NullTelemetryFactory.Instance,
        };
        PluginService fullPluginService = new();
        source.PluginService = fullPluginService;

        FullServicesContainer minimal = ServiceUtility.CreateMinimalContainer(
            source,
            [],
            this.mockDialect.Object,
            this.mockTargetDialect.Object);

        // The minimal container shares the universal slots but must NOT share the per-connection ones.
        Assert.Same(source.DefaultConnectionProvider, minimal.DefaultConnectionProvider);
        Assert.Same(source.HostIdCacheService, minimal.HostIdCacheService);
        Assert.Same(source.TelemetryFactory, minimal.TelemetryFactory);
        Assert.NotSame(source.ConnectionPluginManager, minimal.ConnectionPluginManager);
        Assert.NotSame(source.PluginService, minimal.PluginService);
        Assert.IsType<PartialPluginService>(minimal.PluginService);
    }
}
