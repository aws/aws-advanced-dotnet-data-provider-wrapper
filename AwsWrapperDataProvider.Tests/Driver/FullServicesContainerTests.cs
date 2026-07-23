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
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver;

public class FullServicesContainerTests
{
    private readonly IConnectionProvider mockConnectionProvider = Mock.Of<IConnectionProvider>();
    private readonly IHostIdCacheService mockHostIdCacheService = Mock.Of<IHostIdCacheService>();

    private FullServicesContainer CreateContainer()
    {
        return new FullServicesContainer(this.mockConnectionProvider, this.mockHostIdCacheService, null);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestConstructorInitializedSlots()
    {
        FullServicesContainer container = this.CreateContainer();

        Assert.Same(this.mockConnectionProvider, container.DefaultConnectionProvider);
        Assert.Same(this.mockHostIdCacheService, container.HostIdCacheService);
        Assert.Null(container.ConfigurationProfile);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestNullRequiredSlotsThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new FullServicesContainer(null!, this.mockHostIdCacheService, null));
        Assert.Throws<ArgumentNullException>(
            () => new FullServicesContainer(this.mockConnectionProvider, null!, null));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestLateBoundSlotsThrowBeforeAssignment()
    {
        FullServicesContainer container = this.CreateContainer();

        Assert.Throws<InvalidOperationException>(() => container.ConnectionPluginManager);
        Assert.Throws<InvalidOperationException>(() => container.PluginService);
        Assert.Throws<InvalidOperationException>(() => container.HostListProviderService);
        Assert.Throws<InvalidOperationException>(() => container.TelemetryFactory);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestLateBoundSlotsReturnAssignedInstances()
    {
        FullServicesContainer container = this.CreateContainer();
        IPluginService pluginService = Mock.Of<IPluginService>();
        IHostListProviderService hostListProviderService = Mock.Of<IHostListProviderService>();
        ITelemetryFactory telemetryFactory = Mock.Of<ITelemetryFactory>();

        container.PluginService = pluginService;
        container.HostListProviderService = hostListProviderService;
        container.TelemetryFactory = telemetryFactory;

        Assert.Same(pluginService, container.PluginService);
        Assert.Same(hostListProviderService, container.HostListProviderService);
        Assert.Same(telemetryFactory, container.TelemetryFactory);
    }
}
