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

using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// Holds the services that make up a single wrapper connection's object graph, so that
/// infrastructure such as host list providers and shared monitors can resolve services from one
/// place instead of threading each service through every constructor.
/// </summary>
/// <remarks>
/// Process-wide concerns are intentionally not slots here: keyed caches use
/// <c>MemoryCache</c>, background monitors manage their own <c>Task</c>/
/// <c>CancellationTokenSource</c> lifecycles, and configuration profiles have their own cache.
/// <para>
/// This type is public only because it appears in the public <see cref="Dialects.HostListProviderSupplier"/>
/// signature so custom dialects can build host list providers. Its slots are read-only to callers;
/// only the wrapper's connection bootstrap populates them.
/// </para>
/// </remarks>
public class FullServicesContainer
{
    private ConnectionPluginManager? connectionPluginManager;
    private IPluginService? pluginService;
    private IHostListProviderService? hostListProviderService;
    private ITelemetryFactory? telemetryFactory;

    internal FullServicesContainer(
        IConnectionProvider defaultConnectionProvider,
        IHostIdCacheService hostIdCacheService,
        ConfigurationProfile? configurationProfile)
    {
        this.DefaultConnectionProvider = defaultConnectionProvider ?? throw new ArgumentNullException(nameof(defaultConnectionProvider));
        this.HostIdCacheService = hostIdCacheService ?? throw new ArgumentNullException(nameof(hostIdCacheService));
        this.ConfigurationProfile = configurationProfile;
    }

    public IConnectionProvider DefaultConnectionProvider { get; }

    public IHostIdCacheService HostIdCacheService { get; }

    public ConfigurationProfile? ConfigurationProfile { get; }

    // The slots below are late-bound because the connection bootstrap creates their services in
    // stages that reference each other (the plugin manager and plugin service must each see the
    // other). Reading a slot before bootstrap assigns it indicates a wrapper bug, so the getters
    // throw rather than return null.

    public ConnectionPluginManager ConnectionPluginManager
    {
        get => this.connectionPluginManager ?? throw UninitializedSlot(nameof(this.ConnectionPluginManager));
        internal set => this.connectionPluginManager = value;
    }

    public IPluginService PluginService
    {
        get => this.pluginService ?? throw UninitializedSlot(nameof(this.PluginService));
        internal set => this.pluginService = value;
    }

    public IHostListProviderService HostListProviderService
    {
        get => this.hostListProviderService ?? throw UninitializedSlot(nameof(this.HostListProviderService));
        internal set => this.hostListProviderService = value;
    }

    public ITelemetryFactory TelemetryFactory
    {
        get => this.telemetryFactory ?? throw UninitializedSlot(nameof(this.TelemetryFactory));
        internal set => this.telemetryFactory = value;
    }

    private static InvalidOperationException UninitializedSlot(string slotName)
    {
        return new InvalidOperationException(
            string.Format(Resources.Error_FullServicesContainerSlotNotInitialized, slotName));
    }
}
