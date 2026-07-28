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

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// Builds <see cref="FullServicesContainer"/> instances. The container itself is a plain holder;
/// the assembly sequences that populate its slots live here.
/// </summary>
internal static class ServiceUtility
{
    /// <summary>
    /// Builds the container for a new wrapper connection: creates the telemetry factory, plugin
    /// manager, and <see cref="PluginService"/>, registers them into the container, then
    /// initializes the plugin chain and the plugin service's host list provider.
    /// </summary>
    public static FullServicesContainer CreateStandardContainer(
        AwsWrapperConnection connection,
        IConnectionProvider defaultConnectionProvider,
        Dictionary<string, string> props,
        ITargetConnectionDialect targetConnectionDialect,
        ConfigurationProfile? configurationProfile)
    {
        ITelemetryFactory telemetryFactory = PropertyDefinition.EnableTelemetry.GetBoolean(props)
            ? new DefaultTelemetryFactory(props)
            : NullTelemetryFactory.Instance;

        FullServicesContainer container = new(defaultConnectionProvider, new HostIdCacheService(), configurationProfile, telemetryFactory)
        {
            ConnectionPluginManager = new ConnectionPluginManager(defaultConnectionProvider, null, connection, configurationProfile),
        };

        PluginService pluginService = new(container, connection, props, targetConnectionDialect);
        container.PluginService = pluginService;
        container.HostListProviderService = pluginService;
        pluginService.InitHostListProvider();

        container.ConnectionPluginManager.InitConnectionPluginChain(container, props);
        return container;
    }

    /// <summary>
    /// Creates a monitor-scoped container from a connection's container. Shared background
    /// monitors must not capture the creating connection's <see cref="Driver.PluginService"/>
    /// (they are cached process-wide and outlive it), so this builds them a
    /// <see cref="PartialPluginService"/> with its own plugin chain. The source's
    /// <see cref="Configuration.ConfigurationProfile"/> is inherited so profile-only plugins
    /// (e.g. iam) still apply to monitoring connections; the dialect must be the confirmed
    /// dialect because monitor connections never re-run dialect detection.
    /// </summary>
    public static FullServicesContainer CreateMinimalContainer(
        FullServicesContainer source,
        Dictionary<string, string> props,
        IDialect dialect,
        ITargetConnectionDialect targetConnectionDialect)
    {
        FullServicesContainer container = new(source.DefaultConnectionProvider, source.HostIdCacheService, source.ConfigurationProfile, source.TelemetryFactory)
        {
            ConnectionPluginManager = new ConnectionPluginManager(source.DefaultConnectionProvider, source.ConfigurationProfile),
        };

        // Register the monitor-scoped service into the container before building its host list
        // provider and plugin chain, both of which read the service back off the container.
        PartialPluginService pluginService = new(container, props, dialect, targetConnectionDialect);
        container.PluginService = pluginService;
        container.HostListProviderService = pluginService;
        pluginService.InitHostListProvider();

        container.ConnectionPluginManager.InitConnectionPluginChain(container, props);
        return container;
    }

    /// <summary>
    /// Creates the container for a shared background monitor: a minimal container (see
    /// <see cref="CreateMinimalContainer"/>) cloned from the creating connection's container, so
    /// the monitor gets its own plugin chain instead of pinning the creating connection's. When no
    /// source container exists (components constructed directly with a plugin service, e.g. test
    /// mocks), the creating service is shared instead, wrapped in a bare container so monitors have
    /// one constructor shape. The dialect is snapshotted from the creating service and must already
    /// be confirmed.
    /// </summary>
    public static FullServicesContainer CreateMonitorContainer(
        FullServicesContainer? source,
        IPluginService pluginService,
        Dictionary<string, string> props)
    {
        if (source != null)
        {
            return CreateMinimalContainer(
                source,
                new Dictionary<string, string>(props),
                pluginService.Dialect,
                pluginService.TargetConnectionDialect);
        }

        FullServicesContainer sharedContainer = new(new DbConnectionProvider(), new HostIdCacheService(), null, pluginService.TelemetryFactory ?? NullTelemetryFactory.Instance)
        {
            PluginService = pluginService,
        };
        if (pluginService is IHostListProviderService hostListProviderService)
        {
            sharedContainer.HostListProviderService = hostListProviderService;
        }

        return sharedContainer;
    }
}
