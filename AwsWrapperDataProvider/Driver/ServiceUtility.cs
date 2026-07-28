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

using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// Builds <see cref="FullServicesContainer"/> instances. The container itself is a plain holder;
/// the assembly sequences that populate its slots live here.
/// </summary>
internal static class ServiceUtility
{
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
        FullServicesContainer container = new(source.DefaultConnectionProvider, source.HostIdCacheService, source.ConfigurationProfile)
        {
            ConnectionPluginManager = new ConnectionPluginManager(source.DefaultConnectionProvider, source.ConfigurationProfile),
            TelemetryFactory = source.TelemetryFactory,
        };

        // Constructed for its side effect: the constructor registers itself into the container's
        // PluginService and HostListProviderService slots (which the chain initialization below
        // reads back), then builds this monitor-scoped service's own host list provider.
        _ = new PartialPluginService(container, props, dialect, targetConnectionDialect);
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

        FullServicesContainer sharedContainer = new(new DbConnectionProvider(), new HostIdCacheService(), null)
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
