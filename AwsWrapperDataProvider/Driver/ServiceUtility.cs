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

using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;

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

        PartialPluginService partialPluginService = new(container, props, dialect, targetConnectionDialect);
        container.ConnectionPluginManager.InitConnectionPluginChain(partialPluginService, props);
        return container;
    }

    /// <summary>
    /// Recovers the backing container from a core plugin service. Plugins receive only an
    /// <see cref="IPluginService"/> through the public <c>IConnectionPluginFactory.GetInstance</c>
    /// (unchanged until a major version threads the container through it), so a plugin that needs
    /// the container must derive it from its plugin service. Returns null for services not backed
    /// by a container (e.g. test mocks). Unlike the host list provider supplier — which receives
    /// the container directly — this bridge exists solely for the plugin construction boundary.
    /// </summary>
    public static FullServicesContainer? FromPluginService(IPluginService? pluginService)
    {
        return pluginService switch
        {
            PluginService fullService => fullService.ServicesContainer,
            PartialPluginService partialService => partialService.ServicesContainer,
            _ => null,
        };
    }
}
