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

using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

/// <summary>
/// Host list provider for Aurora Global Databases.
/// Extends <see cref="RdsHostListProvider"/> to support multi-region topology.
/// </summary>
public class GlobalAuroraHostListProvider : RdsHostListProvider
{
    private static readonly ILogger<GlobalAuroraHostListProvider> Logger = LoggerUtils.GetLogger<GlobalAuroraHostListProvider>();

    protected new readonly GlobalAuroraTopologyUtils topologyUtils;
    protected Dictionary<string, HostSpec> instanceTemplatesByRegion = null!;

    public GlobalAuroraHostListProvider(
        Dictionary<string, string> properties,
        IHostListProviderService hostListProviderService,
        string nodeIdQuery,
        IPluginService pluginService,
        GlobalAuroraTopologyUtils topologyUtils)
        : base(properties, hostListProviderService, nodeIdQuery, pluginService, topologyUtils)
    {
        this.topologyUtils = topologyUtils;
    }

    protected override void Init()
    {
        base.Init();

        string? instanceTemplates = PropertyDefinition.GlobalClusterInstanceHostPatterns.GetString(this.properties);
        if (string.IsNullOrWhiteSpace(instanceTemplates))
        {
            throw new InvalidOperationException(Resources.Error_GlobalClusterInstanceHostPatternsRequired);
        }

        this.instanceTemplatesByRegion = this.topologyUtils.ParseInstanceTemplates(
            instanceTemplates,
            this.ValidateHostPattern);
    }

    /// <summary>
    /// Overrides monitor creation to create a <see cref="GlobalAuroraTopologyMonitor"/>
    /// with <see cref="GlobalAuroraTopologyUtils"/> for multi-region topology.
    /// </summary>
    protected override IClusterTopologyMonitor InitMonitor()
    {
        Logger.LogTrace(Resources.GlobalAuroraHostListProvider_InitMonitor_Initializing, this.ClusterId);
        return Monitors.Set(
            this.ClusterId,
            new GlobalAuroraTopologyMonitor(
                this.ClusterId,
                TopologyCache,
                this.initialHostSpec!,
                this.properties,
                this.pluginService,
                this.hostListProviderService,
                this.clusterInstanceTemplate!,
                this.topologyRefreshRate,
                this.highRefreshRate,
                TopologyCacheExpirationTime,
                this.nodeIdQuery,
                this.topologyUtils,
                this.instanceTemplatesByRegion),
            this.CreateCacheEntryOptions());
    }
}
