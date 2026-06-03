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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// Topology monitor for Aurora Global Database clusters.
/// Extends <see cref="ClusterTopologyMonitor"/> to use global topology queries
/// and region-to-template mapping via <see cref="GlobalAuroraTopologyUtils"/>.
/// </summary>
public class GlobalAuroraTopologyMonitor : ClusterTopologyMonitor
{
    private static readonly ILogger<GlobalAuroraTopologyMonitor> Logger =
        LoggerUtils.GetLogger<GlobalAuroraTopologyMonitor>();

    private readonly Dictionary<string, HostSpec> instanceTemplatesByRegion;
    private readonly GlobalAuroraTopologyUtils globalTopologyUtils;

    public GlobalAuroraTopologyMonitor(
        string clusterId,
        MemoryCache topologyMap,
        HostSpec initialHostSpec,
        Dictionary<string, string> properties,
        IPluginService pluginService,
        IHostListProviderService hostListProviderService,
        HostSpec clusterInstanceTemplate,
        TimeSpan refreshRate,
        TimeSpan highRefreshRate,
        TimeSpan topologyCacheExpiration,
        string nodeIdQuery,
        GlobalAuroraTopologyUtils topologyUtils,
        Dictionary<string, HostSpec> instanceTemplatesByRegion)
        : base(
            clusterId,
            topologyMap,
            initialHostSpec,
            properties,
            pluginService,
            hostListProviderService,
            clusterInstanceTemplate,
            refreshRate,
            highRefreshRate,
            topologyCacheExpiration,
            nodeIdQuery,
            topologyUtils)
    {
        this.globalTopologyUtils = topologyUtils;
        this.instanceTemplatesByRegion = instanceTemplatesByRegion;
    }

    protected override async Task<HostSpec> GetInstanceTemplateAsync(string instanceId, DbConnection connection)
    {
        string? region = await this.globalTopologyUtils.GetRegionAsync(instanceId, connection);
        if (!string.IsNullOrEmpty(region))
        {
            if (!this.instanceTemplatesByRegion.TryGetValue(region, out HostSpec? instanceTemplate))
            {
                throw new InvalidOperationException(
                    string.Format(Resources.Error_CannotFindInstanceTemplateForRegion, region));
            }

            return instanceTemplate;
        }

        return this.clusterInstanceTemplate;
    }

    protected override async Task<IList<HostSpec>?> QueryForTopologyAsync(DbConnection connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            return await this.globalTopologyUtils.QueryForTopologyAsync(
                connection,
                this.initialHostSpec,
                this.instanceTemplatesByRegion);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, string.Format(
                Resources.ClusterTopologyMonitor_ErrorProcessingQueryResults, ex));
            return null;
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }
    }
}
