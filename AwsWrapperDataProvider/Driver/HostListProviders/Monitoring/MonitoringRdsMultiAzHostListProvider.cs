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

using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

public class MonitoringRdsMultiAzHostListProvider : MonitoringRdsHostListProvider
{
    private static readonly ILogger<MonitoringRdsMultiAzHostListProvider> Logger = LoggerUtils.GetLogger<MonitoringRdsMultiAzHostListProvider>();

    protected readonly string fetchWriterNodeQuery;
    protected readonly string fetchWriterNodeColumnName;

    public MonitoringRdsMultiAzHostListProvider(
        Dictionary<string, string> properties,
        IHostListProviderService hostListProviderService,
        string topologyQuery,
        string nodeIdQuery,
        string isReaderQuery,
        string isWriterQuery,
        IPluginService pluginService,
        string fetchWriterNodeQuery,
        string fetchWriterNodeColumnName)
        : base(properties, hostListProviderService, topologyQuery, nodeIdQuery, isReaderQuery, isWriterQuery, pluginService)
    {
        this.fetchWriterNodeQuery = fetchWriterNodeQuery;
        this.fetchWriterNodeColumnName = fetchWriterNodeColumnName;
    }

    protected override IClusterTopologyMonitor InitMonitor()
    {
        return Monitors.Set(
            this.ClusterId,
            new MultiAzClusterTopologyMonitor(
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
                this.topologyQuery,
                this.isWriterQuery,
                this.nodeIdQuery,
                this.fetchWriterNodeQuery,
                this.fetchWriterNodeColumnName),
            this.CreateCacheEntryOptions());
    }
}
