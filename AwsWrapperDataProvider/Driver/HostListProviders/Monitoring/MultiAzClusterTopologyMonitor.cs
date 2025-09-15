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
using System.Globalization;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

public class MultiAzClusterTopologyMonitor : ClusterTopologyMonitor
{
    private static readonly ILogger<MultiAzClusterTopologyMonitor> Logger = LoggerUtils.GetLogger<MultiAzClusterTopologyMonitor>();

    protected readonly string fetchWriterNodeQuery;
    protected readonly string fetchWriterNodeColumnName;

    public MultiAzClusterTopologyMonitor(
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
        string topologyQuery,
        string writerTopologyQuery,
        string nodeIdQuery,
        string fetchWriterNodeQuery,
        string fetchWriterNodeColumnName)
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
            topologyQuery,
            writerTopologyQuery,
            nodeIdQuery)
    {
        this.fetchWriterNodeQuery = fetchWriterNodeQuery;
        this.fetchWriterNodeColumnName = fetchWriterNodeColumnName;
    }

    protected override async Task<string?> GetWriterNodeIdAsync(DbConnection connection)
    {
        try
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = this.fetchWriterNodeQuery;
                await using var reader = await command.ExecuteReaderAsync(this.ctsTopologyMonitoring.Token);

                if (await reader.ReadAsync())
                {
                    int columnIndex = reader.GetOrdinal(this.fetchWriterNodeColumnName);
                    string? nodeId = reader.IsDBNull(columnIndex)
                        ? null
                        : Convert.ToString(reader.GetValue(columnIndex), CultureInfo.InvariantCulture);

                    if (!string.IsNullOrEmpty(nodeId))
                    {
                        return null;
                    }
                }
            }

            using (var nodeIdCommand = connection.CreateCommand())
            {
                nodeIdCommand.CommandText = this.nodeIdQuery;
                using var reader = await nodeIdCommand.ExecuteReaderAsync();
                if (reader.Read())
                {
                    return reader.IsDBNull(0)
                        ? null
                        : Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture);
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogTrace("Error getting writer node ID: {Error}", ex.Message);
            return null;
        }
    }

    protected override async Task<string?> GetSuggestedWriterNodeIdAsync(DbConnection connection)
    {
        try
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = this.fetchWriterNodeQuery;
                await using var reader = await command.ExecuteReaderAsync(this.ctsTopologyMonitoring.Token);

                if (await reader.ReadAsync())
                {
                    int columnIndex = reader.GetOrdinal(this.fetchWriterNodeColumnName);
                    string? nodeId = reader.IsDBNull(columnIndex)
                        ? null
                        : Convert.ToString(reader.GetValue(columnIndex), CultureInfo.InvariantCulture);

                    if (!string.IsNullOrEmpty(nodeId))
                    {
                        return nodeId;
                    }
                }
            }

            using (var nodeIdCommand = connection.CreateCommand())
            {
                nodeIdCommand.CommandText = this.nodeIdQuery;
                using var reader = await nodeIdCommand.ExecuteReaderAsync();
                if (reader.Read())
                {
                    return reader.IsDBNull(0)
                        ? null
                        : Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture);
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogTrace("Error getting suggested writer node ID: {Error}", ex.Message);
            return null;
        }
    }

    protected override HostSpec CreateHost(DbDataReader reader, string? suggestedWriterNodeId)
    {
        int endpointOrdinal = reader.GetOrdinal("endpoint");
        int idOrdinal = reader.GetOrdinal("id");
        string endpoint = reader.GetString(endpointOrdinal);
        string instanceName = endpoint.Substring(0, endpoint.IndexOf(".", StringComparison.Ordinal));
        string hostId = reader.GetString(idOrdinal);
        bool isWriter = hostId.Equals(suggestedWriterNodeId, StringComparison.OrdinalIgnoreCase);

        return this.CreateHost(instanceName, hostId, isWriter, 0, DateTime.UtcNow);
    }
}
