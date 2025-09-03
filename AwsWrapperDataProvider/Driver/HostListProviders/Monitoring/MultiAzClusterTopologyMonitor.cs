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
            using var command = connection.CreateCommand();
            command.CommandText = this.fetchWriterNodeQuery;
            await using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                int columnIndex = reader.GetOrdinal(this.fetchWriterNodeColumnName);
                string? nodeId = reader.IsDBNull(columnIndex)
                    ? null
                    : reader.GetString(columnIndex);

                if (!string.IsNullOrEmpty(nodeId))
                {
                    return null;
                }
            }

            return await this.GetNodeIdAsync(connection);
        }
        catch (Exception ex)
        {
            Logger.LogTrace("Error getting writer node ID: {Error}", ex.Message);
            return null;
        }
    }

    protected async Task<string?> GetSuggestedWriterNodeIdAsync(DbConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = this.fetchWriterNodeQuery;
            await using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                int columnIndex = reader.GetOrdinal(this.fetchWriterNodeColumnName);
                string? nodeId = reader.IsDBNull(columnIndex)
                    ? null
                    : reader.GetString(columnIndex);

                if (!string.IsNullOrEmpty(nodeId))
                {
                    return nodeId;
                }
            }

            return await this.GetNodeIdAsync(connection);
        }
        catch (Exception ex)
        {
            Logger.LogTrace("Error getting suggested writer node ID: {Error}", ex.Message);
            return null;
        }
    }

    protected virtual HostSpec? CreateHostFromResultSet(DbDataReader reader, string? suggestedWriterNodeId)
    {
        try
        {
            int endpointOrdinal = reader.GetOrdinal("endpoint");
            int idOrdinal = reader.GetOrdinal("id");
            string endpoint = reader.GetString(endpointOrdinal); // "instance-name.XYZ.us-west-2.rds.amazonaws.com"
            string instanceName = endpoint.Substring(0, endpoint.IndexOf(".", StringComparison.Ordinal)); // "instance-name"
            string hostId = reader.GetString(idOrdinal); // "1034958454"
            bool isWriter = hostId.Equals(suggestedWriterNodeId, StringComparison.OrdinalIgnoreCase);

            return this.CreateHost(instanceName, isWriter, 0, DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            Logger.LogTrace("Error creating host from result set: {Error}", ex.Message);
            return null;
        }
    }

    protected override async Task<IList<HostSpec>?> QueryForTopologyAsync(DbConnection connection)
    {
        try
        {
            string? suggestedWriterNodeId = await this.GetSuggestedWriterNodeIdAsync(connection);

            using var command = connection.CreateCommand();
            command.CommandTimeout = DefaultTopologyQueryTimeoutSec;
            command.CommandText = this.topologyQuery;
            await using var reader = await command.ExecuteReaderAsync();

            var hosts = new List<HostSpec>();
            var writers = new List<HostSpec>();

            while (await reader.ReadAsync())
            {
                try
                {
                    var hostSpec = this.CreateHostFromResultSet(reader, suggestedWriterNodeId);
                    if (hostSpec != null)
                    {
                        (hostSpec.Role == HostRole.Writer ? writers : hosts).Add(hostSpec);
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogTrace("Error processing query results: {Error}", ex.Message);
                    return null;
                }
            }

            if (writers.Count == 0)
            {
                Logger.LogWarning("Invalid topology: no writer nodes found");
                hosts.Clear();
            }
            else if (writers.Count == 1)
            {
                hosts.Add(writers[0]);
            }
            else
            {
                hosts.Add(writers.OrderByDescending(x => x.LastUpdateTime).First());
            }

            return hosts;
        }
        catch (Exception ex)
        {
            Logger.LogTrace("Error querying for topology: {Error}", ex.Message);
            return null;
        }
    }
}
