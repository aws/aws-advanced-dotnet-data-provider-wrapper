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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

/// <summary>
/// Topology utilities for Multi-AZ DB clusters.
/// Parses the topology query result with columns: id, endpoint, port.
/// Determines the writer by comparing each row's id to the writer ID returned by the dialect's writer-id query.
/// </summary>
public class MultiAzTopologyUtils : TopologyUtils
{
    private static readonly ILogger<MultiAzTopologyUtils> Logger = LoggerUtils.GetLogger<MultiAzTopologyUtils>();

    private readonly IMultiAzClusterDialect multiAzDialect;
    private readonly string nodeIdQuery;

    public MultiAzTopologyUtils(
        HostSpecBuilder hostSpecBuilder,
        IMultiAzClusterDialect dialect,
        string nodeIdQuery)
        : base(hostSpecBuilder, dialect)
    {
        this.multiAzDialect = dialect;
        this.nodeIdQuery = nodeIdQuery;
    }

    public override async Task<bool> IsWriterInstanceAsync(DbConnection connection, CancellationToken ct = default)
    {
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = this.multiAzDialect.WriterIdQuery;
            await using var reader = await command.ExecuteReaderAsync(ct);
            // When connected to a writer, the result is empty
            return !await reader.ReadAsync(ct);
        }
        catch (Exception ex)
        {
            Logger.LogTrace(Resources.MultiAzClusterTopologyMonitor_GetWriterNodeIdAsync_Error, ex.Message);
            return false;
        }
    }

    protected override async Task<List<HostSpec>?> GetHostsAsync(
        DbConnection connection,
        DbDataReader reader,
        HostSpec initialHostSpec,
        HostSpec instanceTemplate,
        CancellationToken ct)
    {
        string? writerId = await this.GetWriterIdAsync(connection, ct);

        // Data in the result set is ordered by last update time, so the latest records are last.
        // We add hosts to a map to ensure newer records replace the older ones.
        var hostsMap = new Dictionary<string, HostSpec>();
        while (await reader.ReadAsync(ct))
        {
            try
            {
                HostSpec host = this.CreateHost(reader, initialHostSpec, instanceTemplate, writerId);
                if (!hostsMap.TryGetValue(host.Host, out HostSpec? existing)
                    || existing.LastUpdateTime < host.LastUpdateTime)
                {
                    hostsMap[host.Host] = host;
                }
            }
            catch (Exception ex)
            {
                Logger.LogTrace(Resources.ClusterTopologyMonitor_ErrorProcessingQueryResults, ex.Message);
                return null;
            }
        }

        return [.. hostsMap.Values];
    }

    protected HostSpec CreateHost(
        DbDataReader reader,
        HostSpec initialHostSpec,
        HostSpec instanceTemplate,
        string? writerId)
    {
        int endpointOrdinal = reader.GetOrdinal("endpoint");
        int idOrdinal = reader.GetOrdinal("id");

        // endpoint: "instance-name.XYZ.us-west-2.rds.amazonaws.com"
        string endpoint = reader.GetString(endpointOrdinal);
        // instanceName: "instance-name"
        string instanceName = endpoint.Substring(0, endpoint.IndexOf(".", StringComparison.Ordinal));
        // hostId: e.g. "1034958454" (numeric id returned as string)
        string hostId = Convert.ToString(reader.GetValue(idOrdinal), CultureInfo.InvariantCulture)!;
        bool isWriter = hostId.Equals(writerId, StringComparison.OrdinalIgnoreCase);

        return this.CreateHost(hostId, instanceName, isWriter, 0, DateTime.UtcNow, initialHostSpec, instanceTemplate);
    }

    /// <summary>
    /// Returns the writer node ID. When connected to a reader, the writer-id query returns the writer's ID directly.
    /// When connected to the writer, the query returns no rows, so the node-id query is used to return the current
    /// connection's id instead.
    /// </summary>
    protected async Task<string?> GetWriterIdAsync(DbConnection connection, CancellationToken ct)
    {
        try
        {
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = this.multiAzDialect.WriterIdQuery;
                await using var reader = await command.ExecuteReaderAsync(ct);

                if (await reader.ReadAsync(ct))
                {
                    int columnIndex = reader.GetOrdinal(this.multiAzDialect.WriterIdColumnName);
                    string? writerId = await reader.IsDBNullAsync(columnIndex, ct)
                        ? null
                        : Convert.ToString(reader.GetValue(columnIndex), CultureInfo.InvariantCulture);

                    if (!string.IsNullOrEmpty(writerId))
                    {
                        return writerId;
                    }
                }
            }

            // The writer ID is only returned when connected to a reader, so if the query does not return a value, it
            // means we are connected to a writer. Fall back to the node-id query to retrieve the current instance's id.
            await using (var nodeIdCommand = connection.CreateCommand())
            {
                nodeIdCommand.CommandText = this.nodeIdQuery;
                await using var reader = await nodeIdCommand.ExecuteReaderAsync(ct);
                if (await reader.ReadAsync(ct))
                {
                    return await reader.IsDBNullAsync(0, ct)
                        ? null
                        : Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture);
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogTrace(Resources.MultiAzClusterTopologyMonitor_GetSuggestedWriterNodeIdAsync_Error, ex.Message);
            return null;
        }
    }
}
