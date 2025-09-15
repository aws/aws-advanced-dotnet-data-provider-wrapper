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

using System.Data;
using System.Globalization;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

public class RdsMultiAzDbClusterListProvider : RdsHostListProvider
{
    private static readonly ILogger<RdsMultiAzDbClusterListProvider> Logger = LoggerUtils.GetLogger<RdsMultiAzDbClusterListProvider>();

    private readonly string fetchWriterNodeQuery;
    private readonly string fetchWriterNodeQueryHeader;

    public RdsMultiAzDbClusterListProvider(
        Dictionary<string, string> properties,
        IHostListProviderService hostListProviderService,
        string topologyQuery,
        string nodeIdQuery,
        string isReaderQuery,
        string fetchWriterNodeQuery,
        string fetchWriterNodeQueryHeader) : base(properties, hostListProviderService, topologyQuery, nodeIdQuery, isReaderQuery)
    {
        this.fetchWriterNodeQuery = fetchWriterNodeQuery;
        this.fetchWriterNodeQueryHeader = fetchWriterNodeQueryHeader;
    }

    internal override List<HostSpec> QueryForTopology(IDbConnection conn)
    {
        string? writerNodeId;

        using (IDbCommand fetchWriterNodeCommand = conn.CreateCommand())
        {
            fetchWriterNodeCommand.CommandTimeout = DefaultTopologyQueryTimeoutSec;
            fetchWriterNodeCommand.CommandText = this.fetchWriterNodeQuery;
            using IDataReader writerNodeReader = fetchWriterNodeCommand.ExecuteReader();
            writerNodeId = this.ProcessWriterNodeId(writerNodeReader);
        }

        if (writerNodeId == null)
        {
            using (IDbCommand nodeIdCommand = conn.CreateCommand())
            {
                nodeIdCommand.CommandTimeout = DefaultTopologyQueryTimeoutSec;
                nodeIdCommand.CommandText = this.nodeIdQuery;
                using var nodeIdReader = nodeIdCommand.ExecuteReader();
                while (nodeIdReader.Read())
                {
                    writerNodeId = nodeIdReader.GetString(0);
                }
            }
        }

        using IDbCommand topologyCommand = conn.CreateCommand();
        topologyCommand.CommandTimeout = DefaultTopologyQueryTimeoutSec;
        topologyCommand.CommandText = this.topologyQuery;
        using var topologyReader = topologyCommand.ExecuteReader();
        return this.ProcessTopologyQueryResults(topologyReader, writerNodeId);
    }

    private string? ProcessWriterNodeId(IDataReader fetchWriterNodeReader)
    {
        if (fetchWriterNodeReader.Read())
        {
            int ordinal = fetchWriterNodeReader.GetOrdinal(this.fetchWriterNodeQueryHeader);
            if (!fetchWriterNodeReader.IsDBNull(ordinal))
            {
                return Convert.ToString(fetchWriterNodeReader.GetValue(ordinal), CultureInfo.InvariantCulture);
            }
        }

        Logger.LogWarning("No writer node found in the result of the fetchWriterNodeQuery. " +
                          "Ensure that the query is correct and that the database is configured properly.");
        return null;
    }

    private List<HostSpec> ProcessTopologyQueryResults(IDataReader topologyReader, string? writerNodeId)
    {
        var hostMap = new Dictionary<string, HostSpec>(StringComparer.Ordinal);

        while (topologyReader.Read())
        {
            var host = this.CreateHost(topologyReader, writerNodeId);
            hostMap[host.Host] = host;
        }

        var hosts = new List<HostSpec>();
        var writers = new List<HostSpec>();

        foreach (var host in hostMap.Values)
        {
            if (host.Role != HostRole.Writer)
            {
                hosts.Add(host);
            }
            else
            {
                writers.Add(host);
            }
        }

        if (writers.Count == 0)
        {
            Logger.LogError("Invalid topology: no writer instance found.");
            hosts.Clear();
        }
        else
        {
            hosts.Add(writers[0]);
        }

        return hosts;
    }

    private HostSpec CreateHost(IDataReader reader, string? writerNodeId)
    {
        var endpointOrdinal = reader.GetOrdinal("endpoint");
        var idOrdinal = reader.GetOrdinal("id");
        var portOrdinal = reader.GetOrdinal("port");

        if (reader.IsDBNull(endpointOrdinal))
        {
            throw new DataException("Topology query result is missing 'endpoint'.");
        }

        string hostName = reader.GetString(endpointOrdinal);
        int firstDot = hostName.IndexOf('.', StringComparison.Ordinal);
        string instanceName = firstDot > 0 ? hostName.Substring(0, firstDot) : hostName;

        // Build DNS from template: replace '?' with node/instance name
        string endpoint = this.GetHostEndpoint(instanceName);

        string hostId = reader.IsDBNull(idOrdinal) ? string.Empty : reader.GetString(idOrdinal);

        int queryPort = reader.IsDBNull(portOrdinal) ? 0 : reader.GetInt32(portOrdinal);
        int port = this.clusterInstanceTemplate!.IsPortSpecified
            ? this.clusterInstanceTemplate.Port
            : queryPort;

        bool isWriter = !string.IsNullOrEmpty(hostId) &&
                        string.Equals(hostId, writerNodeId, StringComparison.Ordinal);

        var hostSpec = this.hostListProviderService.HostSpecBuilder
            .WithHost(endpoint)
            .WithHostId(hostId)
            .WithPort(port)
            .WithRole(isWriter ? HostRole.Writer : HostRole.Reader)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(0)
            .WithLastUpdateTime(DateTime.UtcNow)
            .Build();

        hostSpec.AddAlias(hostName);
        return hostSpec;
    }

    protected string GetHostEndpoint(string nodeName)
    {
        string host = this.clusterInstanceTemplate!.Host;
        return host.Replace("?", nodeName, StringComparison.Ordinal);
    }
}
