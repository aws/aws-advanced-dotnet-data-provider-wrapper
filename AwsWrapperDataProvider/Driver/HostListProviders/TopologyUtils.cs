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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

/// <summary>
/// Abstract base class defining utility methods for retrieving and processing database topology information.
/// Subclasses define logic specific to various database engine deployments (Aurora, Multi-AZ, Global Aurora).
/// </summary>
public abstract class TopologyUtils
{
    protected const int DefaultQueryTimeoutSec = 1;

    protected readonly HostSpecBuilder hostSpecBuilder;
    protected readonly ITopologyDialect dialect;

    protected TopologyUtils(HostSpecBuilder hostSpecBuilder, ITopologyDialect dialect)
    {
        this.hostSpecBuilder = hostSpecBuilder;
        this.dialect = dialect;
    }

    /// <summary>
    /// Creates a <see cref="HostSpec"/> from a topology query result row.
    /// Called by <see cref="Monitoring.ClusterTopologyMonitor"/> during topology refresh.
    /// </summary>
    public abstract HostSpec CreateHost(
        DbDataReader reader,
        string? suggestedWriterNodeId,
        HostSpec initialHostSpec,
        HostSpec clusterInstanceTemplate,
        IHostListProviderService hostListProviderService);

    /// <summary>
    /// Gets the suggested writer node ID. For Multi-AZ this queries the database;
    /// for Aurora this returns null (not needed).
    /// </summary>
    public virtual Task<string?> GetSuggestedWriterNodeIdAsync(
        DbConnection connection,
        string nodeIdQuery,
        CancellationToken ct)
        => Task.FromResult<string?>(null);

    /// <summary>
    /// Checks if the current connection is to the writer instance.
    /// Aurora: returns true if the writer ID query returns a non-empty result.
    /// Multi-AZ: returns true if the writer ID query returns NO rows (opposite logic).
    /// </summary>
    public abstract Task<bool> IsWriterInstanceAsync(DbConnection connection, CancellationToken ct = default);

    /// <summary>
    /// Queries the database for topology information and returns a list of hosts.
    /// Executes the topology query, parses each row via <see cref="CreateHost"/>,
    /// and verifies exactly one writer exists.
    /// </summary>
    public virtual async Task<List<HostSpec>?> QueryForTopologyAsync(
        DbConnection connection,
        HostSpec initialHostSpec,
        HostSpec clusterInstanceTemplate,
        IHostListProviderService hostListProviderService,
        CancellationToken ct = default)
    {
        string? suggestedWriterNodeId = await this.GetSuggestedWriterNodeIdAsync(
            connection, string.Empty, ct);

        await using var command = connection.CreateCommand();
        command.CommandText = this.dialect.TopologyQuery;

        // The topology query is not monitored by the EFM plugin, so it needs a timeout.
        if (command.CommandTimeout == 0)
        {
            command.CommandTimeout = DefaultQueryTimeoutSec;
        }

        await using var reader = await command.ExecuteReaderAsync(ct);

        if (reader.FieldCount == 0)
        {
            // We expect at least 4 columns. The server may return 0 columns if failover has occurred.
            return null;
        }

        var allHosts = new List<HostSpec>();
        while (await reader.ReadAsync(ct))
        {
            HostSpec hostSpec = this.CreateHost(
                reader, suggestedWriterNodeId, initialHostSpec, clusterInstanceTemplate, hostListProviderService);
            allHosts.Add(hostSpec);
        }

        return VerifyWriter(allHosts);
    }

    /// <summary>
    /// Creates a <see cref="HostSpec"/> from the given topology information.
    /// Called by subclasses and by <see cref="Monitoring.ClusterTopologyMonitor"/> when building host entries.
    /// </summary>
    public HostSpec CreateHost(
        string instanceId,
        string instanceName,
        bool isWriter,
        long weight,
        DateTime? lastUpdateTime,
        HostSpec initialHostSpec,
        HostSpec instanceTemplate)
    {
        instanceName ??= "?";
        string endpoint = instanceTemplate.Host.Replace("?", instanceName);
        int port = instanceTemplate.IsPortSpecified
            ? instanceTemplate.Port
            : initialHostSpec.Port;

        HostSpec host = this.hostSpecBuilder
            .WithHost(endpoint)
            .WithHostId(instanceId)
            .WithPort(port)
            .WithRole(isWriter ? HostRole.Writer : HostRole.Reader)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithLastUpdateTime(lastUpdateTime)
            .Build();

        return host;
    }

    /// <summary>
    /// Verifies that exactly one writer exists in the host list.
    /// If multiple writers exist, takes the latest updated one.
    /// </summary>
    protected static List<HostSpec>? VerifyWriter(List<HostSpec> allHosts)
    {
        if (allHosts.Count == 0)
        {
            return null;
        }

        List<HostSpec> hosts = [];
        List<HostSpec> writers = [];

        foreach (HostSpec host in allHosts)
        {
            if (host.Role == HostRole.Writer)
            {
                writers.Add(host);
            }
            else
            {
                hosts.Add(host);
            }
        }

        if (writers.Count == 0)
        {
            return null;
        }
        else if (writers.Count == 1)
        {
            hosts.Add(writers[0]);
        }
        else
        {
            // Take the latest updated writer node as the current writer.
            hosts.Add(writers.MaxBy(x => x.LastUpdateTime)!);
        }

        return hosts;
    }
}
