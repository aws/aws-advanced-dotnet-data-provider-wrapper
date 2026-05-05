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
    /// Checks if the current connection is to the writer instance.
    /// Aurora: returns true if the writer ID query returns a non-empty result.
    /// Multi-AZ: returns true if the writer ID query returns NO rows (opposite logic).
    /// </summary>
    public abstract Task<bool> IsWriterInstanceAsync(DbConnection connection, CancellationToken ct = default);

    /// <summary>
    /// Queries the database for topology information and returns a list of hosts.
    /// Executes the topology query, delegates row processing to <see cref="GetHostsAsync"/>,
    /// and verifies exactly one writer exists.
    /// </summary>
    public virtual async Task<List<HostSpec>?> QueryForTopologyAsync(
        DbConnection connection,
        HostSpec initialHostSpec,
        HostSpec clusterInstanceTemplate,
        IHostListProviderService hostListProviderService,
        CancellationToken ct = default)
    {
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

        List<HostSpec>? hosts = await this.GetHostsAsync(
            connection, reader, initialHostSpec, clusterInstanceTemplate, ct);
        return VerifyWriter(hosts);
    }

    /// <summary>
    /// Reads rows from the topology query result and constructs the list of <see cref="HostSpec"/> objects.
    /// Subclasses implement deployment-specific parsing (Aurora, Multi-AZ, Global Aurora).
    /// </summary>
    /// <param name="connection">The connection used to run the topology query (may be used to run auxiliary queries).</param>
    /// <param name="reader">The topology query result reader.</param>
    /// <param name="initialHostSpec">The initial host specification used for connecting.</param>
    /// <param name="instanceTemplate">The cluster instance template used to construct new hosts.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A list of hosts, or null if processing fails.</returns>
    protected abstract Task<List<HostSpec>?> GetHostsAsync(
        DbConnection connection,
        DbDataReader reader,
        HostSpec initialHostSpec,
        HostSpec instanceTemplate,
        CancellationToken ct);

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
    protected static List<HostSpec>? VerifyWriter(List<HostSpec>? allHosts)
    {
        if (allHosts == null || allHosts.Count == 0)
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
