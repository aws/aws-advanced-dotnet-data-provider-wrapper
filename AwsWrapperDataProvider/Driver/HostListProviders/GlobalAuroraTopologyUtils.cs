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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

/// <summary>
/// Topology utilities for Aurora Global Database clusters.
/// Extends <see cref="AuroraTopologyUtils"/> to handle multi-region instance template resolution.
/// </summary>
public class GlobalAuroraTopologyUtils : AuroraTopologyUtils
{
    private static readonly ILogger<GlobalAuroraTopologyUtils> Logger = LoggerUtils.GetLogger<GlobalAuroraTopologyUtils>();

    private new readonly IGlobalAuroraTopologyDialect dialect;

    public GlobalAuroraTopologyUtils(
        IGlobalAuroraTopologyDialect dialect,
        HostSpecBuilder hostSpecBuilder)
        : base(hostSpecBuilder, dialect)
    {
        this.dialect = dialect;
    }

    /// <summary>
    /// This overload should not be called on GlobalAuroraTopologyUtils.
    /// Use the overload that accepts instanceTemplatesByRegion instead.
    /// </summary>
    public override Task<List<HostSpec>?> QueryForTopologyAsync(
        DbConnection connection,
        HostSpec initialHostSpec,
        HostSpec clusterInstanceTemplate,
        IHostListProviderService hostListProviderService,
        CancellationToken ct = default)
    {
        throw new NotSupportedException();
    }

    /// <summary>
    /// This overload should not be called on GlobalAuroraTopologyUtils.
    /// Global topology queries use <see cref="QueryForTopologyAsync(DbConnection, HostSpec, Dictionary{string, HostSpec})"/>.
    /// </summary>
    protected override Task<List<HostSpec>?> GetHostsAsync(
        DbConnection connection,
        DbDataReader reader,
        HostSpec initialHostSpec,
        HostSpec instanceTemplate,
        CancellationToken ct)
    {
        throw new NotSupportedException();
    }

    /// <summary>
    /// Creates a <see cref="HostSpec"/> from the global topology query result row.
    /// The global topology query returns 4 columns: SERVER_ID, isWriter, lag (float), AWS_REGION.
    /// Uses the AWS_REGION to look up the region-specific instance template.
    /// </summary>
    protected HostSpec CreateHost(
        DbDataReader reader,
        HostSpec initialHostSpec,
        Dictionary<string, HostSpec> instanceTemplatesByRegion)
    {
        // According to the topology query the result set should contain 4 columns:
        // instance ID, 1/0 (writer/reader), node lag in time (msec), AWS region.
        string hostName = reader.GetString(0);
        bool isWriter = reader.GetBoolean(1);
        float nodeLag = reader.GetFloat(2);
        string awsRegion = reader.GetString(3);

        // Calculate weight based on node lag in time.
        long weight = (long)Math.Round(nodeLag) * 100L;

        if (!instanceTemplatesByRegion.TryGetValue(awsRegion, out HostSpec? instanceTemplate))
        {
            throw new InvalidOperationException(
                string.Format(Resources.Error_CannotFindInstanceTemplateForRegion, awsRegion));
        }

        return this.CreateHost(hostName, hostName, isWriter, weight, DateTime.UtcNow, initialHostSpec, instanceTemplate);
    }

    public Dictionary<string, HostSpec> ParseInstanceTemplates(
        string instanceTemplatesString,
        Action<string> hostValidator)
    {
        if (string.IsNullOrWhiteSpace(instanceTemplatesString))
        {
            throw new InvalidOperationException(Resources.Error_GlobalClusterInstanceHostPatternsRequired);
        }

        var instanceTemplates = new Dictionary<string, HostSpec>(StringComparer.OrdinalIgnoreCase);
        string[] entries = instanceTemplatesString.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        foreach (string entry in entries)
        {
            var (region, hostSpec) = ConnectionPropertiesUtils.ParseHostPortPairWithRegionPrefix(entry, this.hostSpecBuilder);
            hostValidator(hostSpec.Host);
            instanceTemplates[region] = hostSpec;
        }

        Logger.LogTrace(Resources.GlobalAuroraTopologyUtils_DetectedGdbPatterns,
            string.Join(", ", instanceTemplates.Select(kv => $"{kv.Key}={kv.Value.Host}")));

        return instanceTemplates;
    }

    public async Task<List<HostSpec>?> QueryForTopologyAsync(
        DbConnection conn,
        HostSpec initialHostSpec,
        Dictionary<string, HostSpec> instanceTemplatesByRegion)
    {
        await using var command = conn.CreateCommand();
        command.CommandText = this.dialect.TopologyQuery;

        // The topology query is not monitored by the EFM plugin, so it needs a timeout.
        if (command.CommandTimeout == 0)
        {
            command.CommandTimeout = DefaultQueryTimeoutSec;
        }

        await using var reader = await command.ExecuteReaderAsync();

        if (reader.FieldCount == 0)
        {
            // We expect at least 4 columns. The server may return 0 columns if failover has occurred.
            return null;
        }

        // Data in the result set is ordered by last update time, so the latest records are last.
        // We add hosts to a map to ensure newer records replace the older ones.
        var hostsMap = new Dictionary<string, HostSpec>();

        while (await reader.ReadAsync())
        {
            HostSpec hostSpec = this.CreateHost(reader, initialHostSpec, instanceTemplatesByRegion);
            hostsMap[hostSpec.Host] = hostSpec;
        }

        return VerifyWriter([.. hostsMap.Values]);
    }

    public async Task<string?> GetRegionAsync(string instanceId, DbConnection conn)
    {
        await using var command = conn.CreateCommand();
        command.CommandText = string.Format(this.dialect.RegionByInstanceIdQuery, instanceId);

        await using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            string awsRegion = reader.GetString(0);
            return string.IsNullOrEmpty(awsRegion) ? null : awsRegion;
        }

        return null;
    }
}
