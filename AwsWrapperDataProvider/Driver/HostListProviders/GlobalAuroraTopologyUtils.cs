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
    private Dictionary<string, HostSpec>? instanceTemplatesByRegion;

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
    /// Sets the instance templates by region for use in <see cref="CreateHost"/>.
    /// Must be called after <see cref="ParseInstanceTemplates"/> and before the monitor starts.
    /// </summary>
    public void SetInstanceTemplatesByRegion(Dictionary<string, HostSpec> templates)
    {
        this.instanceTemplatesByRegion = templates;
    }

    /// <summary>
    /// Overrides host creation to handle the global topology query result set format.
    /// The global topology query returns 4 columns: SERVER_ID, isWriter, lag (float), AWS_REGION.
    /// Uses the AWS_REGION to look up the region-specific instance template.
    /// </summary>
    public override HostSpec CreateHost(
        DbDataReader reader,
        string? suggestedWriterNodeId,
        HostSpec initialHostSpec,
        HostSpec clusterInstanceTemplate,
        IHostListProviderService hostListProviderService)
    {
        if (this.instanceTemplatesByRegion == null)
        {
            throw new InvalidOperationException(Resources.Error_InstanceTemplatesNotSet);
        }

        string hostName = reader.GetString(0);
        bool isWriter = reader.GetBoolean(1);
        float nodeLag = reader.GetFloat(2);
        string awsRegion = reader.GetString(3);

        long weight = (long)Math.Round(nodeLag) * 100L;

        if (!this.instanceTemplatesByRegion.TryGetValue(awsRegion, out HostSpec? instanceTemplate))
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
            return null;
        }

        // Use a dictionary to ensure newer records replace older ones for the same host.
        var hostsMap = new Dictionary<string, HostSpec>();

        while (await reader.ReadAsync())
        {
            // The result set contains 4 columns:
            // SERVER_ID, isWriter (bool), lag (float), AWS_REGION
            string hostName = reader.GetString(0);
            bool isWriter = reader.GetBoolean(1);
            float nodeLag = reader.GetFloat(2);
            string awsRegion = reader.GetString(3);

            long weight = (long)Math.Round(nodeLag) * 100L;

            if (!instanceTemplatesByRegion.TryGetValue(awsRegion, out HostSpec? instanceTemplate))
            {
                throw new InvalidOperationException(
                    string.Format(Resources.Error_CannotFindInstanceTemplateForRegion, awsRegion));
            }

            string endpoint = instanceTemplate.Host.Replace("?", hostName);
            int port = instanceTemplate.IsPortSpecified
                ? instanceTemplate.Port
                : initialHostSpec.Port;

            HostSpec hostSpec = this.hostSpecBuilder
                .WithHost(endpoint)
                .WithHostId(hostName)
                .WithPort(port)
                .WithRole(isWriter ? HostRole.Writer : HostRole.Reader)
                .WithAvailability(HostAvailability.Available)
                .WithWeight(weight)
                .WithLastUpdateTime(DateTime.UtcNow)
                .Build();

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
