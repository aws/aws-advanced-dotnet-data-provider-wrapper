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
/// Topology utilities for standard Aurora clusters.
/// Parses the 5-column topology query result: SERVER_ID, isWriter, CPU, lag, timestamp.
/// </summary>
public class AuroraTopologyUtils : TopologyUtils
{
    public AuroraTopologyUtils(HostSpecBuilder hostSpecBuilder, ITopologyDialect dialect) : base(hostSpecBuilder, dialect)
    {
    }

    public override async Task<bool> IsWriterInstanceAsync(DbConnection connection, CancellationToken ct = default)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = this.dialect.WriterIdQuery;
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return !string.IsNullOrEmpty(reader.GetString(0));
        }

        return false;
    }

    public override HostSpec CreateHost(
        DbDataReader reader,
        string? suggestedWriterNodeId,
        HostSpec initialHostSpec,
        HostSpec clusterInstanceTemplate,
        IHostListProviderService hostListProviderService)
    {
        string hostName = reader.GetString(0);
        bool isWriter = reader.GetBoolean(1);
        double cpuUtilization = reader.GetDouble(2);
        double nodeLag = reader.GetDouble(3);
        DateTime lastUpdateTime = reader.IsDBNull(4)
            ? DateTime.UtcNow
            : reader.GetDateTime(4);

        long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));

        return this.CreateHost(hostName, hostName, isWriter, weight, lastUpdateTime, initialHostSpec, clusterInstanceTemplate);
    }
}
