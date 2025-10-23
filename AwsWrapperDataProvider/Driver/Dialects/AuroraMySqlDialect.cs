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
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class AuroraMySqlDialect : MySqlDialect
{
    private static readonly ILogger<AuroraMySqlDialect> Logger = LoggerUtils.GetLogger<AuroraMySqlDialect>();

    private static readonly string TopologyQuery = "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
          + "FROM information_schema.replica_host_status "
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' ";

    private static readonly string IsReaderQuery = "SELECT @@innodb_read_only";

    private static readonly string NodeIdQuery = "SELECT @@aurora_server_id";

    internal static readonly string IsDialectQuery = "SHOW VARIABLES LIKE 'aurora_version'";

    private static readonly string IsWriterQuery = "SELECT SERVER_ID FROM information_schema.replica_host_status "
        + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id";

    public override IList<Type> DialectUpdateCandidates { get; } = [
        typeof(RdsMultiAzDbClusterMySqlDialect),
    ];

    public override async Task<bool> IsDialect(DbConnection connection)
    {
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = IsDialectQuery;
            await using var reader = await command.ExecuteReaderAsync();
            return await reader.ReadAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error occurred when checking whether it's Aurora MySql dialect");
        }

        return false;
    }

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        return (props, hostListProviderService, pluginService) =>
            PropertyDefinition.Plugins.GetString(props)!.Contains("failover") ?
                new MonitoringRdsHostListProvider(
                    props,
                    hostListProviderService,
                    TopologyQuery,
                    NodeIdQuery,
                    IsReaderQuery,
                    IsWriterQuery,
                    pluginService) :
                new RdsHostListProvider(
                    props,
                    hostListProviderService,
                    TopologyQuery,
                    NodeIdQuery,
                    IsReaderQuery);
    }
}
