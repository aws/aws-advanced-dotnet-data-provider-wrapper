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
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class AuroraPgDialect : PgDialect
{
    private static readonly ILogger<AuroraPgDialect> Logger = LoggerUtils.GetLogger<AuroraPgDialect>();

    private static readonly string ExtensionsSql = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_catalog.pg_settings "
          + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'";

    private static readonly string TopologySql = "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1";

    private static readonly string TopologyQuery = "SELECT SERVER_ID, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
          + "FROM pg_catalog.aurora_replica_status() "
          + "WHERE EXTRACT("
          + "EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 "
          + "OR SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
          + "OR LAST_UPDATE_TIMESTAMP IS NULL";

    private static readonly string NodeIdQuery = "SELECT pg_catalog.aurora_db_instance_identifier()";

    private static readonly string IsReaderQuery = "SELECT pg_catalog.pg_is_in_recovery()";

    private static readonly string IsWriterQuery = "SELECT SERVER_ID FROM pg_catalog.aurora_replica_status() "
        + "WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AND SERVER_ID OPERATOR(pg_catalog.=) aurora_db_instance_identifier()";

    public override IList<Type> DialectUpdateCandidates { get; } = [
        typeof(RdsMultiAzDbClusterPgDialect),
    ];

    public override bool IsDialect(IDbConnection connection)
    {
        if (!base.IsDialect(connection))
        {
            return false;
        }

        bool hasExtensions = false;
        bool hasTopology = false;

        try
        {
            using IDbCommand command = connection.CreateCommand();
            command.CommandText = $"{ExtensionsSql}; {TopologySql}";
            using IDataReader reader = command.ExecuteReader();
            if (reader.Read())
            {
                bool auroraUtils = reader.GetBoolean(reader.GetOrdinal("aurora_stat_utils"));
                if (auroraUtils)
                {
                    hasExtensions = true;
                }
            }

            if (reader.NextResult() && reader.Read())
            {
                hasTopology = true;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error occurred when checking whether it's Aurora PG dialect");
        }

        return hasExtensions && hasTopology;
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
