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
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class AuroraPgDialect : PgDialect
{
    private static readonly string ExtensionsSql = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_settings "
          + "WHERE name='rds.extensions'";

    private static readonly string TopologySql = "SELECT 1 FROM aurora_replica_status() LIMIT 1";

    private static readonly string TopologyQuery = "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
          + "FROM aurora_replica_status() "
          + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "OR LAST_UPDATE_TIMESTAMP IS NULL";

    private static readonly string NodeIdQuery = "SELECT aurora_db_instance_identifier()";

    private static readonly string IsReaderQuery = "SELECT pg_is_in_recovery()";

    public override IList<Type> DialectUpdateCandidates { get; } =
    [
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
            command.CommandText = ExtensionsSql;
            using IDataReader reader = command.ExecuteReader();
            if (reader.Read())
            {
                bool auroraUtils = reader.GetBoolean(reader.GetOrdinal("aurora_stat_utils"));
                if (auroraUtils)
                {
                    hasExtensions = true;
                }
            }
        }
        catch (DbException)
        {
            // ignored
        }

        if (!hasExtensions)
        {
            return false;
        }

        try
        {
            using IDbCommand command = connection.CreateCommand();
            command.CommandText = TopologySql;
            using IDataReader reader = command.ExecuteReader();
            if (reader.Read())
            {
                hasTopology = true;
            }
        }
        catch (DbException)
        {
            // ignored
        }

        return hasExtensions && hasTopology;
    }

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        // TODO add MonitoringRdsHostListProvider for failover plugin
        return (props, hostListProviderService, pluginService) => new AuroraHostListProvider(
            props, hostListProviderService, TopologyQuery, NodeIdQuery, IsReaderQuery);
    }
}
