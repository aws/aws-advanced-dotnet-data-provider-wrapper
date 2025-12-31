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
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class AuroraPgDialect : PgDialect
{
    private const string ReaderOrdinal = "aurora_stat_utils";

    private static readonly ILogger<AuroraPgDialect> Logger = LoggerUtils.GetLogger<AuroraPgDialect>();

    internal static readonly string ExtensionsSql = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_catalog.pg_settings "
          + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'";

    internal static readonly string TopologySql = "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1";

    private static readonly string TopologyQuery = "SELECT SERVER_ID, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
          + "FROM pg_catalog.aurora_replica_status() "
          + "WHERE EXTRACT("
          + "EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 "
          + "OR SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
          + "OR LAST_UPDATE_TIMESTAMP IS NULL";

    private static readonly string NodeIdQuery = "SELECT pg_catalog.aurora_db_instance_identifier(), pg_catalog.aurora_db_instance_identifier()";

    private static readonly string IsReaderQuery = "SELECT pg_catalog.pg_is_in_recovery()";

    private static readonly string IsWriterQuery = "SELECT SERVER_ID FROM pg_catalog.aurora_replica_status() "
        + "WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AND SERVER_ID OPERATOR(pg_catalog.=) aurora_db_instance_identifier()";

    public override IList<Type> DialectUpdateCandidates { get; } = [
        typeof(RdsMultiAzDbClusterPgDialect),
    ];

    public override async Task<bool> IsDialect(DbConnection connection)
    {
        if (!(await base.IsDialect(connection)))
        {
            return false;
        }

        bool hasExtensions = false;
        bool hasTopology = false;

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"{ExtensionsSql}; {TopologySql}";
            await using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                bool auroraUtils = reader.GetBoolean(reader.GetOrdinal(ReaderOrdinal));
                if (auroraUtils)
                {
                    hasExtensions = true;
                }
            }

            if (await reader.NextResultAsync() && await reader.ReadAsync())
            {
                hasTopology = true;
            }
        }
        catch (Exception ex) when (this.ExceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, Resources.Error_CantCheckDialect, nameof(AuroraPgDialect));
        }

        return hasExtensions && hasTopology;
    }

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        return (props, hostListProviderService, pluginService) =>
            (PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCodes).Contains("failover") ?
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
