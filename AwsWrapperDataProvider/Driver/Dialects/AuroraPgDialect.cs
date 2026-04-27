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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class AuroraPgDialect : PgDialect, ITopologyDialect, IAuroraLimitlessDialect, IBlueGreenDialect
{
    private const string ReaderOrdinal = "aurora_stat_utils";

    private static readonly ILogger<AuroraPgDialect> Logger = LoggerUtils.GetLogger<AuroraPgDialect>();

    internal static readonly string ExtensionsSql = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_catalog.pg_settings "
          + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'";

    internal static readonly string TopologySql = "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1";

    private static readonly string NodeIdQuery = "SELECT pg_catalog.aurora_db_instance_identifier(), pg_catalog.aurora_db_instance_identifier()";

    public string TopologyQuery =>
        "SELECT SERVER_ID, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
        + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
        + "FROM pg_catalog.aurora_replica_status() "
        + "WHERE EXTRACT("
        + "EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 "
        + "OR SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
        + "OR LAST_UPDATE_TIMESTAMP IS NULL";

    public string WriterIdQuery =>
        "SELECT SERVER_ID FROM pg_catalog.aurora_replica_status() "
        + "WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AND SERVER_ID OPERATOR(pg_catalog.=) aurora_db_instance_identifier()";

    protected static readonly string AuroraPostgreSqlBgTopologyExistsQuery = "SELECT 'pg_catalog.get_blue_green_fast_switchover_metadata'::regproc";

    protected static readonly string DriverVersion = "1.0.1";
    protected static readonly string AuroraPostgreSqlBgStatusQuery = $"SELECT * FROM pg_catalog.get_blue_green_fast_switchover_metadata('aws_advanced_dotnet_data_provider_wrapper-{DriverVersion}')";

    public override IList<Type> DialectUpdateCandidates { get; } = [
        typeof(GlobalAuroraPgDialect),
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
            new RdsHostListProvider(
                props,
                hostListProviderService,
                NodeIdQuery,
                pluginService,
                new AuroraTopologyUtils(hostListProviderService.HostSpecBuilder, this));
    }

    public async Task<bool> IsBlueGreenStatusAvailable(DbConnection connection)
    {
        return await DialectUtils.CheckExistenceQueries(connection, this.ExceptionHandler, Logger, AuroraPostgreSqlBgTopologyExistsQuery);
    }

    public string GetBlueGreenStatusQuery()
    {
        return AuroraPostgreSqlBgStatusQuery;
    }

    public string LimitlessRouterEndpointQuery { get => "SELECT router_endpoint, load from aurora_limitless_router_endpoints()"; }
}
