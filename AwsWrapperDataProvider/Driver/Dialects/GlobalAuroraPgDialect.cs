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

public class GlobalAuroraPgDialect : AuroraPgDialect, IGlobalAuroraTopologyDialect
{
    private static readonly ILogger<GlobalAuroraPgDialect> Logger =
        LoggerUtils.GetLogger<GlobalAuroraPgDialect>();

    internal static readonly string GlobalStatusFuncExistsQuery =
        "select 'aurora_global_db_status'::regproc";

    internal static readonly string GlobalInstanceStatusFuncExistsQuery =
        "select 'aurora_global_db_instance_status'::regproc";

    internal static readonly string RegionCountQuery =
        "SELECT count(1) FROM aurora_global_db_status()";

    public new string TopologyQuery =>
        "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
        + "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
        + "FROM aurora_global_db_instance_status()";

    public string RegionByInstanceIdQuery =>
        "SELECT AWS_REGION FROM aurora_global_db_instance_status() WHERE SERVER_ID = '{0}'";

    public override IList<Type> DialectUpdateCandidates { get; } = [];

    public override async Task<bool> IsDialect(DbConnection connection)
    {
        if (!await base.IsDialect(connection))
        {
            return false;
        }

        if (!await DialectUtils.CheckExistenceQueries(
                connection, this.ExceptionHandler, Logger,
                GlobalStatusFuncExistsQuery,
                GlobalInstanceStatusFuncExistsQuery))
        {
            return false;
        }

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = RegionCountQuery;
            await using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
            {
                return false;
            }

            int awsRegionCount = reader.GetInt32(0);
            return awsRegionCount > 1;
        }
        catch (Exception ex) when (this.ExceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, Resources.Error_CantCheckDialect, nameof(GlobalAuroraPgDialect));
        }

        return false;
    }

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        return (props, hostListProviderService, pluginService) =>
            new GlobalAuroraHostListProvider(
                props,
                hostListProviderService,
                "SELECT pg_catalog.aurora_db_instance_identifier(), pg_catalog.aurora_db_instance_identifier()",
                pluginService,
                new GlobalAuroraTopologyUtils(this, hostListProviderService.HostSpecBuilder));
    }
}
