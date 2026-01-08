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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class RdsMultiAzDbClusterMySqlDialect : MySqlDialect
{
    internal const string TopologyQuery = "SELECT id, endpoint, port FROM mysql.rds_topology";

    internal const string TopologyTableExistQuery =
        "SELECT 1 AS tmp FROM information_schema.tables WHERE"
        + " table_schema = 'mysql' AND table_name = 'rds_topology'";

    private const string FetchWriterNodeQuery = "SHOW REPLICA STATUS";
    private const string FetchWriterNodeQueryColumnName = "Source_Server_Id";

    private const string NodeIdQuery = "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) FROM mysql.rds_topology WHERE id = @@server_id";
    private const string IsReaderQuery = "SELECT @@read_only";

    internal const string IsDialectQuery = "SHOW VARIABLES LIKE 'report_host'";

    private static readonly ILogger<RdsMultiAzDbClusterMySqlDialect> Logger = LoggerUtils.GetLogger<RdsMultiAzDbClusterMySqlDialect>();

    public override async Task<bool> IsDialect(DbConnection connection)
    {
        try
        {
            await using (var topologyTableExistCommand = connection.CreateCommand())
            {
                topologyTableExistCommand.CommandText = TopologyTableExistQuery;
                await using var topologyTableExistReader = await topologyTableExistCommand.ExecuteReaderAsync();
                if (!(await topologyTableExistReader.ReadAsync()))
                {
                    Logger.LogDebug(Resources.RdsMultiAzDbClusterMySqlDialect_IsDialect_AsyncReader, nameof(topologyTableExistReader));
                    return false;
                }
            }

            await using (var topologyCommand = connection.CreateCommand())
            {
                topologyCommand.CommandText = TopologyQuery;
                await using var topologyReader = await topologyCommand.ExecuteReaderAsync();
                if (!(await topologyReader.ReadAsync()))
                {
                    Logger.LogDebug(Resources.RdsMultiAzDbClusterMySqlDialect_IsDialect_AsyncReader, nameof(topologyReader));
                    return false;
                }
            }

            await using (var isDialectCommand = connection.CreateCommand())
            {
                isDialectCommand.CommandText = IsDialectQuery;

                await using var reader = await isDialectCommand.ExecuteReaderAsync(CommandBehavior.SingleRow);
                if (!(await reader.ReadAsync()))
                {
                    Logger.LogDebug(Resources.RdsMultiAzDbClusterMySqlDialect_IsDialect_AsyncReader, nameof(reader));
                    return false;
                }

                string? reportHost = await reader.IsDBNullAsync(1) ? null : reader.GetString(1);
                return !string.IsNullOrEmpty(reportHost);
            }
        }
        catch (Exception ex) when (this.ExceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, Resources.Error_CantCheckDialect, nameof(RdsMultiAzDbClusterMySqlDialect));
        }

        return false;
    }

    public override IList<Type> DialectUpdateCandidates { get; } = new List<Type>();

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        return (props, hostListProviderService, pluginService) =>
            (PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCodes).Contains("failover") ?
                new MonitoringRdsMultiAzHostListProvider(
                    props,
                    hostListProviderService,
                    TopologyQuery,
                    NodeIdQuery,
                    IsReaderQuery,
                    FetchWriterNodeQuery,
                    pluginService,
                    FetchWriterNodeQuery,
                    FetchWriterNodeQueryColumnName) :
                new RdsMultiAzDbClusterListProvider(
                    props,
                    hostListProviderService,
                    TopologyQuery,
                    NodeIdQuery,
                    IsReaderQuery,
                    FetchWriterNodeQuery,
                    FetchWriterNodeQueryColumnName);
    }

    public override void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
    {
        // TODO: check what properties are needed for Multi-AZ RDS MySQL connections.
    }
}
