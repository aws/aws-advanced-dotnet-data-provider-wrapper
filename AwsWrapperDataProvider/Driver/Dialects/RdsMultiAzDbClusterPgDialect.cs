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
using System.Globalization;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class RdsMultiAzDbClusterPgDialect : PgDialect
{
    private static readonly string DriverVersion = "1.0.1";

    private static readonly ILogger<RdsMultiAzDbClusterPgDialect> Logger = LoggerUtils.GetLogger<RdsMultiAzDbClusterPgDialect>();

    private static readonly string TopologyQuery =
        $"SELECT id, endpoint, port FROM rds_tools.show_topology('aws_dotnet_driver-{DriverVersion}')";

    private static readonly string FetchWriterNodeQuery =
        "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
        + " WHERE multi_az_db_cluster_source_dbi_resource_id OPERATOR(pg_catalog.!=)"
        + " (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())";

    internal static readonly string HasRdsToolsExtensionQuery =
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname OPERATOR(pg_catalog.=) 'rds_tools');";

    internal static readonly string IsRdsClusterQuery =
        "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()";

    private static readonly string FetchWriterNodeQueryColumnName =
        "multi_az_db_cluster_source_dbi_resource_id";

    private static readonly string NodeIdQuery =
        "SELECT id, SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) FROM rds_tools.show_topology() WHERE id OPERATOR(pg_catalog.=) rds_tools.dbi_resource_id()";

    private static readonly string IsReaderQuery =
        "SELECT pg_catalog.pg_is_in_recovery()";

    public override async Task<bool> IsDialect(DbConnection connection)
    {
        Logger.LogDebug(Resources.RdsMultiAzDbClusterPgDialect_IsDialect,
            connection.State,
            connection.GetType().FullName,
            RuntimeHelpers.GetHashCode(connection),
            connection.Database);

        try
        {
            // check if rds_tools extension is installed
            using (var command = connection.CreateCommand())
            {
                command.CommandText = HasRdsToolsExtensionQuery;
                var existsObject = await command.ExecuteScalarAsync();
                var hasRdsTools = existsObject is bool b ? b : Convert.ToBoolean(existsObject, CultureInfo.InvariantCulture);
                if (!hasRdsTools)
                {
                    Logger.LogTrace(Resources.RdsMultiAzDbClusterPgDialect_IsDialect_InvalidRdsTools);
                    return false;
                }
            }

            // check is rds multi az cluster
            using (var command = connection.CreateCommand())
            {
                command.CommandText = IsRdsClusterQuery;
                await using var reader = await command.ExecuteReaderAsync();
                return await reader.ReadAsync() && !(await reader.IsDBNullAsync(0));
            }
        }
        catch (Exception ex) when (this.ExceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, Resources.Error_CantCheckDialect_ConnectionState, nameof(RdsMultiAzDbClusterPgDialect), connection.State);
        }

        return false;
    }

    public override IList<Type> DialectUpdateCandidates { get; } = new List<Type>();

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        return (props, hostListProviderService, pluginService) =>
            (PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCodes).Contains(PluginCodes.Failover) ?
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
