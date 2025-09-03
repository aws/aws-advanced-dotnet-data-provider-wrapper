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

namespace AwsWrapperDataProvider.Driver.Dialects;

public class RdsMultiAzPgDialect : PgDialect
{
    private static readonly string TopologyQuery =
        $"SELECT id, endpoint, port FROM rds_tools.show_topology('aws_jdbc_driver-{PropertyDefinition.MultiAzRdsJdbcDriverVersion.DefaultValue}')";

    private static readonly string WriterNodeFuncExistsQuery =
        "SELECT 1 AS tmp FROM information_schema.routines"
        + " WHERE routine_schema='rds_tools' AND routine_name='multi_az_db_cluster_source_dbi_resource_id'";

    private static readonly string FetchWriterNodeQuery =
        "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
        + " WHERE multi_az_db_cluster_source_dbi_resource_id !="
        + " (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())";

    private static readonly string FetchWriterNodeQueryColumnName =
        "multi_az_db_cluster_source_dbi_resource_id";

    private static readonly string NodeIdQuery =
        "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()";

    private static readonly string NodeIdFuncExistsQuery =
        "SELECT 1 AS tmp "
        + "FROM information_schema.routines "
        + "WHERE routine_schema='rds_tools' AND routine_name='dbi_resource_id'";

    private static readonly string IsReaderQuery =
        "SELECT pg_is_in_recovery()";

    public override bool IsDialect(IDbConnection connection)
    {
        try
        {
            using IDbCommand topologyCommand = connection.CreateCommand();
            topologyCommand.CommandText = TopologyQuery;
            using IDataReader topologyReader = topologyCommand.ExecuteReader();
            return topologyReader.Read();
        }
        catch (DbException)
        {
            // ignore
        }

        return false;
    }

    // Java returned `null` here; provide an empty list in C#.
    public override IList<Type> DialectUpdateCandidates { get; } = new List<Type>();

    public override HostListProviderSupplier HostListProviderSupplier => this.GetHostListProviderSupplier();

    private HostListProviderSupplier GetHostListProviderSupplier()
    {
        return (props, hostListProviderService, pluginService) =>
            PropertyDefinition.Plugins.GetString(props)!.Contains("failover") ?
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
                new MultiAzRdsHostListProvider(
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
