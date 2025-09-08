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

public class RdsMultiAzDbClusterMySqlDialect : MySqlDialect
{
    private const string TopologyQuery = "SELECT id, endpoint, port FROM mysql.rds_topology";

    private const string TopologyTableExistQuery =
        "SELECT 1 AS tmp FROM information_schema.tables WHERE"
        + " table_schema = 'mysql' AND table_name = 'rds_topology'";

    private const string FetchWriterNodeQuery = "SHOW REPLICA STATUS";
    private const string FetchWriterNodeQueryColumnName = "Source_Server_Id";

    private const string NodeIdQuery = "SELECT @@server_id";
    private const string IsReaderQuery = "SELECT @@read_only";
    
    private const string IsDialectQuery = "SHOW VARIABLES LIKE 'report_host'";

    public override bool IsDialect(IDbConnection connection)
    {
        try
        {
            using IDbCommand topologyTableExistCommand = connection.CreateCommand();
            topologyTableExistCommand.CommandText = TopologyTableExistQuery;
            using IDataReader topologyTableExistReader = topologyTableExistCommand.ExecuteReader();
            if (!topologyTableExistReader.Read())
            {
                return false;
            }

            using IDbCommand topologyCommand = connection.CreateCommand();
            topologyCommand.CommandText = TopologyQuery;
            using IDataReader topologyReader = topologyCommand.ExecuteReader();
            if (!topologyReader.Read())
            {
                return false;
            }

            using IDbCommand isDialectCommand = connection.CreateCommand();
            isDialectCommand.CommandText = IsDialectQuery;

            using var reader = isDialectCommand.ExecuteReader(CommandBehavior.SingleRow);
            if (!reader.Read())
            {
                return false;
            }

            string? reportHost = reader.IsDBNull(1) ? null : reader.GetString(1);
            return !string.IsNullOrEmpty(reportHost);
        }
        catch (DbException)
        {
            // ignore
        }

        return false;
    }

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
