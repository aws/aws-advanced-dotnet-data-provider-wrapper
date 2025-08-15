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

public class MultiAzRdsMySqlDialect : MySqlDialect
{
    private const string TopologyQuery = "SELECT id, endpoint, port FROM mysql.rds_topology";

    private const string FetchWriterNodeQuery = "SHOW REPLICA STATUS";
    private const string FetchWriterNodeQueryColumnName = "Source_Server_Id";

    private const string NodeIdQuery = "SELECT @@server_id";
    private const string IsReaderQuery = "SELECT @@read_only";

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
                new MonitoringRdsHostListProvider(
                    props,
                    hostListProviderService,
                    TopologyQuery,
                    NodeIdQuery,
                    IsReaderQuery,
                    FetchWriterNodeQuery,
                    pluginService) :
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
