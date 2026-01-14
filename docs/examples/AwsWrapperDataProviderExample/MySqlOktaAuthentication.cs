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
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.MySqlClient;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;
using Npgsql;

namespace AwsWrapperDataProviderExample;

public static class MySqlOktaAuthentication
{
    public static async Task Main(string[] args)
    {
        // Load relevant DbConnection dialect
        MySqlClientDialectLoader.Load();

        ConnectionPluginChainBuilder.RegisterPluginFactory<OktaAuthPluginFactory>(PluginCodes.Okta);

        const string connectionString =
            "Server=<host>;Database=<database>;DbUser=<db user>;Plugins=okta;IamRoleArn=<iam role arn>;IamIdpArn=<iam saml arn>;IdpEndpoint=<idp endpoint>;IdpPort=<idp port>;IdpUsername=<username>;IdpPassword=<password>;AppId=<app id>;";
        const string query = "select aurora_db_instance_identifier()";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        try
        {
            connection.Open();
            IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }

        await Task.CompletedTask;
    }
}
