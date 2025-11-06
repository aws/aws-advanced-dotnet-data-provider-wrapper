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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

namespace AwsWrapperDataProviderExample;

public static class MySqlFederatedAuthentication
{
    public static async Task Main(string[] args)
    {
        ConnectionPluginChainBuilder.RegisterPluginFactory<FederatedAuthPluginFactory>(PluginCodes.FederatedAuth);

        const string connectionString =
            "Server=<insert_rds_instance_here>;" +
            "Initial Catalog=mysql" +
            ";Database=<database_name_here>;" +
            "DbUser=<db_user_with_iam_login>;" +
            "Plugins=federatedAuth;" +
            "IamRegion=<iam_region>;" +
            "IamRoleArn=<iam_role_arn>;" +
            "IamIdpArn=<iam_idp_arn>;" +
            "IdpEndpoint=<idp_endpoint>;" +
            "IdpUsername=<idp_username>;" +
            "IdpPassword=<idp_password>;";
        const string query = "select * from test";

        using (AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection =
               new(connectionString))
        {
            AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
            command.CommandText = query;

            try
            {
                connection.Open();
                IDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetInt32(0));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        await Task.CompletedTask;
    }
}
