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

using Npgsql;

namespace AwsWrapperDataProvider.Tests
{
    public class PluginConnectivityTests
    {
        [Fact]
        [Trait("Category", "Integration")]
        public void PgWrapperIamConnectionTest()
        {
            const string connectionString =
                "Host=<insert_rds_instance_here>;Username=<db_user_with_iam_login>;Database=<database_name_here>;Plugins=iam;";
            const string query = "select aurora_db_instance_identifier()";

            using (AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString))
            {
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
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        public void PgWrapperSecretsManagerConnectionTest()
        {
            const string connectionString =
                "Host=<insert_rds_instance_here>;Database=<database_name_here>;Plugins=awsSecretsManager;secretsManagerSecretId=<secret_name_or_arn>;secretsManagerRegion=<optional_secret_region>;";
            const string query = "select aurora_db_instance_identifier()";

            using (AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString))
            {
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
            }
        }
    }
}
