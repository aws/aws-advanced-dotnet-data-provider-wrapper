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
using Npgsql;

namespace AwsWrapperDataProviderExample;

public static class LimitlessPostgresql
{
    public static async Task Main(string[] args)
    {
        // Configure the connection string with the DB shard group endpoint
        // Replace placeholders with your actual database information
        const string connectionString =
            "Host=db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com;" +
            "Database=test;" +
            "Port=5432;" +
            "Username=user;" +
            "Password=password;" +
            "Plugins=limitless;";

        // Optional: Configure Limitless Connection Plugin properties
        // You can add these to the connection string:
        // "LimitlessTransactionRouterMonitorIntervalMs=30000;" +
        // "LimitlessWaitForTransactionRouterInfo=false;" +
        // "LimitlessConnectMaxRetries=13;"

        const string query = "SELECT * FROM aurora_db_instance_identifier()";

        try
        {
            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
            using AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = query;

            connection.Open();
            using IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex}");
        }

        await Task.CompletedTask;
    }
}
