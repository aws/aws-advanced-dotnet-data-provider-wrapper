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
using AwsWrapperDataProvider.Driver.Plugins.Failover;

namespace AwsWrapperDataProviderExample;

public static class MySqlFailover
{
    private static void ExecuteQuery(AwsWrapperConnection connection)
    {
        using AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command =
            connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
        command.CommandText = "SELECT 1 AS result;";
        using IDataReader reader = command.ExecuteReader();
        if (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
        }
    }

    public static async Task Main(string[] args)
    {
        const string connectionString =
            "Server=<host>;Database=<db_name>;User Id=<user>;Password=<password>;Plugins=failover;";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);
        connection.Open();
        try
        {
            ExecuteQuery(connection);
        }
        catch (FailoverSuccessException)
        {
            // Query execution failed and AWS Advanced Python Driver successfully failed over to an available instance.
            // The old AwsWrapperCommand is no longer reusable and the application needs to be reconfigured.
            // Retry Query
            ExecuteQuery(connection);
        }
        catch (FailoverFailedException)
        {
            // User application should open a new connection, check the results of the failed transaction and re-run it if needed.
            throw;
        }
        catch (TransactionStateUnknownException)
        {
            // User application should check the status of the failed transaction and restart it if needed.
            throw;
        }

        await Task.CompletedTask;
    }
}
