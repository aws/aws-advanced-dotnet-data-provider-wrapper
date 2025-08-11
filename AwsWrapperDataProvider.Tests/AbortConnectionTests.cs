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
using System.Diagnostics;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AbortConnectionTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public async Task MysqlWrapperCommandCancelTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);

        using AwsWrapperConnection<MySqlConnection> connection = new(connectionString);
        connection.Open();
        AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.CommandText = "select sleep(60)";

        var queryExecutionStopwatch = Stopwatch.StartNew();

        await Task.WhenAll([
            Task.Run(() =>
                {
                    try
                    {
                        var reader = command.ExecuteReader();

                        Console.WriteLine("Query executed.");
                        queryExecutionStopwatch.Stop();

                        while (reader.Read())
                        {
                            Console.WriteLine("Returned data: " + reader.GetInt64(0));
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Executing query error: " + ex);
                    }
                    finally
                    {
                        queryExecutionStopwatch.Stop();
                        Console.WriteLine("Query execution time: " + queryExecutionStopwatch.Elapsed.ToString());
                    }
                },
                TestContext.Current.CancellationToken),

                Task.Run(async () =>
                {
                    await Task.Delay(5000);
                    Console.WriteLine("Cancelling command...");
                    try
                    {
                        command.Cancel();
                        Console.WriteLine("Command cancelled");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error cancelling command: " + ex);
                    }
                },
                TestContext.Current.CancellationToken)
        ]);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public async Task PgWrapperCommandCancelTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        connection.Open();
        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = "select pg_sleep(60)";

        var queryExecutionStopwatch = Stopwatch.StartNew();

        await Task.WhenAll([
            Task.Run(() =>
                {
                    try
                    {
                        IDataReader reader = command.ExecuteReader();

                        Console.WriteLine("Query executed.");
                        queryExecutionStopwatch.Stop();

                        while (reader.Read())
                        {
                            Console.WriteLine("Returned data: " + reader.GetInt64(0));
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Executing query error: " + ex);
                    }
                    finally
                    {
                        queryExecutionStopwatch.Stop();
                        Console.WriteLine("Query execution time: " + queryExecutionStopwatch.Elapsed.ToString());
                    }
                },
                TestContext.Current.CancellationToken),

                Task.Run(async () =>
                {
                    await Task.Delay(5000);
                    Console.WriteLine("Cancelling command...");
                    try
                    {
                        command.Cancel();
                        Console.WriteLine("Command cancelled");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error cancelling command: " + ex);
                    }
                },
                TestContext.Current.CancellationToken)
        ]);
    }
}
