using MySqlConnector;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider.Tests
{
    public class AbortConnectionTests
    {
        [Fact]
        public async Task MysqlWrapperCommandCancelTest()
        {
            const string connectionString = "Server=global-ohio-mysql-instance-1.c12pgqavxipt.us-east-2.rds.amazonaws.com;User ID=admin;Password=my_password_2020;Initial Catalog=test;";

            using (AwsWrapperConnection<MySqlConnection> connection = new(connectionString))
            {
                connection.Open();
                AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
                command.CommandText = "select sleep(60)";

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
                    }),
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
                    })
                ]);
            }
        }

        [Fact]
        public async Task PgWrapperCommandCancelTest()
        {
            const string connectionString = "Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=postgres;";

            using (AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString))
            {
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
                    }),
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
                    })
                ]);
            }
        }
    }
}
