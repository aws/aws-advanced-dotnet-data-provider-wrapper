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

namespace AwsWrapperDataProvider.Tests;

public class FailoverConnectivityTests
{
    [Fact]
    [Trait("Category", "Integration")]
    public void MySqlClientWrapperConnectionTest()
    {
        const string connectionString = "Server=127.0.0.1;User ID=root;Password=password;Initial Catalog=mysql;";
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
    }

    [Fact]
    [Trait("Category", "Manual")]
    public void AuroraPostgreSqlFailoverTest()
    {
        // TODO: Replace with your actual Aurora PostgreSQL cluster endpoint
        const string clusterEndpoint = "atlas-postgres.cluster-cx422ywmsto6.us-east-2.rds.amazonaws.com";
        const string username = "pgadmin"; // Replace with your username
        const string password = "my_password_2020"; // Replace with your password
        const string database = "postgres"; // Replace with your database name

        // Build connection string as simple string - AWS wrapper will parse it properly
        var connectionString = $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
                              $"Plugins=failover;FailoverTimeoutMs=60000;FailoverMode=strict-writer;EnableConnectFailover=true;" +
                              $"FailoverReaderHostSelectorStrategy=random;";

        Console.WriteLine("=== Aurora PostgreSQL Failover Test ===");
        Console.WriteLine($"Cluster Endpoint: {clusterEndpoint}");
        Console.WriteLine($"Connection String: {connectionString}");
        Console.WriteLine();

        using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine($"   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            // Get initial writer information
            Console.WriteLine("\n2. Identifying current writer...");
            var writerInfo = GetCurrentWriterInfo(connection);
            Console.WriteLine($"   Current Writer: {writerInfo.Host}:{writerInfo.Port}");
            Console.WriteLine($"   Server Version: {writerInfo.Version}");

            Console.WriteLine("\n3. Starting long-running query (60 second wait)...");
            Console.WriteLine("   ⚠️  TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");
            Console.WriteLine("   (You have about 10 seconds before the query starts)");

            // Wait a moment for user to see the message
            Thread.Sleep(10000);

            // Execute long-running query that should survive failover
            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Query started at: {startTime:HH:mm:ss}");

            string serverIp = "unknown";
            DateTime queryTime = DateTime.UtcNow;

            using (var command = connection.CreateCommand<NpgsqlCommand>())
            {
                command.CommandText = "SELECT pg_sleep(500), now() as query_time, inet_server_addr()::text as server_ip";
                command.CommandTimeout = 500; // Allow extra time for failover

                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        queryTime = reader.GetDateTime("query_time");
                        serverIp = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
                    }
                }
            } // Ensure command and reader are fully disposed before continuing

            var endTime = DateTime.UtcNow;
            Console.WriteLine($"   Query completed at: {endTime:HH:mm:ss}");
            Console.WriteLine($"   Total duration: {(endTime - startTime).TotalSeconds:F1} seconds");
            Console.WriteLine($"   Server reported time: {queryTime:HH:mm:ss}");
            Console.WriteLine($"   Server IP: {serverIp}");

            Console.WriteLine("\n4. Verifying connection after potential failover...");
            var newWriterInfo = GetCurrentWriterInfo(connection);
            Console.WriteLine($"   Current Writer: {newWriterInfo.Host}:{newWriterInfo.Port}");

            if (writerInfo.Host != newWriterInfo.Host || writerInfo.Port != newWriterInfo.Port)
            {
                Console.WriteLine("   ✓ FAILOVER DETECTED! Writer changed successfully.");
                Console.WriteLine($"   Old Writer: {writerInfo.Host}:{writerInfo.Port}");
                Console.WriteLine($"   New Writer: {newWriterInfo.Host}:{newWriterInfo.Port}");
            }
            else
            {
                Console.WriteLine("   ℹ️  No failover detected or same instance became writer again.");
            }

            Console.WriteLine("\n5. Testing additional queries after failover...");
            for (int i = 1; i <= 3; i++)
            {
                using (var testCommand = connection.CreateCommand<NpgsqlCommand>())
                {
                    testCommand.CommandText = "SELECT current_timestamp, inet_server_addr()::text";
                    using (var testReader = testCommand.ExecuteReader())
                    {
                        if (testReader.Read())
                        {
                            var timestamp = testReader.GetDateTime(0);
                            var testServerIp = testReader.IsDBNull(1) ? "unknown" : testReader.GetString(1);
                            Console.WriteLine($"   Query {i}: {timestamp:HH:mm:ss.fff} from {testServerIp}");
                        }
                    }
                }
            }

            Console.WriteLine("\n✓ Failover test completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Test failed with exception:");
            Console.WriteLine($"   Type: {ex.GetType().Name}");
            Console.WriteLine($"   Message: {ex.Message}");

            if (ex.InnerException != null)
            {
                Console.WriteLine($"   Inner Exception: {ex.InnerException.GetType().Name}");
                Console.WriteLine($"   Inner Message: {ex.InnerException.Message}");
            }

            Console.WriteLine($"\n   Full Stack Trace:");
            Console.WriteLine(ex.ToString());

            throw; // Re-throw to fail the test
        }
        finally
        {
            Console.WriteLine("\n6. Cleaning up connection...");
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
                Console.WriteLine("   Connection closed.");
            }
        }
    }

    private static (string Host, int Port, string Version) GetCurrentWriterInfo(AwsWrapperConnection<NpgsqlConnection> connection)
    {
        using var command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = @"
            SELECT 
                inet_server_addr()::text as server_ip,
                inet_server_port() as server_port,
                version() as server_version";

        using var reader = command.ExecuteReader();
        if (reader.Read())
        {
            var host = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
            var port = reader.IsDBNull("server_port") ? 5432 : reader.GetInt32("server_port");
            var version = reader.IsDBNull("server_version") ? "unknown" : reader.GetString("server_version");

            return (host, port, version);
        }

        return ("unknown", 5432, "unknown");
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void AuroraPostgreSqlReaderFailoverTest()
    {
        // TODO: Replace with your actual Aurora PostgreSQL cluster reader endpoint
        const string readerEndpoint = "atlas-postgres.cluster-ro-xxxxxxxxxx.us-east-1.rds.amazonaws.com";
        const string username = "postgres"; // Replace with your username
        const string password = "your-password"; // Replace with your password
        const string database = "postgres"; // Replace with your database name

        // Build connection string as simple string for reader failover
        var connectionString = $"Host={readerEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
                              $"Plugins=failover;FailoverTimeoutMs=60000;FailoverMode=strict-reader;EnableConnectFailover=true;" +
                              $"FailoverReaderHostSelectorStrategy=random;CommandTimeout=30;Timeout=15";

        Console.WriteLine("=== Aurora PostgreSQL Reader Failover Test ===");
        Console.WriteLine($"Reader Endpoint: {readerEndpoint}");
        Console.WriteLine();

        using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            Console.WriteLine("1. Opening connection to reader...");
            connection.Open();
            Console.WriteLine($"   ✓ Connected successfully");

            // Test read-only queries
            Console.WriteLine("\n2. Testing read-only queries...");
            for (int i = 1; i <= 5; i++)
            {
                using (var command = connection.CreateCommand<NpgsqlCommand>())
                {
                    command.CommandText = "SELECT current_timestamp, inet_server_addr()::text, pg_is_in_recovery()";
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var timestamp = reader.GetDateTime(0);
                            var readerServerIp = reader.IsDBNull(1) ? "unknown" : reader.GetString(1);
                            var isReplica = reader.GetBoolean(2);

                            Console.WriteLine($"   Query {i}: {timestamp:HH:mm:ss.fff} from {readerServerIp} (replica: {isReplica})");
                        }
                    }
                }

                // Small delay between queries
                Thread.Sleep(2000);
            }

            Console.WriteLine("\n✓ Reader failover test completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Reader test failed: {ex.Message}");
            throw;
        }
    }
}
