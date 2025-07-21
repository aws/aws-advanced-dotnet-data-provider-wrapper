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
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class FailoverConnectivityTests
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithStrictWriterMode()
    {
        // TODO: Replace with your actual Aurora PostgreSQL cluster endpoint
        const string clusterEndpoint = "atlas-postgres.cluster-cx422ywmsto6.us-east-2.rds.amazonaws.com";
        const string username = "pgadmin"; // Replace with your username
        const string password = "my_password_2020"; // Replace with your password
        const string database = "postgres"; // Replace with your database name

        // Build connection string as simple string - AWS wrapper will parse it properly
        var connectionString = $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
                              $"Plugins=failover;FailoverTimeoutMs=60000;FailoverMode=StrictWriter;EnableConnectFailover=true;" +
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
            var writerInfo = GetCurrentConnectionInfo(connection);
            Console.WriteLine($"   Current Writer: {writerInfo.Host}:{writerInfo.Port}");
            Console.WriteLine($"   Current Writer Role: {writerInfo.Role}");
            Console.WriteLine($"   Server Version: {writerInfo.Version}");

            Console.WriteLine("\n3. Starting long-running query (60 second wait)...");
            Console.WriteLine("   ⚠️  TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");
            Console.WriteLine("   (You have about 10 seconds before the query starts)");

            // Execute long-running query that should survive failover
            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Query started at: {startTime:HH:mm:ss}");

            string serverIp = "unknown";
            DateTime queryTime = DateTime.UtcNow;

            using (var command = connection.CreateCommand<NpgsqlCommand>())
            {
                command.CommandText =
                    "SELECT pg_sleep(500), now() as query_time, inet_server_addr()::text as server_ip";
                command.CommandTimeout = 500; // Allow extra time for failover

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            queryTime = reader.GetDateTime("query_time");
                            serverIp = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
                            Console.WriteLine($"   Initial Query: {queryTime:HH:mm:ss.fff} from {serverIp}");
                        }
                    }
                }
                catch (FailoverSuccessException)
                {
                    var newWriterInfo = GetCurrentConnectionInfo(connection);

                    Console.WriteLine("\n4. Verifying connection after potential failover...");
                    Console.WriteLine($"   Current Writer: {newWriterInfo.Host}:{newWriterInfo.Port}");

                    if (writerInfo.Host != newWriterInfo.Host || writerInfo.Port != newWriterInfo.Port)
                    {
                        Console.WriteLine("   ✓ FAILOVER DETECTED! Writer changed successfully.");
                        Console.WriteLine($"   Old Writer: {writerInfo.Host}:{writerInfo.Port}");
                        Console.WriteLine($"   Old Writer Role: {writerInfo.Role}");
                        Console.WriteLine($"   New Writer: {newWriterInfo.Host}:{newWriterInfo.Port}");
                        Console.WriteLine($"   New Writer Role: {newWriterInfo.Role}");
                    }
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
                    throw;
                }
            }

            // Ensure command and reader are fully disposed before continuing

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
                            Console.WriteLine($"   Post Failover Query {i}: {timestamp:HH:mm:ss.fff} from {testServerIp}");
                        }
                    }
                }
            }

            Console.WriteLine("\n✓ Failover test completed successfully!");
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

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithStrictReaderMode()
    {
        const string clusterEndpointRo = "atlas-postgres.cluster-cx422ywmsto6.us-east-2.rds.amazonaws.com";
        const string username = "pgadmin";
        const string password = "my_password_2020";
        const string database = "postgres";

        var connectionString =
            $"Host={clusterEndpointRo};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverTimeoutMs=60000;FailoverMode=StrictReader;EnableConnectFailover=true;" +
            $"FailoverReaderHostSelectorStrategy=random;";

        Console.WriteLine("=== Aurora PostgreSQL Failover Test: STRICT READER ===");
        Console.WriteLine($"Cluster Endpoint: {clusterEndpointRo}");
        Console.WriteLine($"Connection String: {connectionString}");

        using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine($"   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            var readerInfo = GetCurrentConnectionInfo(connection);
            Console.WriteLine($"\n2. Identifying current reader...");
            Console.WriteLine($"   Current Reader: {readerInfo.Host}:{readerInfo.Port}");
            Console.WriteLine($"   Current Reader Role: {readerInfo.Role}");
            Console.WriteLine($"   Server Version: {readerInfo.Version}");

            Console.WriteLine("\n3. Starting long-running query (60 second wait)...");
            Console.WriteLine("   ⚠️  TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");
            Console.WriteLine("   (You have about 10 seconds before the query starts)");

            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Query started at: {startTime:HH:mm:ss}");

            using (var command = connection.CreateCommand<NpgsqlCommand>())
            {
                command.CommandText =
                    "SELECT pg_sleep(500), now() as query_time, inet_server_addr()::text as server_ip";
                command.CommandTimeout = 500;

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var queryTime = reader.GetDateTime("query_time");
                            var serverIp = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
                            Console.WriteLine($"   Query: {queryTime:HH:mm:ss.fff} from {serverIp}");
                        }
                    }
                }
                catch (FailoverSuccessException)
                {
                    var newReaderInfo = GetCurrentConnectionInfo(connection);
                    Console.WriteLine("\n4. Verifying connection after failover...");
                    Console.WriteLine($"   New Reader: {newReaderInfo.Host}:{newReaderInfo.Port}");
                    Console.WriteLine($"   New Reader Role: {newReaderInfo.Role}");
                }
            }

            Console.WriteLine("\n✓ Strict Reader failover test completed successfully!");
        }
        finally
        {
            Console.WriteLine("\n5. Cleaning up connection...");
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
                Console.WriteLine("   Connection closed.");
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_ReadOnlyNode_WithStrictReaderMode()
    {
        const string clusterEndpointRo = "atlas-postgres.cluster-ro-cx422ywmsto6.us-east-2.rds.amazonaws.com";
        const string username = "pgadmin";
        const string password = "my_password_2020";
        const string database = "postgres";

        var connectionString =
            $"Host={clusterEndpointRo};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverTimeoutMs=60000;FailoverMode=StrictReader;EnableConnectFailover=true;" +
            $"FailoverReaderHostSelectorStrategy=random;";

        Console.WriteLine("=== Aurora PostgreSQL Failover Test: STRICT READER ===");
        Console.WriteLine($"Cluster Endpoint: {clusterEndpointRo}");
        Console.WriteLine($"Connection String: {connectionString}");

        using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine($"   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            var readerInfo = GetCurrentConnectionInfo(connection);
            Console.WriteLine($"\n2. Identifying current reader...");
            Console.WriteLine($"   Current Reader: {readerInfo.Host}:{readerInfo.Port}");
            Console.WriteLine($"   Current Reader Role: {readerInfo.Role}");
            Console.WriteLine($"   Server Version: {readerInfo.Version}");

            Console.WriteLine("\n3. Starting long-running query (60 second wait)...");
            Console.WriteLine("   ⚠️  TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");
            Console.WriteLine("   (You have about 10 seconds before the query starts)");

            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Query started at: {startTime:HH:mm:ss}");

            using (var command = connection.CreateCommand<NpgsqlCommand>())
            {
                command.CommandText =
                    "SELECT pg_sleep(500), now() as query_time, inet_server_addr()::text as server_ip";
                command.CommandTimeout = 500;

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var queryTime = reader.GetDateTime("query_time");
                            var serverIp = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
                            Console.WriteLine($"   Query: {queryTime:HH:mm:ss.fff} from {serverIp}");
                        }
                    }
                }
                catch (FailoverSuccessException)
                {
                    var newReaderInfo = GetCurrentConnectionInfo(connection);
                    Console.WriteLine("\n4. Verifying connection after failover...");
                    Console.WriteLine($"   New Reader: {newReaderInfo.Host}:{newReaderInfo.Port}");
                    Console.WriteLine($"   New Reader Role: {newReaderInfo.Role}");
                }
            }

            Console.WriteLine("\n✓ Strict Reader failover test completed successfully!");
        }
        finally
        {
            Console.WriteLine("\n5. Cleaning up connection...");
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
                Console.WriteLine("   Connection closed.");
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithReaderOrWriterMode()
    {
        const string clusterEndpointRo = "atlas-postgres.cluster-cx422ywmsto6.us-east-2.rds.amazonaws.com";
        const string username = "pgadmin";
        const string password = "my_password_2020";
        const string database = "postgres";

        var connectionString =
            $"Host={clusterEndpointRo};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverTimeoutMs=60000;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;" +
            $"FailoverReaderHostSelectorStrategy=random;";

        Console.WriteLine("=== Aurora PostgreSQL Failover Test: STRICT READER ===");
        Console.WriteLine($"Cluster Endpoint: {clusterEndpointRo}");
        Console.WriteLine($"Connection String: {connectionString}");

        using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine($"   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            var readerInfo = GetCurrentConnectionInfo(connection);
            Console.WriteLine($"\n2. Identifying current reader...");
            Console.WriteLine($"   Current Reader: {readerInfo.Host}:{readerInfo.Port}");
            Console.WriteLine($"   Current Reader Role: {readerInfo.Role}");
            Console.WriteLine($"   Server Version: {readerInfo.Version}");

            Console.WriteLine("\n3. Starting long-running query (60 second wait)...");
            Console.WriteLine("   ⚠️  TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");
            Console.WriteLine("   (You have about 10 seconds before the query starts)");

            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Query started at: {startTime:HH:mm:ss}");

            using (var command = connection.CreateCommand<NpgsqlCommand>())
            {
                command.CommandText =
                    "SELECT pg_sleep(500), now() as query_time, inet_server_addr()::text as server_ip";
                command.CommandTimeout = 500;

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var queryTime = reader.GetDateTime("query_time");
                            var serverIp = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
                            Console.WriteLine($"   Query: {queryTime:HH:mm:ss.fff} from {serverIp}");
                        }
                    }
                }
                catch (FailoverSuccessException)
                {
                    var newReaderInfo = GetCurrentConnectionInfo(connection);
                    Console.WriteLine("\n4. Verifying connection after failover...");
                    Console.WriteLine($"   New Reader: {newReaderInfo.Host}:{newReaderInfo.Port}");
                    Console.WriteLine($"   New Reader Role: {newReaderInfo.Role}");
                }
            }

            Console.WriteLine("\n✓ Strict Reader failover test completed successfully!");
        }
        finally
        {
            Console.WriteLine("\n5. Cleaning up connection...");
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
                Console.WriteLine("   Connection closed.");
            }
        }
    }

    private static (string Host, int Port, string Version, string Role) GetCurrentConnectionInfo(AwsWrapperConnection<NpgsqlConnection> connection)
    {
        using var command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = @"
        SELECT 
            inet_server_addr()::text as server_ip,
            inet_server_port() as server_port,
            version() as server_version,
            CASE WHEN pg_is_in_recovery() THEN 'reader' ELSE 'writer' END as node_role;";

        using var reader = command.ExecuteReader();
        if (reader.Read())
        {
            var host = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
            var port = reader.IsDBNull("server_port") ? 5432 : reader.GetInt32("server_port");
            var version = reader.IsDBNull("server_version") ? "unknown" : reader.GetString("server_version");
            var role = reader.IsDBNull("node_role") ? "unknown" : reader.GetString("node_role");

            return (host, port, version, role);
        }

        return ("unknown", 5432, "unknown", "unknown");
    }
}
