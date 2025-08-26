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
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class FailoverConnectivityTests : IntegrationTestBase
{
    private readonly ITestOutputHelper logger;

    protected override bool MakeSureFirstInstanceWriter => true;

    public FailoverConnectivityTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    /// <summary>
    /// Current writer dies, driver failover occurs when executing a method against the connection.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    public async Task WriterFailover_FailOnConnectionInvocation()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            initialWriterInstanceInfo.Host,
            initialWriterInstanceInfo.Port,
            Username,
            Password,
            ProxyDatabaseInfo.DefaultDbName,
            2,
            10,
            "failover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        await AuroraUtils.CrashInstance(currentWriter);

        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine("Executing instance ID query to trigger failover...");
            AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        });
    }

    /// <summary>
    /// Current reader dies, no other reader instance, failover to writer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    public async Task FailFromReaderToWriter()
    {
        Assert.SkipWhen(NumberOfInstances != 2, "Skipped due to test requiring number of database instances = 2.");

        // Connect to the only available reader instance
        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var readerInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances[1];

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            readerInstanceInfo.Host,
            readerInstanceInfo.Port,
            Username,
            Password,
            ProxyDatabaseInfo.DefaultDbName,
            2,
            10,
            "failover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        await ProxyHelper.DisableConnectivityAsync(readerInstanceInfo.InstanceId);

        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine("Executing instance ID query to trigger failover...");
            AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        });

        // Assert that we are currently connected to the writer instance.
        var currentConnectionId = AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        Assert.Equal(currentWriter, currentConnectionId);
        Assert.True(await AuroraUtils.IsDBInstanceWriterAsync(currentConnectionId));
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    public async Task WriterFailover_WriterReelected()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            initialWriterInstanceInfo.Host,
            initialWriterInstanceInfo.Port,
            Username,
            Password,
            ProxyDatabaseInfo.DefaultDbName,
            2,
            10,
            "failover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(15), tcs);

        // Wait for the simulation to start
        await tcs.Task;
        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} Executing instance ID query to trigger failover...");
            this.logger.WriteLine(AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} Finished executing without exception thrown");
        });

        // Assert that we are currently connected to the writer instance.
        var currentConnectionId = AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        Assert.Equal(currentWriter, currentConnectionId);
        Assert.True(await AuroraUtils.IsDBInstanceWriterAsync(currentConnectionId));
        await simulationTask;
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    public async Task ReaderFailover_ReaderOrWriter()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            initialWriterInstanceInfo.Host,
            initialWriterInstanceInfo.Port,
            Username,
            Password,
            ProxyDatabaseInfo.DefaultDbName,
            2,
            10,
            "failover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}" +
            $"; FailoverMode=ReaderOrWriter";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        await ProxyHelper.DisableConnectivityAsync(currentWriter);

        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine("Executing instance ID query to trigger failover...");
            AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        });
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    public async Task ReaderFailover_StrictReader()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            initialWriterInstanceInfo.Host,
            initialWriterInstanceInfo.Port,
            Username,
            Password,
            ProxyDatabaseInfo.DefaultDbName,
            2,
            10,
            "failover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}" +
            $"; FailoverMode=StrictReader";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        await AuroraUtils.CrashInstance(currentWriter);

        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine("Executing instance ID query to trigger failover...");
            AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        });

        // Assert that we are currently connected to the reader instance.
        var currentConnectionId = AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
        Assert.False(await AuroraUtils.IsDBInstanceWriterAsync(currentConnectionId));
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    public async Task ReaderFailover_WriterReelected()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            initialWriterInstanceInfo.Host,
            initialWriterInstanceInfo.Port,
            Username,
            Password,
            ProxyDatabaseInfo.DefaultDbName,
            2,
            10,
            "failover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}" +
            $"; FailoverMode=ReaderOrWriter";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(20), tcs);

        // Wait for the simulation to start
        await tcs.Task;
        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} Executing instance ID query to trigger failover...");
            AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} Finished executing without exception thrown");
        });
        await simulationTask;
    }
}

public class ManualFailoverConnectivityTests
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithStrictWriterMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString = $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
                              $"Plugins=failover;FailoverMode=StrictWriter;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithStrictReaderMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverMode=StrictReader;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_ReadOnlyNode_WithStrictReaderMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverMode=StrictReader;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithReaderOrWriterMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithStrictWriterMode_WithRoundRobinHostSelectorStrategy()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;" +
            $"FailoverReaderHostSelectorStrategy=RoundRobin;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithStrictWriterMode_WithHighestWeightHostSelectorStrategy()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;" +
            $"FailoverReaderHostSelectorStrategy=HighestWeight;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithAuroraInitialConnectionStrategyPlugin()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;" +
            $"Plugins=failover,initialConnection;FailoverMode=StrictReader;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_WithIamAuth()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Database={database};Port=5432;" +
            $"Plugins=failover,iam;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    private static void PerformFailoverTest(string connectionString)
    {
        using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine($"   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            // Get initial writer information
            Console.WriteLine("\n2. Identifying current host...");
            var hostInfo = GetCurrentConnectionInfo(connection);
            Console.WriteLine($"   Current Host: {hostInfo.Host}:{hostInfo.Port}");
            Console.WriteLine($"   Current Host Name: {hostInfo.HostName}");
            Console.WriteLine($"   Current Host Role: {hostInfo.Role}");
            Console.WriteLine($"   Server Version: {hostInfo.Version}");

            Console.WriteLine("\n3. Starting long-running query (60 second wait)...");
            Console.WriteLine("   TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");

            // Execute long-running query that should survive failover
            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Query started at: {startTime:HH:mm:ss}");

            string serverIp;
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
                    var newHostInfo = GetCurrentConnectionInfo(connection);

                    Console.WriteLine("\n4. Verifying connection after potential failover...");
                    Console.WriteLine($"   Current Host: {hostInfo.Host}:{hostInfo.Port}");
                    Console.WriteLine($"   Current Host Name: {hostInfo.HostName}");
                    Console.WriteLine($"   Current Host Role: {hostInfo.Role}");

                    if (hostInfo.Host != newHostInfo.Host || hostInfo.Port != newHostInfo.Port)
                    {
                        Console.WriteLine("   ✓ FAILOVER DETECTED! Host changed successfully.");
                        Console.WriteLine($"   New Host: {newHostInfo.Host}:{newHostInfo.Port}");
                        Console.WriteLine($"   Current Host Name: {newHostInfo.HostName}");
                        Console.WriteLine($"   New Host Role: {newHostInfo.Role}");
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

    private static (string HostName, string Host, int Port, string Version, string Role) GetCurrentConnectionInfo(AwsWrapperConnection<NpgsqlConnection> connection)
    {
        using var command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = @"
        SELECT
            aurora_db_instance_identifier()::text as server_name,
            inet_server_addr()::text as server_ip,
            inet_server_port() as server_port,
            version() as server_version,
            CASE WHEN pg_is_in_recovery() THEN 'reader' ELSE 'writer' END as node_role;";

        using var reader = command.ExecuteReader();
        if (reader.Read())
        {
            var hostName = reader.IsDBNull("server_name") ? "unknow" : reader.GetString("server_name");
            var host = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
            var port = reader.IsDBNull("server_port") ? 5432 : reader.GetInt32("server_port");
            var version = reader.IsDBNull("server_version") ? "unknown" : reader.GetString("server_version");
            var role = reader.IsDBNull("node_role") ? "unknown" : reader.GetString("node_role");

            return (hostName, host, port, version, role);
        }

        return ("unknown", "unknown", 5432, "unknown", "unknown");
    }
}
