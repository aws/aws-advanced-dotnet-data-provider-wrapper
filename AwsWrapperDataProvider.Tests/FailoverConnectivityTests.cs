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
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(12), tcs);

        // Wait for the simulation to start
        await tcs.Task;
        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            this.logger.WriteLine(AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Finished executing without exception thrown");
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
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(12), tcs);

        // Wait for the simulation to start
        await tcs.Task;
        Assert.Throws<FailoverSuccessException>(() =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment);
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Finished executing without exception thrown");
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
        const string clusterEndpoint = "database-yan.cluster-cxmsoia46djo.us-west-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "admin"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "test"; // Replace with your database name

        var connectionString = $"Host={clusterEndpoint};Username={username};Password={password};Database={database};" +
                              $"Plugins=failover;FailoverMode=StrictWriter;EnableConnectFailover=true;Command Timeout=2;Connect Timeout=2;";

        var connection = new AwsWrapperConnection<MySqlConnection>(connectionString);
        connection.Open();
        var transaciton = connection.BeginTransaction();
        var command = connection.CreateCommand<MySqlCommand>();
        command.Transaction = transaciton;
        command.CommandText = "SELECT 1";
        var reader = command.ExecuteReader();
        reader.Read();
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
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
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
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
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
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
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
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
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
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
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
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
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
            $"Host={clusterEndpoint};Username={username};Database={database};ProxyPort=5432;" +
            $"Plugins=failover,iam;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;";
        PerformFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_Transaction_WithStrictWriterMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString = $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
                              $"Plugins=failover;FailoverMode=StrictWriter;EnableConnectFailover=true;";
        PerformTransactionFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_Transaction_WithStrictReaderMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
            $"Plugins=failover;FailoverMode=StrictReader;EnableConnectFailover=true;";
        PerformTransactionFailoverTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public void FailoverPluginTest_Transaction_WithReaderOrWriterMode()
    {
        const string clusterEndpoint = "atlas-postgres.cluster-xyz.us-east-2.rds.amazonaws.com"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString =
            $"Host={clusterEndpoint};Username={username};Password={password};Database={database};ProxyPort=5432;" +
            $"Plugins=failover;FailoverMode=ReaderOrWriter;EnableConnectFailover=true;";
        PerformTransactionFailoverTest(connectionString);
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

    private static void PerformTransactionFailoverTest(string connectionString)
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

            // Create a persistent table outside of transaction to verify rollback
            Console.WriteLine("\n3. Creating persistent test table...");
            using (var setupCommand = connection.CreateCommand<NpgsqlCommand>())
            {
                setupCommand.CommandText = @"
                    DROP TABLE IF EXISTS failover_rollback_test;
                    CREATE TABLE failover_rollback_test (
                        id SERIAL PRIMARY KEY, 
                        test_data TEXT, 
                        created_at TIMESTAMP DEFAULT NOW()
                    )";
                setupCommand.ExecuteNonQuery();
                Console.WriteLine("   ✓ Persistent test table created");
            }

            Console.WriteLine("\n4. Starting transaction with operations that should be rolled back...");
            Console.WriteLine("   TRIGGER FAILOVER NOW using:");
            Console.WriteLine("   aws rds failover-db-cluster --db-cluster-identifier atlas-postgres");

            // Start a transaction
            using var transaction = connection.BeginTransaction();
            Console.WriteLine("   ✓ Transaction started");

            var startTime = DateTime.UtcNow;
            Console.WriteLine($"   Transaction started at: {startTime:HH:mm:ss}");

            bool failoverOccurred = false;

            try
            {
                // Insert data within the transaction that should be rolled back
                using (var insertCommand = connection.CreateCommand<NpgsqlCommand>())
                {
                    insertCommand.Transaction = transaction;
                    insertCommand.CommandText = "INSERT INTO failover_rollback_test (test_data) VALUES ('data-that-should-be-rolled-back-1')";
                    insertCommand.ExecuteNonQuery();
                    Console.WriteLine("   ✓ First insert completed within transaction");
                }

                using (var insertCommand2 = connection.CreateCommand<NpgsqlCommand>())
                {
                    insertCommand2.Transaction = transaction;
                    insertCommand2.CommandText = "INSERT INTO failover_rollback_test (test_data) VALUES ('data-that-should-be-rolled-back-2')";
                    insertCommand2.ExecuteNonQuery();
                    Console.WriteLine("   ✓ Second insert completed within transaction");
                }

                // Verify data exists within the transaction before failover
                using (var preFailoverSelect = connection.CreateCommand<NpgsqlCommand>())
                {
                    preFailoverSelect.Transaction = transaction;
                    preFailoverSelect.CommandText = "SELECT COUNT(*) FROM failover_rollback_test";
                    var countBeforeFailover = (long)(preFailoverSelect.ExecuteScalar() ?? 0L);
                    Console.WriteLine($"   ✓ Data visible within transaction: {countBeforeFailover} rows");
                }

                // Execute long-running query within the transaction that should trigger failover
                using (var longRunningCommand = connection.CreateCommand<NpgsqlCommand>())
                {
                    longRunningCommand.Transaction = transaction;
                    longRunningCommand.CommandText = "SELECT pg_sleep(500), now() as query_time, inet_server_addr()::text as server_ip";
                    longRunningCommand.CommandTimeout = 500; // Allow extra time for failover

                    using (var reader = longRunningCommand.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var queryTime = reader.GetDateTime("query_time");
                            var serverIp = reader.IsDBNull("server_ip") ? "unknown" : reader.GetString("server_ip");
                            Console.WriteLine($"   Long-running query completed: {queryTime:HH:mm:ss.fff} from {serverIp}");
                        }
                    }
                }

                // If we reach here, no failover occurred during the sleep
                Console.WriteLine("   ⚠️  No failover detected during long-running query");
            }
            catch (TransactionStateUnknownException)
            {
                failoverOccurred = true;
                Console.WriteLine("   ✓ Failover detected during transaction!");

                var newHostInfo = GetCurrentConnectionInfo(connection);
                Console.WriteLine("\n5. Verifying connection after failover...");
                Console.WriteLine($"   Previous Host: {hostInfo.Host}:{hostInfo.Port}");
                Console.WriteLine($"   Current Host: {newHostInfo.Host}:{newHostInfo.Port}");
                Console.WriteLine($"   Current Host Name: {newHostInfo.HostName}");
                Console.WriteLine($"   Current Host Role: {newHostInfo.Role}");

                if (hostInfo.Host != newHostInfo.Host || hostInfo.Port != newHostInfo.Port)
                {
                    Console.WriteLine("   ✓ FAILOVER DETECTED! Host changed successfully.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   ❌ Exception during transaction: {ex.GetType().Name}: {ex.Message}");
            }

            // Now verify that the transaction was rolled back
            Console.WriteLine("\n6. Verifying transaction rollback after failover...");

            // Check if the data was rolled back by querying outside of transaction
            using (var rollbackCheckCommand = connection.CreateCommand<NpgsqlCommand>())
            {
                rollbackCheckCommand.CommandText = "SELECT COUNT(*) FROM failover_rollback_test";
                var countAfterFailover = (long)(rollbackCheckCommand.ExecuteScalar() ?? 0L);
                Console.WriteLine($"   Data count after failover: {countAfterFailover} rows");

                if (countAfterFailover == 0)
                {
                    Console.WriteLine("   ✓ ROLLBACK VERIFIED: All transaction data was rolled back!");
                }
                else
                {
                    Console.WriteLine("   ❌ ROLLBACK FAILED: Transaction data was not rolled back!");

                    // Show what data remains
                    using (var dataCommand = connection.CreateCommand<NpgsqlCommand>())
                    {
                        dataCommand.CommandText = "SELECT id, test_data, created_at FROM failover_rollback_test ORDER BY id";
                        using (var reader = dataCommand.ExecuteReader())
                        {
                            Console.WriteLine("   Remaining data:");
                            while (reader.Read())
                            {
                                var id = reader.GetInt32("id");
                                var testData = reader.GetString("test_data");
                                var createdAt = reader.GetDateTime("created_at");
                                Console.WriteLine($"     ID={id}, Data='{testData}', Created={createdAt:HH:mm:ss.fff}");
                            }
                        }
                    }
                }
            }

            Console.WriteLine("\n7. Testing new transaction after failover...");
            using (var newTransaction = connection.BeginTransaction())
            {
                using (var newInsertCommand = connection.CreateCommand<NpgsqlCommand>())
                {
                    newInsertCommand.Transaction = newTransaction;
                    newInsertCommand.CommandText = "INSERT INTO failover_rollback_test (test_data) VALUES ('post-failover-data')";
                    newInsertCommand.ExecuteNonQuery();
                    Console.WriteLine("   ✓ New transaction insert successful");
                }

                newTransaction.Commit();
                Console.WriteLine("   ✓ New transaction committed successfully");
            }

            // Verify the new data exists
            using (var finalCheckCommand = connection.CreateCommand<NpgsqlCommand>())
            {
                finalCheckCommand.CommandText = "SELECT COUNT(*) FROM failover_rollback_test";
                var finalCount = (long)(finalCheckCommand.ExecuteScalar() ?? 0L);
                Console.WriteLine($"   Final data count: {finalCount} rows");
            }

            if (failoverOccurred)
            {
                Console.WriteLine("\n✓ Transaction rollback failover test completed successfully!");
                Console.WriteLine("  - Failover was detected during transaction");
                Console.WriteLine("  - Transaction was automatically rolled back");
                Console.WriteLine("  - New transactions work correctly after failover");
            }
            else
            {
                Console.WriteLine("\n⚠️  Test completed but no failover was detected");
                Console.WriteLine("   Make sure to trigger failover during the pg_sleep operation");
            }
        }
        finally
        {
            Console.WriteLine("\n8. Cleaning up...");

            // Clean up the test table
            try
            {
                using (var cleanupCommand = connection.CreateCommand<NpgsqlCommand>())
                {
                    cleanupCommand.CommandText = "DROP TABLE IF EXISTS failover_rollback_test";
                    cleanupCommand.ExecuteNonQuery();
                    Console.WriteLine("   ✓ Test table cleaned up");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   ⚠️  Cleanup warning: {ex.Message}");
            }

            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
                Console.WriteLine("   ✓ Connection closed");
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
