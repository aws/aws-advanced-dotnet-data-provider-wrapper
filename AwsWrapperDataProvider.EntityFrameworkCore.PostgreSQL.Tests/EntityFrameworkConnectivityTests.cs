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

using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: CaptureConsole]

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL.Tests;

public class EntityFrameworkConnectivityTests : IntegrationTestBase
{
    protected override bool MakeSureFirstInstanceWriter => true;

    private readonly ITestOutputHelper logger;
    private readonly ILoggerFactory loggerFactory;

    public EntityFrameworkConnectivityTests(ITestOutputHelper output)
    {
        this.logger = output;

        this.loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Trace)
                .AddDebug()
                .AddConsole(options => options.FormatterName = "simple");

            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.UseUtcTimestamp = true;
                options.ColorBehavior = LoggerColorBehavior.Enabled;
            });
        });
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public void PgEFAddTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
        var wrapperConnectionString = connectionString + $";Plugins=initialConnection,failover;";
        if (Deployment != DatabaseEngineDeployment.AURORA && Deployment != DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
        {
            wrapperConnectionString = connectionString + $";Plugins=initialConnection,failover;";
        }

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder.UseNpgsql(connectionString))
            .LogTo(Console.WriteLine)
            .Options;

        using (var db = new PersonDbContext(options))
        {
            db.Database.ExecuteSqlRaw($"Truncate table persons;");
        }

        using (var db = new PersonDbContext(options))
        {
            Person person = new() { FirstName = "Jane", LastName = "Smith" };
            db.Add(person);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            foreach (Person p in db.Persons.Where(x => x.FirstName != null && x.FirstName.StartsWith("J")))
            {
                Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task PgEFAddTestAsync()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
        var wrapperConnectionString = connectionString + $";Plugins=initialConnection,failover;";
        if (Deployment != DatabaseEngineDeployment.AURORA && Deployment != DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
        {
            wrapperConnectionString = connectionString + $";Plugins=initialConnection,failover;";
        }

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder.UseNpgsql(connectionString))
            .LogTo(Console.WriteLine)
            .Options;

        using (var db = new PersonDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Person person = new() { FirstName = "Jane", LastName = "Smith" };
            await db.AddAsync(person, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            await foreach (Person p in db.Persons.Where(x => x.FirstName != null && x.FirstName.StartsWith("J")).AsAsyncEnumerable())
            {
                Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFCrashBeforeOpenWithFailoverPluginTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=initialConnection,failover;" +
            $"EnableConnectFailover=true;" +
            $"VerifyOpenedConnectionType=writer;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            db.Database.ExecuteSqlRaw($"Truncate table persons;");
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            db.Add(jane);
            db.SaveChanges();

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node before crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await AuroraUtils.CrashInstance(currentWriter, tcs);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node after crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            db.Add(john);
            db.SaveChanges();

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            db.Add(joe);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.True(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(3, db.Persons.Count());
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFCrashBeforeOpenWithFailoverPluginTestAsync()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=initialConnection,failover;" +
            $"EnableConnectFailover=true;" +
            $"VerifyOpenedConnectionType=writer;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            await db.AddAsync(jane, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node before crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await AuroraUtils.CrashInstance(currentWriter, tcs);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node after crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await db.AddAsync(john, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            await db.AddAsync(joe, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.True(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(3, db.Persons.Count());
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    public async Task EFCrashAfterOpenWithFailoverPluginTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=initialConnection,failover;" +
            $"EnableConnectFailover=true;" +
            $"VerifyOpenedConnectionType=writer;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            db.Database.ExecuteSqlRaw($"Truncate table persons;");
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            db.Add(jane);
            db.SaveChanges();

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node before crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                var connection = db.Database.GetDbConnection();
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        // Open explicly to trigger failover on execute pipeline
                        connection.Open();
                    }

                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var crashInstanceTask = AuroraUtils.CrashInstance(currentWriter, tcs);
                    await tcs.Task;

                    // Query to trigger failover
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    db.Add(john);
                    db.SaveChanges();
                    await crashInstanceTask;
                }
                finally
                {
                    connection.Close();
                }
            });

            Assert.Equal(EntityState.Detached, db.Entry(john).State);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node after crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            db.Add(joe);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.False(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(2, db.Persons.Count());
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    public async Task EFCrashAfterOpenWithFailoverPluginTestAsync()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=initialConnection,failover;" +
            $"EnableConnectFailover=true;" +
            $"VerifyOpenedConnectionType=writer;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            await db.AddAsync(jane, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node before crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                var connection = db.Database.GetDbConnection();
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        // Open explicly to trigger failover on execute pipeline
                        await connection.OpenAsync(TestContext.Current.CancellationToken);
                    }

                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var crashInstanceTask = AuroraUtils.CrashInstance(currentWriter, tcs);
                    await tcs.Task;

                    // Query to trigger failover
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    await db.AddAsync(john, TestContext.Current.CancellationToken);
                    await db.SaveChangesAsync(TestContext.Current.CancellationToken);
                    await crashInstanceTask;
                }
                finally
                {
                    await connection.CloseAsync();
                }
            });

            Assert.Equal(EntityState.Detached, db.Entry(john).State);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.logger.WriteLine($"==========================================");
            this.logger.WriteLine($"Current node after crash is {instanceId}");
            this.logger.WriteLine($"Current data source {db.Database.GetDbConnection().DataSource}");
            await db.Database.CloseConnectionAsync();

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            await db.AddAsync(joe, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.False(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(2, db.Persons.Count());
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFTempFailureWithFailoverPluginTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ProxyClusterEndpoint, ProxyPort, Username, Password, DefaultDbName, 5, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=initialConnection,failover;" +
            $"EnableConnectFailover=true;" +
            $"VerifyOpenedConnectionType=writer;" +
            $"ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            db.Database.ExecuteSqlRaw($"Truncate table persons;");
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            db.Add(jane);
            db.SaveChanges();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                var connection = db.Database.GetDbConnection();
                Task? clusterFailureTask = null;
                Task? writerNodeFailureTask = null;
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        // Open explicly to trigger failover on execute pipeline
                        connection.Open();
                    }

                    clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(ProxyClusterEndpoint, TimeSpan.Zero, TimeSpan.FromSeconds(15), tcs);
                    writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(15), tcs);
                    await tcs.Task;

                    // Query to trigger failover
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    db.Add(john);
                    db.SaveChanges();
                }
                finally
                {
                    tcs.TrySetResult();

                    if (clusterFailureTask != null && writerNodeFailureTask != null)
                    {
                        await Task.WhenAll(clusterFailureTask, writerNodeFailureTask).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
                    }

                    connection.Close();
                }
            });

            Assert.Equal(EntityState.Detached, db.Entry(john).State);

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            db.Add(joe);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.False(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(2, db.Persons.Count());
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFTempFailureWithFailoverPluginTestAsync()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ProxyClusterEndpoint, ProxyPort, Username, Password, DefaultDbName, 5, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=initialConnection,failover;" +
            $"EnableConnectFailover=true;" +
            $"VerifyOpenedConnectionType=writer;" +
            $"ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            await db.AddAsync(jane, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                var connection = db.Database.GetDbConnection();
                Task? clusterFailureTask = null;
                Task? writerNodeFailureTask = null;
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        // Open explicly to trigger failover on execute pipeline
                        await connection.OpenAsync(TestContext.Current.CancellationToken);
                    }

                    clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(ProxyClusterEndpoint, TimeSpan.Zero, TimeSpan.FromSeconds(15), tcs);
                    writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(15), tcs);
                    await tcs.Task;

                    // Query to trigger failover
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    await db.AddAsync(john, TestContext.Current.CancellationToken);
                    await db.SaveChangesAsync(TestContext.Current.CancellationToken);
                }
                finally
                {
                    tcs.TrySetResult();

                    if (clusterFailureTask != null && writerNodeFailureTask != null)
                    {
                        await Task.WhenAll(clusterFailureTask, writerNodeFailureTask).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
                    }

                    await connection.CloseAsync();
                }
            });

            Assert.Equal(EntityState.Detached, db.Entry(john).State);

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            await db.AddAsync(joe, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.False(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(2, db.Persons.Count());
        }
    }
}
