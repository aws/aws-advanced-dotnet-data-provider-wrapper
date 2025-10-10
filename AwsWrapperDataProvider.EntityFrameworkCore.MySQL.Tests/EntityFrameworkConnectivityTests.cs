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

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySQL.Tests;

public class EntityFrameworkConnectivityTests : IntegrationTestBase
{
    protected override bool MakeSureFirstInstanceWriter => true;

    private readonly ITestOutputHelper logger;
    private readonly MySqlServerVersion version = new("8.0.32");
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
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public void MysqlEFAddTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
        var wrapperConnectionString = connectionString + $";Plugins=failover;";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
            wrapperConnectionString,
            wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, this.version))
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
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task MysqlEFAddAsyncTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
        var wrapperConnectionString = connectionString + $";Plugins=failover;";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
            wrapperConnectionString,
            wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, this.version))
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
            await foreach (Person p in db.Persons.Where(x => x.FirstName != null && x.FirstName.StartsWith('J')).AsAsyncEnumerable())
            {
                Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFCrashBeforeOpenWithFailoverPluginTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=failover;" +
            $"EnableConnectFailover=true;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseMySql(connectionString, this.version))
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

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await AuroraUtils.CrashInstance(currentWriter, tcs);

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
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EFCrashAfterOpenWithFailoverPluginTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=failover;" +
            $"EnableConnectFailover=true;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseMySql(connectionString, this.version))
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
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EFCrashAfterOpenWithFailoverPluginTestAsync()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=failover;" +
            $"EnableConnectFailover=true;" +
            $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseMySql(connectionString, this.version))
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
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFTempFailureWithFailoverPluginTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ProxyClusterEndpoint, ProxyPort, Username, Password, DefaultDbName, 2, 5);

        var wrapperConnectionString = connectionString
            + $";Plugins=failover;" +
            $"EnableConnectFailover=true;" +
            $"ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(this.loggerFactory)
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(this.loggerFactory)
                    .UseMySql(connectionString, this.version))
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
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        // Open explicly to trigger failover on execute pipeline
                        connection.Open();
                    }

                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(ProxyClusterEndpoint, TimeSpan.Zero, TimeSpan.FromSeconds(20), tcs);
                    var writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(20), tcs);
                    await tcs.Task;

                    // Query to trigger failover
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    db.Add(john);
                    db.SaveChanges();
                    await Task.WhenAll(clusterFailureTask, writerNodeFailureTask);
                }
                finally
                {
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
}
