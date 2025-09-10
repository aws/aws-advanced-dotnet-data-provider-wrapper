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

using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using MySqlConnector;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySQL.Tests;

public class EntityFrameowrkConnectivityTests : IntegrationTestBase
{
    private readonly ITestOutputHelper logger;

    public EntityFrameowrkConnectivityTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-ef")]
    public void MysqlEFAddTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, Username, Password, DefaultDbName);
        var wrapperConnectionString = connectionString + $";Plugins=failover;";
        var version = new MySqlServerVersion("8.0.32");

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
            wrapperConnectionString,
            wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, version))
            .LogTo(Console.WriteLine)
            .Options;

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
    public async Task EFFailoverTest()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        var loggerFactory = LoggerFactory.Create(builder =>
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

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, Username, Password, DefaultDbName, 2, 10);

        var wrapperConnectionString = connectionString
            + $";Plugins=failover;" +
            $"EnableConnectFailover=true;" +
            $"ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        var version = new MySqlServerVersion("8.0.32");

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(loggerFactory)
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder
                    .UseLoggerFactory(loggerFactory)
                    .UseMySql(connectionString, version))
            .Options;

        using (var db = new PersonDbContext(options))
        {
            await Assert.ThrowsAsync<TransactionStateUnknownException>(async () =>
            {
                Person jane = new() { FirstName = "Jane", LastName = "Smith" };
                db.Add(jane);
                db.SaveChanges();

                using (var connection = db.Database.GetDbConnection())
                {
                    this.logger.WriteLine("Current Connection state: " + connection.State);

                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    this.logger.WriteLine("Current Connection current node: " + AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
                    this.logger.WriteLine("Current Connection Writer node: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsWriterQuery));
                    this.logger.WriteLine("Current Connection Is current node reader: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsReaderQuery));
                }

                using (AwsWrapperConnection<MySqlConnection> connection = new(wrapperConnectionString))
                {
                    connection.Open();
                    this.logger.WriteLine("New Connection current node: " + AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
                    this.logger.WriteLine("New Connection Writer node: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsWriterQuery));
                    this.logger.WriteLine("New Connection Is current node reader: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsReaderQuery));
                }

                await AuroraUtils.CrashInstance(currentWriter);

                using (var connection = db.Database.GetDbConnection())
                {
                    this.logger.WriteLine("Current Connection state: " + connection.State);
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    this.logger.WriteLine("Current Connection Current node: " + AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
                    this.logger.WriteLine("Current Connection Writer node: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsWriterQuery));
                    this.logger.WriteLine("Current Connection Is current node reader: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsReaderQuery));
                }

                using (AwsWrapperConnection<MySqlConnection> connection = new(wrapperConnectionString))
                {
                    connection.Open();
                    this.logger.WriteLine("New Connection Current node: " + AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
                    this.logger.WriteLine("New Connection Writer node: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsWriterQuery));
                    this.logger.WriteLine("New Connection Is current node reader: " + AuroraUtils.ExecuteQuery(connection, Engine, Deployment, AuroraMysqlDialect.IsReaderQuery));
                }

                Person john = new() { FirstName = "John", LastName = "Smith" };
                db.Add(john);
                db.SaveChanges();

                using (AwsWrapperConnection<MySqlConnection> connection = new(wrapperConnectionString))
                {
                    connection.Open();
                    this.logger.WriteLine(AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment));
                }
            });

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            db.Add(joe);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.False(db.Persons.Any(p => p.FirstName == "John"));
        }
    }
}
