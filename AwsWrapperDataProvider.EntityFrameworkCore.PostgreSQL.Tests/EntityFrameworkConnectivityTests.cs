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

using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: CaptureConsole]

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL.Tests;

public class EntityFrameworkConnectivityTests // : IntegrationTestBase
{
    //protected override bool MakeSureFirstInstanceWriter => true;

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
        var connectionString = ConnectionStringHelper.GetUrl(DatabaseEngine.PG,
            "database-yan-pg.cluster-cxmsoia46djo.us-west-2.rds.amazonaws.com",
            5432,
            "postgres",
            "postgres",
            "postgres");
        var wrapperConnectionString = connectionString + $";Plugins=initialConnection,failover;";
        //if (Deployment != DatabaseEngineDeployment.AURORA && Deployment != DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
        //{
        //    wrapperConnectionString = connectionString + $";Plugins=failover;";
        //}

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder.UseNpgsql(connectionString))
            .LogTo(Console.WriteLine)
            .Options;

        using (var db = new PersonDbContext(options))
        {
            db.Database.EnsureCreated();
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

    //[Fact]
    //[Trait("Category", "Integration")]
    //[Trait("Database", "pg-ef")]
    //[Trait("Engine", "aurora")]
    //[Trait("Engine", "multi-az-cluster")]
    //[Trait("Engine", "multi-az-instance")]
    //public async Task PgEFAddTestAsync()
    //{
    //    var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
    //    var wrapperConnectionString = connectionString + $";Plugins=initialConnection,failover;";
    //    if (Deployment != DatabaseEngineDeployment.AURORA && Deployment != DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
    //    {
    //        wrapperConnectionString = connectionString + $";Plugins=failover;";
    //    }

    //    var options = new DbContextOptionsBuilder<PersonDbContext>()
    //        .UseAwsWrapper(
    //            wrapperConnectionString,
    //            wrappedOptionBuilder => wrappedOptionBuilder.UseNpgsql(connectionString))
    //        .LogTo(Console.WriteLine)
    //        .Options;

    //    using (var db = new PersonDbContext(options))
    //    {
    //        await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
    //    }

    //    using (var db = new PersonDbContext(options))
    //    {
    //        Person person = new() { FirstName = "Jane", LastName = "Smith" };
    //        await db.AddAsync(person, TestContext.Current.CancellationToken);
    //        await db.SaveChangesAsync(TestContext.Current.CancellationToken);
    //    }

    //    using (var db = new PersonDbContext(options))
    //    {
    //        await foreach (Person p in db.Persons.Where(x => x.FirstName != null && x.FirstName.StartsWith("J")).AsAsyncEnumerable())
    //        {
    //            Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
    //        }
    //    }
    //}
}
