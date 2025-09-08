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

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySQL.Tests;

public class EntityFrameowrkConnectivityTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-ef")]
    public void MysqlEFAddTest()
    {
        var connectionString = EFUtils.GetConnectionString();
        var version = new MySqlServerVersion("8.0.32");

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
            connectionString,
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
            foreach (Person p in db.Persons.Where(x => x.FirstName != null && x.FirstName.StartsWith('J')))
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

        var version = new MySqlServerVersion("8.0.32");

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
            connectionString,
            wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, version))
            .LogTo(Console.WriteLine)
            .Options;

        using (var db = new PersonDbContext(options))
        {
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                Person jane = new() { FirstName = "Jane", LastName = "Smith" };
                db.Add(jane);
                db.SaveChanges();

                await AuroraUtils.CrashInstance(currentWriter);

                Person john = new() { FirstName = "John", LastName = "Smith" };
                db.Add(john);
                db.SaveChanges();
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
