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
using System.Reflection;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySql.Data.MySqlClient;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Criterion;
using NHibernate.Driver;
using NHibernate.Driver.MySqlConnector;
using Npgsql;
using Org.BouncyCastle.Pqc.Crypto.Crystals.Dilithium;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: CaptureConsole]

namespace AwsWrapperDataProvider.NHibernate.Tests
{
    public class AwsWrapperDriverTests : IntegrationTestBase
    {
        protected override bool MakeSureFirstInstanceWriter => true;

        private string GetSleepQuery()
        {
            switch (Engine)
            {
                case DatabaseEngine.PG:
                    return "SELECT pg_sleep(120)";
                case DatabaseEngine.MYSQL:
                default:
                    return "SELECT SLEEP(120)";
            }
        }

        private void CreateAndClearPersonsTable(ISession session)
        {
            switch (Engine)
            {
                case DatabaseEngine.PG:
                    // PostgreSQL syntax - create sequence first
                    session.CreateSQLQuery("CREATE SEQUENCE IF NOT EXISTS hibernate_sequence START 1").ExecuteUpdate();

                    session.CreateSQLQuery(@"
                        CREATE TABLE IF NOT EXISTS persons (
                            Id SERIAL PRIMARY KEY,
                            FirstName VARCHAR(255),
                            LastName VARCHAR(255)
                        )").ExecuteUpdate();
                    break;
                case DatabaseEngine.MYSQL:
                default:
                    // MySQL syntax
                    session.CreateSQLQuery(@"
                        CREATE TABLE IF NOT EXISTS persons (
                            Id INT AUTO_INCREMENT PRIMARY KEY,
                            FirstName VARCHAR(255),
                            LastName VARCHAR(255)
                        )").ExecuteUpdate();
                    break;
            }

            session.CreateSQLQuery("TRUNCATE TABLE persons").ExecuteUpdate();
        }

        private Configuration GetNHibernateConfiguration(string connectionString)
        {
            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", connectionString },
            };

            var cfg = new Configuration().AddAssembly(Assembly.GetExecutingAssembly());

            switch (Engine)
            {
                case DatabaseEngine.PG:
                    properties.Add("dialect", "NHibernate.Dialect.PostgreSQLDialect");
                    cfg.DataBaseIntegration(c => c.UseAwsWrapperDriver<NpgsqlDriver>());
                    break;

                case DatabaseEngine.MYSQL:
                default:
                    properties.Add("dialect", "NHibernate.Dialect.MySQLDialect");
                    cfg.DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>());
                    break;
            }

            return cfg.AddProperties(properties);
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public void NHibernateAddTest()
        {
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString + ";Plugins=initialConnection,failover;";

            var cfg = this.GetNHibernateConfiguration(wrapperConnectionString);
            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(session);

                using (var transaction = session.BeginTransaction())
                {
                    var person = new Person { FirstName = "Jane", LastName = "Smith" };
                    session.Save(person);
                    transaction.Commit();
                }

                var persons = session.CreateCriteria(typeof(Person))
                    .Add(Restrictions.Like("FirstName", "J%"))
                    .List<Person>();

                Assert.NotEmpty(persons);
                Assert.Contains(persons, p => p.FirstName == "Jane");
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateCrashBeforeOpenWithFailoverTest_WithoutPooling()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString
                + ";Plugins=failover,initialConnection;"
                + "EnableConnectFailover=true;"
                + "FailoverMode=StrictWriter;"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort};"
                + $"Pooling=false;";
            var cfg = this.GetNHibernateConfiguration(wrapperConnectionString);
            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(session);

                var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                using (var transaction = session.BeginTransaction())
                {
                    session.Save(jane);
                    transaction.Commit();
                }

                // Crash instance before opening new connection
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await AuroraUtils.CrashInstance(currentWriter, tcs);

                // Since the new transaction should be a fresh connection adding timeout just to make sure that failover has completed.
                await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

                var john = new Person { FirstName = "John", LastName = "Smith" };

                using (var transaction = session.BeginTransaction())
                {
                    session.Save(john);
                    transaction.Commit();
                }

                var joe = new Person { FirstName = "Joe", LastName = "Smith" };

                using (var transaction = session.BeginTransaction())
                {
                    session.Save(joe);
                    transaction.Commit();
                }

                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "John");
                Assert.Contains(persons, p => p.FirstName == "Joe");
                Assert.Equal(3, persons.Count);
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateCrashBeforeOpenWithFailoverTest_WithPooling()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString
                + ";Plugins=failover,initialConnection;"
                + "EnableConnectFailover=true;"
                + "FailoverMode=StrictWriter;"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort};"
                + $"Pooling=true;";
            var cfg = this.GetNHibernateConfiguration(wrapperConnectionString);
            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(session);

                var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                using (var transaction = session.BeginTransaction())
                {
                    session.Save(jane);
                    transaction.Commit();
                }

                // Crash instance before opening new connection
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await AuroraUtils.CrashInstance(currentWriter, tcs);

                // Since the new transaction should be a fresh connection adding timeout just to make sure that failover has completed.
                await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

                var john = new Person { FirstName = "John", LastName = "Smith" };

                using (var transaction = session.BeginTransaction())
                {
                    session.Save(john);
                    transaction.Commit();
                }

                var joe = new Person { FirstName = "Joe", LastName = "Smith" };

                using (var transaction = session.BeginTransaction())
                {
                    session.Save(joe);
                    transaction.Commit();
                }

                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "John");
                Assert.Contains(persons, p => p.FirstName == "Joe");
                Assert.Equal(3, persons.Count);
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        public async Task NHibernateCrashAfterOpenWithFailoverTest()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString =
                ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString
                                          + ";Plugins=failover,initialConnection;"
                                          + "EnableConnectFailover=true;"
                                          + "FailoverMode=StrictWriter;"
                                          + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
            var cfg = this.GetNHibernateConfiguration(wrapperConnectionString);
            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(session);

                using (var transaction = session.BeginTransaction())
                {
                    var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                    session.Save(jane);
                    transaction.Commit();
                }

                using (var newTransaction = session.BeginTransaction())
                {
                    var token = TestContext.Current.CancellationToken;

                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var writerNodeFailureTask = Task.Run(async () =>
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5));
                        await AuroraUtils.CrashInstance(currentWriter, tcs);
                    },
                        token);
                    await tcs.Task;

                    var exception = await Assert.ThrowsAnyAsync<HibernateException>(async () =>
                    {
                        await session.CreateSQLQuery(this.GetSleepQuery()).ExecuteUpdateAsync(token);
                        await newTransaction.CommitAsync(token);
                    });

                    await Task.WhenAny(writerNodeFailureTask, Task.Delay(TimeSpan.FromMinutes(2), token));

                    // Verify the inner exception is FailoverSuccessException
                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                // Since the new transaction should be a fresh connection adding timeout just to make sure that failover has completed.
                await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);


                var joe = new Person { FirstName = "Joe", LastName = "Smith" };

                using (var finalTransaction = session.BeginTransaction())
                {
                    session.Save(joe);
                    finalTransaction.Commit();
                }

                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "Joe");

                // John may or may not be saved depending on when failover occurred
                Assert.True(persons.Count >= 2);
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateCrashAfterOpenWithFailoverTest_MultiAzCluster()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString =
                ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString
                                          + ";Plugins=failover,initialConnection;"
                                          + "EnableConnectFailover=true;"
                                          + "FailoverMode=StrictWriter;"
                                          + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
            var cfg = this.GetNHibernateConfiguration(wrapperConnectionString);
            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(session);

                using (var transaction = session.BeginTransaction())
                {
                    var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                    session.Save(jane);
                    transaction.Commit();
                }

                using (var newTransaction = session.BeginTransaction())
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(ProxyClusterEndpoint,
                        TimeSpan.Zero,
                        TimeSpan.FromSeconds(20),
                        tcs);
                    var writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter,
                        TimeSpan.Zero,
                        TimeSpan.FromSeconds(20),
                        tcs);
                    await tcs.Task;

                    var exception = Assert.ThrowsAny<HibernateException>(() =>
                    {
                        session.CreateSQLQuery(this.GetSleepQuery()).ExecuteUpdate();
                        newTransaction.Commit();
                    });

                    await Task.WhenAll(clusterFailureTask, writerNodeFailureTask);

                    // Verify the inner exception is FailoverSuccessException
                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                // Since the new transaction should be a fresh connection adding timeout just to make sure that failover has completed.
                await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);


                var joe = new Person { FirstName = "Joe", LastName = "Smith" };

                using (var finalTransaction = session.BeginTransaction())
                {
                    session.Save(joe);
                    finalTransaction.Commit();
                }

                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "Joe");

                // John may or may not be saved depending on when failover occurred
                Assert.True(persons.Count >= 2);
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateTempFailureWithFailoverTest()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

            var connectionString = ConnectionStringHelper.GetUrl(Engine, ProxyClusterEndpoint, ProxyPort, Username, Password, DefaultDbName, 2, 10);
            var wrapperConnectionString = connectionString
                + ";Plugins=failover;"
                + "EnableConnectFailover=true;"
                + "FailoverMode=StrictWriter;"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
            var cfg = this.GetNHibernateConfiguration(wrapperConnectionString);

            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(session);

                using (var transaction = session.BeginTransaction())
                {
                    var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                    session.Save(jane);
                    transaction.Commit();
                }

                var john = new Person { FirstName = "John", LastName = "Smith" };

                // Crash instance and let driver handle failover
                using (var transaction = session.BeginTransaction())
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(ProxyClusterEndpoint,
                        TimeSpan.Zero,
                        TimeSpan.FromSeconds(20),
                        tcs);
                    var writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter,
                        TimeSpan.Zero,
                        TimeSpan.FromSeconds(20),
                        tcs);
                    await tcs.Task;

                    var exception = await Assert.ThrowsAnyAsync<HibernateException>(() =>
                    {
                        session.Save(john);
                        transaction.Commit();
                        return Task.CompletedTask;
                    });

                    await Task.WhenAll(clusterFailureTask, writerNodeFailureTask);

                    // Verify the inner exception is FailoverSuccessException
                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                var joe = new Person { FirstName = "Joe", LastName = "Smith" };
                using (var transaction = session.BeginTransaction())
                {
                    session.Save(joe);
                    transaction.Commit();
                }

                // Verify records - John should not exist (failed during failover), Jane and Joe should exist
                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "Joe");
                Assert.DoesNotContain(persons, p => p.FirstName == "John");
                Assert.Equal(2, persons.Count);
            }
        }
    }
}
