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

using System.Reflection;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using NHibernate.Cfg;
using NHibernate.Criterion;
using NHibernate.Driver;
using NHibernate.Driver.MySqlConnector;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace AwsWrapperDataProvider.NHibernate.Tests
{
    public class AwsWrapperDriverTests : IntegrationTestBase
    {
        protected override bool MakeSureFirstInstanceWriter => true;

        private void CreateAndClearPersonsTable(ISession session, bool isPostgreSQL = false)
        {
            if (isPostgreSQL)
            {
                // PostgreSQL syntax
                session.CreateSQLQuery(@"
                    CREATE TABLE IF NOT EXISTS persons (
                        Id SERIAL PRIMARY KEY,
                        FirstName VARCHAR(255),
                        LastName VARCHAR(255)
                    )").ExecuteUpdate();
            }
            else
            {
                // MySQL syntax
                session.CreateSQLQuery(@"
                    CREATE TABLE IF NOT EXISTS persons (
                        Id INT AUTO_INCREMENT PRIMARY KEY,
                        FirstName VARCHAR(255),
                        LastName VARCHAR(255)
                    )").ExecuteUpdate();
            }
            
            session.CreateSQLQuery("TRUNCATE TABLE persons").ExecuteUpdate();
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Engine", "aurora")]
        public void NHibernateMySqlAddTest()
        {
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString + ";Plugins=failover;";

            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", wrapperConnectionString },
                { "dialect", "NHibernate.Dialect.MySQLDialect" },
            };

            var cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>())
                .AddProperties(properties);

            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                CreateAndClearPersonsTable(session);
            }

            using (var session = sessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                var person = new Person { FirstName = "Jane", LastName = "Smith" };
                session.Save(person);
                transaction.Commit();
            }

            using (var session = sessionFactory.OpenSession())
            {
                var persons = session.CreateCriteria(typeof(Person))
                    .Add(Restrictions.Like("FirstName", "J%"))
                    .List<Person>();

                Assert.NotEmpty(persons);
                Assert.Contains(persons, p => p.FirstName == "Jane");
            }
        }

        [Fact]
        [Trait("Category", "Integration")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        public void NHibernatePostgreSqlAddTest()
        {
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString + ";Plugins=failover;";

            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", wrapperConnectionString },
                { "dialect", "NHibernate.Dialect.PostgreSQLDialect" },
            };

            var cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<NpgsqlDriver>())
                .AddProperties(properties);

            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                CreateAndClearPersonsTable(session, isPostgreSQL: true);
            }

            using (var session = sessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                var person = new Person { FirstName = "Jane", LastName = "Smith" };
                session.Save(person);
                transaction.Commit();
            }

            using (var session = sessionFactory.OpenSession())
            {
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
        [Trait("Engine", "aurora")]
        public async Task NHibernateCrashBeforeOpenWithFailoverTest()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);
            var wrapperConnectionString = connectionString
                + ";Plugins=failover;"
                + "EnableConnectFailover=true;"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", wrapperConnectionString },
                { "dialect", "NHibernate.Dialect.MySQLDialect" },
            };

            var cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>())
                .AddProperties(properties);

            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                CreateAndClearPersonsTable(session);
            }

            using (var session = sessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                session.Save(jane);
                transaction.Commit();

                // Crash instance before opening new connection
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await AuroraUtils.CrashInstance(currentWriter, tcs);

                // These operations should work transparently - driver handles failover during connection
                using (var newTransaction = session.BeginTransaction())
                {
                    var john = new Person { FirstName = "John", LastName = "Smith" };
                    session.Save(john);
                    newTransaction.Commit();
                }

                using (var anotherTransaction = session.BeginTransaction())
                {
                    var joe = new Person { FirstName = "Joe", LastName = "Smith" };
                    session.Save(joe);
                    anotherTransaction.Commit();
                }
            }

            using (var session = sessionFactory.OpenSession())
            {
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
        [Trait("Engine", "aurora")]
        public async Task NHibernateCrashAfterOpenWithFailoverTest()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);
            var wrapperConnectionString = connectionString
                + ";Plugins=failover;"
                + "EnableConnectFailover=true;"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", wrapperConnectionString },
                { "dialect", "NHibernate.Dialect.MySQLDialect" },
            };

            var cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>())
                .AddProperties(properties);

            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                CreateAndClearPersonsTable(session);
            }

            using (var session = sessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                session.Save(jane);
                transaction.Commit();

                var john = new Person { FirstName = "John", LastName = "Smith" };
                await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
                {
                    var connection = session.Connection;
                    try
                    {
                        if (connection.State == System.Data.ConnectionState.Closed)
                        {
                            connection.Open();
                        }

                        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                        var crashInstanceTask = AuroraUtils.CrashInstance(currentWriter, tcs);
                        await tcs.Task;

                        // Query to trigger failover on active connection
                        var anyUser = session.CreateCriteria(typeof(Person)).SetMaxResults(1).List<Person>().Any();

                        using (var newTransaction = session.BeginTransaction())
                        {
                            session.Save(john);
                            newTransaction.Commit();
                        }

                        await crashInstanceTask;
                    }
                    finally
                    {
                        connection.Close();
                    }
                });

                // Session state may be invalid after failover exception, continue with new operations
                using (var finalTransaction = session.BeginTransaction())
                {
                    var joe = new Person { FirstName = "Joe", LastName = "Smith" };
                    session.Save(joe);
                    finalTransaction.Commit();
                }
            }

            using (var session = sessionFactory.OpenSession())
            {
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
        [Trait("Engine", "aurora")]
        public async Task NHibernateFailoverTest()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10);
            var wrapperConnectionString = connectionString
                + ";Plugins=failover;"
                + "EnableConnectFailover=true;"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", wrapperConnectionString },
                { "dialect", "NHibernate.Dialect.MySQLDialect" },
            };

            var cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>())
                .AddProperties(properties);

            var sessionFactory = cfg.BuildSessionFactory();

            using (var session = sessionFactory.OpenSession())
            {
                CreateAndClearPersonsTable(session);
            }

            // Add initial data
            using (var session = sessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                var jane = new Person { FirstName = "Jane", LastName = "Smith" };
                session.Save(jane);
                transaction.Commit();
            }

            // Crash instance and let driver handle failover
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await AuroraUtils.CrashInstance(currentWriter, tcs);

            // Driver should handle failover transparently
            using (var session = sessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                var john = new Person { FirstName = "John", LastName = "Smith" };
                session.Save(john);
                transaction.Commit();
            }

            // Verify both records exist
            using (var session = sessionFactory.OpenSession())
            {
                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "John");
                Assert.Equal(2, persons.Count);
            }
        }
    }
}
