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
using AwsWrapperDataProvider.Tests.Container.Utils;
using NHibernate;
using NHibernate.Criterion;

namespace AwsWrapperDataProvider.NHibernate.Tests
{
    /// <summary>
    /// NHibernate integration tests for the <c>gdbFailover</c> plugin.
    /// Mirrors the scenarios covered in <see cref="AwsWrapperDriverTests"/> but exercises
    /// the Global Database Failover Plugin and its <c>ActiveHomeFailoverMode</c> /
    /// <c>InactiveHomeFailoverMode</c> configuration knobs.
    /// </summary>
    public class GdbFailoverDriverTests : NHibernateTestBase
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

        /// <summary>
        /// Builds the gdbFailover wrapper connection string used by tests that crash the
        /// writer instance via the RDS API and rely on the cluster endpoint (or DNS update)
        /// to discover the new writer. Uses the non-proxy instance endpoint suffix so the
        /// driver re-connects through the real Aurora topology.
        /// </summary>
        private static string GetGdbCrashWrapperConnectionString(string connectionString, string mode)
        {
            return connectionString
                + ";Plugins=gdbFailover,initialConnection;"
                + "EnableConnectFailover=true;"
                + $"ActiveHomeFailoverMode={mode};"
                + $"InactiveHomeFailoverMode={mode};"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        }

        /// <summary>
        /// Builds the gdbFailover wrapper connection string for tests that trigger temporary
        /// failures through Toxiproxy on the proxied cluster endpoint.
        /// </summary>
        private static string GetGdbProxyWrapperConnectionString(string connectionString, string mode)
        {
            return connectionString
                + ";Plugins=gdbFailover;"
                + "EnableConnectFailover=true;"
                + $"ActiveHomeFailoverMode={mode};"
                + $"InactiveHomeFailoverMode={mode};"
                + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        }

        /// <summary>
        /// Smoke test: open an NHibernate session through the gdbFailover plugin in
        /// strict-writer mode, save a row, and read it back. Verifies that the gdbFailover
        /// plugin chain does not break the happy path.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        [Trait("Engine", "multi-az-instance")]
        public void NHibernateGdbAddTest()
        {
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = connectionString
                + ";Plugins=gdbFailover;"
                + "ActiveHomeFailoverMode=strict-writer;"
                + "InactiveHomeFailoverMode=strict-writer;";

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

        /// <summary>
        /// Crashes the writer between NHibernate transactions while pooling is disabled.
        /// The next transaction opens a fresh connection, so the gdbFailover plugin in
        /// strict-writer mode must transparently re-connect to the newly elected writer.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateGdbCrashBeforeOpenTest_WithoutPooling()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer")
                + ";Pooling=false;";
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

                // Crash instance before opening a new connection.
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await AuroraUtils.CrashInstance(currentWriter, tcs);

                // Allow time for the cluster to elect a new writer.
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

        /// <summary>
        /// Same as <see cref="NHibernateGdbCrashBeforeOpenTest_WithoutPooling"/> but with
        /// connection pooling enabled. Verifies that pooled connections marked invalid by
        /// the gdbFailover plugin are re-established against the new writer.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateGdbCrashBeforeOpenTest_WithPooling()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer")
                + ";Pooling=true;";
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

                // Crash instance before opening a new connection.
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await AuroraUtils.CrashInstance(currentWriter, tcs);

                // Allow time for the cluster to elect a new writer.
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

        /// <summary>
        /// Crashes the writer while an NHibernate transaction is in flight (long-running query).
        /// The gdbFailover plugin in strict-writer mode must abort the in-flight transaction
        /// with <see cref="TransactionStateUnknownException"/>, then allow subsequent
        /// transactions on the same session against the new writer.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        public async Task NHibernateGdbCrashAfterOpenTest_StrictWriter()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer");
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
                    var writerNodeFailureTask = Task.Run(
                        async () =>
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

                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                // Allow time for the cluster to elect a new writer.
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
                Assert.True(persons.Count >= 2);
            }
        }

        /// <summary>
        /// Crashes the writer mid-transaction while running on a multi-az cluster. Mirrors
        /// <c>NHibernateCrashAfterOpenWithFailoverTest_MultiAzCluster</c> but with the
        /// gdbFailover plugin chain in strict-writer mode.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateGdbCrashAfterOpenTest_MultiAzCluster()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer");
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
                    var clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(
                        ProxyClusterEndpoint,
                        TimeSpan.Zero,
                        TimeSpan.FromSeconds(20),
                        tcs);
                    var writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(
                        currentWriter,
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

                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                // Allow time for the cluster to elect a new writer.
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
                Assert.True(persons.Count >= 2);
            }
        }

        /// <summary>
        /// Simulates a temporary failure of the cluster and writer through Toxiproxy.
        /// The gdbFailover plugin in strict-writer mode must surface the in-flight failure
        /// as <see cref="TransactionStateUnknownException"/>, then recover so the next
        /// transaction succeeds against the re-elected writer.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateGdbTempFailureTest_StrictWriter()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

            var connectionString = ConnectionStringHelper.GetUrl(
                Engine, ProxyClusterEndpoint, ProxyPort, Username, Password, DefaultDbName, 2, 10);
            var wrapperConnectionString = GetGdbProxyWrapperConnectionString(connectionString, "strict-writer");
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

                using (var transaction = session.BeginTransaction())
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var clusterFailureTask = AuroraUtils.SimulateTemporaryFailureTask(
                        ProxyClusterEndpoint,
                        TimeSpan.Zero,
                        TimeSpan.FromSeconds(20),
                        tcs);
                    var writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(
                        currentWriter,
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

                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                var joe = new Person { FirstName = "Joe", LastName = "Smith" };
                using (var transaction = session.BeginTransaction())
                {
                    session.Save(joe);
                    transaction.Commit();
                }

                // John was attempted during the failover so it should not be persisted.
                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.Contains(persons, p => p.FirstName == "Joe");
                Assert.DoesNotContain(persons, p => p.FirstName == "John");
                Assert.Equal(2, persons.Count);
            }
        }

        /// <summary>
        /// Crashes the writer between NHibernate transactions and lets the gdbFailover plugin
        /// fail over to a reader using <c>strict-home-reader</c> mode. The next session
        /// transaction is expected to succeed even though the connection is now bound to a
        /// reader instance (read-only queries via the session).
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        public async Task NHibernateGdbCrashBeforeOpenTest_StrictHomeReader()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
            var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-home-reader")
                + ";Pooling=false;";
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

                // Crash the writer; gdbFailover should fall over to a home-region reader on the next open.
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await AuroraUtils.CrashInstance(currentWriter, tcs);

                // Allow time for failover to complete.
                await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

                // Reads are still served on the reader the plugin failed over to.
                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
            }
        }

        /// <summary>
        /// Simulates a temporary failure of the writer through Toxiproxy and verifies that the
        /// gdbFailover plugin in <c>home-reader-or-writer</c> mode fails the in-flight
        /// transaction with <see cref="TransactionStateUnknownException"/>, then allows the
        /// session to keep working once connectivity is restored.
        /// </summary>
        [Fact(Timeout = 60 * 60 * 1000)]
        [Trait("Category", "Integration")]
        [Trait("Database", "mysql-nh")]
        [Trait("Database", "pg-nh")]
        [Trait("Engine", "aurora")]
        [Trait("Engine", "multi-az-cluster")]
        public async Task NHibernateGdbTempFailureTest_HomeReaderOrWriter()
        {
            Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

            string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;

            var connectionString = ConnectionStringHelper.GetUrl(
                Engine, ProxyClusterEndpoint, ProxyPort, Username, Password, DefaultDbName, 2, 10);
            var wrapperConnectionString = GetGdbProxyWrapperConnectionString(connectionString, "home-reader-or-writer");
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

                using (var transaction = session.BeginTransaction())
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var writerNodeFailureTask = AuroraUtils.SimulateTemporaryFailureTask(
                        currentWriter,
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

                    await writerNodeFailureTask;

                    Assert.IsType<TransactionStateUnknownException>(exception.InnerException);
                }

                session.Clear();

                // Reads against any host (writer or home-region reader) should succeed.
                var persons = session.CreateCriteria(typeof(Person)).List<Person>();
                Assert.Contains(persons, p => p.FirstName == "Jane");
                Assert.DoesNotContain(persons, p => p.FirstName == "John");
            }
        }
    }
}
