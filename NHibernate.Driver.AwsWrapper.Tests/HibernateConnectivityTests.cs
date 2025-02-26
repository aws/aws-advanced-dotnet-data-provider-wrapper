using NHibernate;
using NHibernate.Cfg;
using NHibernate.Criterion;
using NHibernate.Driver;
using NHibernate.Driver.AwsWrapper;
using NHibernate.Driver.MySqlConnector;
using System.Diagnostics;
using System.Reflection;

namespace NHibernate.Driver.AwsWrapper.Tests
{
    public class HibernateConnectivityTests
    {
        [Fact]
        public void MysqlProvidedConnectionTest() {
            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", "Server=global-ohio-mysql-instance-1.c12pgqavxipt.us-east-2.rds.amazonaws.com;User ID=admin;Password=my_password_2020;Initial Catalog=test;" },
                { "dialect", "NHibernate.Dialect.MySQLDialect" }
            };

            Configuration cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .AddFile("Person.hbm.xml")
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>())
                .AddProperties(properties)
                .Configure();
            ISessionFactory sessions = cfg.BuildSessionFactory();

            using (var session = sessions.OpenSession())
            {
                using (ITransaction transaction = session.BeginTransaction())
                {
                    var person = new Person() { FirstName = "John", LastName = "Doe" };
                    session.Save(person);
                    transaction.Commit();
                }
            }

            using (var session = sessions.OpenSession())
            {
                var persons = session.CreateCriteria(typeof(Person))
                    .Add(Restrictions.Eq("FirstName", "John"))
                    .List<Person>();
                foreach (Person p in persons) 
                {
                    Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
                }
            }
        }

        [Fact]
        public void PgProvidedConnectionTest()
        {
            var properties = new Dictionary<string, string>
            {
                { "connection.connection_string", "Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=test;" },
                { "dialect", "NHibernate.Dialect.PostgreSQLDialect" }
            };
            
            Configuration cfg = new Configuration()
                .AddAssembly(Assembly.GetExecutingAssembly())
                .AddFile("Person.hbm.xml")
                .DataBaseIntegration(c => c.UseAwsWrapperDriver<NpgsqlDriver>())
                .AddProperties(properties)
                .Configure();
            ISessionFactory sessions = cfg.BuildSessionFactory();

            using (var session = sessions.OpenSession())
            {
                using (ITransaction transaction = session.BeginTransaction())
                {
                    var person = new Person() { FirstName = "John", LastName = "Doe" };
                    session.Save(person);
                    transaction.Commit();
                }
            }

            using (var session = sessions.OpenSession())
            {
                var persons = session.CreateCriteria(typeof(Person))
                    .Add(Restrictions.Eq("FirstName", "John"))
                    .List<Person>();
                foreach (Person p in persons)
                {
                    Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
                }
            }
        }
    }
}
