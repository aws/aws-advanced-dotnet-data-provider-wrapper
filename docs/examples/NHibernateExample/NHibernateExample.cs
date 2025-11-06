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
using AwsWrapperDataProvider.NHibernate;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Driver.MySqlConnector;

namespace NHibernateExample;

public class NHibernateExample
{
    private static readonly ISessionFactory SessionFactory = CreateSessionFactory();

    public static async Task Main(string[] args)
    {
        var start = DateTime.UtcNow;
        var threshold = TimeSpan.FromMinutes(5);

        int i = 0;
        try
        {
            // Keep inserting data for 5 minutes.
            while (DateTime.UtcNow - start < threshold)
            {
                var product = new Product { Name = $"Product{i}", Price = 29.99m + i, CreatedAt = DateTime.UtcNow, };

                await InsertProduct(product);
                i++;
            }
        }
        catch (HibernateException ex) when (ex.InnerException is FailoverSuccessException)
        {
            // NHibernate wraps FailoverSuccessException in HibernateException
            Console.WriteLine($"Failover completed successfully: {ex.InnerException.Message}");
        }
        catch (HibernateException ex) when (ex.InnerException is TransactionStateUnknownException)
        {
            // NHibernate wraps TransactionStateUnknownException in HibernateException
            Console.WriteLine($"Transaction state is unknown after failover: {ex.InnerException.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error occurred: {ex.Message}");
        }
    }

    private static async Task InsertProduct(Product product)
    {
        using var session = SessionFactory.OpenSession();
        using var transaction = session.BeginTransaction();

        await session.SaveAsync(product);
        await transaction.CommitAsync();
    }

    private static ISessionFactory CreateSessionFactory()
    {
        const string connectionString = "Server=<endpoint>;" +
                                        "Database=<db name>;" +
                                        "User Id=<user>;" +
                                        "Password=<password>;" +
                                        "Plugins=failover,initialConnection;";

        var cfg = new Configuration()
            .AddAssembly(Assembly.GetExecutingAssembly())
            .AddProperties(new Dictionary<string, string>
            {
                { "connection.connection_string", connectionString },
                { "dialect", "NHibernate.Dialect.MySQLDialect" },
            });

        cfg.DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>());

        return cfg.BuildSessionFactory();
    }
}
