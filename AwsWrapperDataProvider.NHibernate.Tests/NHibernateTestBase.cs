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
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Driver;
using NHibernate.Driver.MySqlConnector;

namespace AwsWrapperDataProvider.NHibernate.Tests;

public abstract class NHibernateTestBase : IntegrationTestBase
{
    protected Configuration GetNHibernateConfiguration(
        string connectionString,
        Dictionary<string, string>? extraProperties = null)
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

        if (extraProperties != null)
        {
            foreach (var kvp in extraProperties)
            {
                properties[kvp.Key] = kvp.Value;
            }
        }

        return cfg.AddProperties(properties);
    }

    protected void CreateAndClearPersonsTable(ISession session)
    {
        switch (Engine)
        {
            case DatabaseEngine.PG:
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
}
