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

using System.Data.Common;
using System.Reflection;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.Extensions.Logging;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Driver;
using Npgsql;

namespace AwsWrapperDataProvider.NHibernate.Tests;

public class LimitlessConnectivityTests : IntegrationTestBase
{
    private static readonly ILogger<LimitlessConnectivityTests> Logger = LoggerUtils.GetLogger<LimitlessConnectivityTests>();
    private readonly AuroraTestUtils auroraTestUtils;

    public LimitlessConnectivityTests()
    {
        this.auroraTestUtils = AuroraTestUtils.GetUtility();
    }

    private Configuration GetNHibernateConfiguration(string connectionString)
    {
        var properties = new Dictionary<string, string>
        {
            { "connection.connection_string", connectionString },
            { "dialect", "NHibernate.Dialect.PostgreSQLDialect" },
        };

        var cfg = new Configuration().AddAssembly(Assembly.GetExecutingAssembly());
        cfg.DataBaseIntegration(c => c.UseAwsWrapperDriver<NpgsqlDriver>());

        return cfg.AddProperties(properties);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-nh")]
    [Trait("Engine", "aurora-limitless")]
    public void NHibernate_LimitlessValidConnectionProperties()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, plugins: "limitless");

        var cfg = this.GetNHibernateConfiguration(connectionString);
        var sessionFactory = cfg.BuildSessionFactory();

        Logger.LogInformation("1. Opening NHibernate session with limitless plugin...");
        using (var session = sessionFactory.OpenSession())
        {
            Logger.LogInformation("   ✓ Connected successfully");

            using (var transaction = session.BeginTransaction())
            {
                var connection = session.Connection as DbConnection;
                if (connection != null)
                {
                    var instanceId = this.auroraTestUtils.QueryInstanceId(connection);
                    Assert.NotNull(instanceId);
                    Assert.NotEmpty(instanceId);
                    Logger.LogInformation("   ✓ Instance ID: {InstanceId}", instanceId);
                }

                // Verify we can execute a simple query
                var result = session.CreateSQLQuery("SELECT 1").UniqueResult<int>();
                Assert.Equal(1, result);
                Logger.LogInformation("   ✓ Query executed successfully");

                transaction.Commit();
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-nh")]
    [Trait("Engine", "aurora-limitless")]
    public void NHibernate_LimitlessAndIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName, plugins: "iam,limitless");
        connectionString += $";IamRegion={iamRegion}";

        var cfg = this.GetNHibernateConfiguration(connectionString);
        var sessionFactory = cfg.BuildSessionFactory();

        Logger.LogInformation("1. Opening NHibernate session with limitless and IAM plugins...");
        using (var session = sessionFactory.OpenSession())
        {
            Logger.LogInformation("   ✓ Connected successfully");

            using (var transaction = session.BeginTransaction())
            {
                var connection = session.Connection as DbConnection;
                if (connection != null)
                {
                    var instanceId = this.auroraTestUtils.QueryInstanceId(connection);
                    Assert.NotNull(instanceId);
                    Assert.NotEmpty(instanceId);
                    Logger.LogInformation("   ✓ Instance ID: {InstanceId}", instanceId);
                }

                // Verify we can execute a simple query
                var result = session.CreateSQLQuery("SELECT 1").UniqueResult<int>();
                Assert.Equal(1, result);
                Logger.LogInformation("   ✓ Query executed successfully");

                transaction.Commit();
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-nh")]
    [Trait("Engine", "aurora-limitless")]
    public void NHibernate_LimitlessAndAwsSecretsManagerPlugin()
    {
        var secretName = $"TestSecret-{Guid.NewGuid()}";
        _ = this.auroraTestUtils.CreateSecrets(secretName);

        try
        {
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, null, null, DefaultDbName, plugins: "awsSecretsManager,limitless");
            connectionString += $";SecretsManagerSecretId={secretName};SecretsManagerRegion={TestEnvironment.Env.Info.Region}";

            var cfg = this.GetNHibernateConfiguration(connectionString);
            var sessionFactory = cfg.BuildSessionFactory();

            Logger.LogInformation("1. Opening NHibernate session with limitless and Secrets Manager plugins...");
            using (var session = sessionFactory.OpenSession())
            {
                Logger.LogInformation("   ✓ Connected successfully");

                using (var transaction = session.BeginTransaction())
                {
                    var connection = session.Connection as DbConnection;
                    if (connection != null)
                    {
                        var instanceId = this.auroraTestUtils.QueryInstanceId(connection);
                        Assert.NotNull(instanceId);
                        Assert.NotEmpty(instanceId);
                        Logger.LogInformation("   ✓ Instance ID: {InstanceId}", instanceId);
                    }

                    // Verify we can execute a simple query
                    var result = session.CreateSQLQuery("SELECT 1").UniqueResult<int>();
                    Assert.Equal(1, result);
                    Logger.LogInformation("   ✓ Query executed successfully");

                    transaction.Commit();
                }
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretName);
        }
    }
}
