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
using AwsWrapperDataProvider.Tests.Container.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class SecretsManagerConnectivityTests : IntegrationTestBase
{
    private readonly AuroraTestUtils auroraTestUtils;

    public SecretsManagerConnectivityTests()
    {
        this.auroraTestUtils = AuroraTestUtils.GetUtility();
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public void PgWrapper_WithSecretId()
    {
        var secretId = "PGValidSecretId";
        _ = this.auroraTestUtils.CreateSecrets(secretId);

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, null, null, DefaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening connection via Secrets Manager...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");

            using AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = query;

            Console.WriteLine("2. Executing query to connection via Secrets Manager...");
            using DbDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
                Console.WriteLine("   ✓ Executed successfully");
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public void PgWrapper_WithSecretArn()
    {
        var secretId = "PgValidSecretArn";
        var secretsArn = this.auroraTestUtils.CreateSecrets(secretId);

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, null, null, DefaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretsArn};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening connection via Secrets Manager...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");

            using AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = query;

            Console.WriteLine("2. Executing query to connection via Secrets Manager...");
            using DbDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
                Console.WriteLine("   ✓ Executed successfully");
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public void MySqlClientWrapper_WithSecretId()
    {
        var secretId = "MySqlClientValidSecretId";
        _ = this.auroraTestUtils.CreateSecrets(secretId);

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, null, null, DefaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening connection via Secrets Manager...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");

            using AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
            command.CommandText = query;

            Console.WriteLine("2. Executing query to connection via Secrets Manager...");
            using DbDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
                Console.WriteLine("   ✓ Executed successfully");
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public void MySqlClientWrapper_WithSecretArn()
    {
        var secretId = "MySqlClientValidSecretArn";
        var secretsArn = this.auroraTestUtils.CreateSecrets(secretId);

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, null, null, DefaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretsArn};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening connection via Secrets Manager...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");

            using AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
            command.CommandText = query;

            Console.WriteLine("2. Executing query to connection via Secrets Manager...");
            using DbDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
                Console.WriteLine("   ✓ Executed successfully");
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public void MySqlConnectorWrapper_WithSecretId()
    {
        var secretId = "MySqlConnectorValidSecretId";
        _ = this.auroraTestUtils.CreateSecrets(secretId);

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, null, null, DefaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening connection via Secrets Manager...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");

            using AwsWrapperCommand<MySqlConnector.MySqlCommand> command = connection.CreateCommand<MySqlConnector.MySqlCommand>();
            command.CommandText = query;

            Console.WriteLine("2. Executing query to connection via Secrets Manager...");
            using DbDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
                Console.WriteLine("   ✓ Executed successfully");
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public void MySqlConnectorWrapper_WithSecretArn()
    {
        var secretId = "MySqlConnectorValidSecretArn";
        var secretsArn = this.auroraTestUtils.CreateSecrets(secretId);

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, null, null, DefaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretsArn};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening connection via Secrets Manager...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");

            using AwsWrapperCommand<MySqlConnector.MySqlCommand> command = connection.CreateCommand<MySqlConnector.MySqlCommand>();
            command.CommandText = query;

            Console.WriteLine("2. Executing query to connection via Secrets Manager...");
            using DbDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
                Console.WriteLine("   ✓ Executed successfully");
            }
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretId);
        }
    }
}
