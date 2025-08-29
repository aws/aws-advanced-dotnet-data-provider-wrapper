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
using AwsWrapperDataProvider.Tests.Container.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class SecretsManagerConnectivityTests : IntegrationTestBase
{
    private readonly AuroraTestUtils auroraTestUtils;
    private readonly string secretId;
    private readonly string secretsArn;

    public SecretsManagerConnectivityTests()
    {
        this.auroraTestUtils = AuroraTestUtils.GetUtility();
        this.secretId = this.engine.ToString() + "TestValidSecretId";
        this.secretsArn = this.auroraTestUtils.CreateSecrets(this.secretId);
    }

    public override void AfterAll()
    {
        this.auroraTestUtils.DeleteSecrets(this.secretId);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapper_WithSecretId()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={this.secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via Secrets Manager...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        Console.WriteLine("2. Executing query to connection via Secrets Manager...");
        IDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
            Console.WriteLine("   ✓ Executed successfully");
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapper_WithSecretId()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={this.secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via Secrets Manager...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
        command.CommandText = query;

        Console.WriteLine("2. Executing query to connection via Secrets Manager...");
        IDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
            Console.WriteLine("   ✓ Executed successfully");
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlConnectorWrapper_WithSecretId()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={this.secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via Secrets Manager...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        AwsWrapperCommand<MySqlConnector.MySqlCommand> command = connection.CreateCommand<MySqlConnector.MySqlCommand>();
        command.CommandText = query;

        Console.WriteLine("2. Executing query to connection via Secrets Manager...");
        IDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
            Console.WriteLine("   ✓ Executed successfully");
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapper_WithSecretArn()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={this.secretsArn};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via Secrets Manager...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        Console.WriteLine("2. Executing query to connection via Secrets Manager...");
        IDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
            Console.WriteLine("   ✓ Executed successfully");
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapper_WithSecretArn()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={this.secretsArn};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via Secrets Manager...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
        command.CommandText = query;

        Console.WriteLine("2. Executing query to connection via Secrets Manager...");
        IDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
            Console.WriteLine("   ✓ Executed successfully");
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlConnectorWrapper_WithSecretArn()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={this.secretsArn};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via Secrets Manager...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        AwsWrapperCommand<MySqlConnector.MySqlCommand> command = connection.CreateCommand<MySqlConnector.MySqlCommand>();
        command.CommandText = query;

        Console.WriteLine("2. Executing query to connection via Secrets Manager...");
        IDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
            Console.WriteLine("   ✓ Executed successfully");
        }
    }
}
