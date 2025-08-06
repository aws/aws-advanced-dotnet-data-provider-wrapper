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

public class PluginConnectivityTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    public void PgWrapperAdfsConnectionTest()
    {
        const string connectionString =
            "Host=<insert_rds_instance_here>;Database=<database_name_here>;dbUser=<db_user_with_iam_login>;Plugins=federatedAuth;iamRegion=<iam_region>;iamRoleArn=<iam_role_arn>;iamIdpArn=<iam_idp_arn>;idpEndpoint=<idp_endpoint>;idpUsername=<idp_username>;idpPassword=<idp_password>;";
        const string query = "select aurora_db_instance_identifier()";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        try
        {
            connection.Open();
            IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public void MySqlClientWrapperAdfsConnectionTest()
    {
        const string connectionString = "Server=<insert_rds_instance_here>;Initial Catalog=mysql;Database=<database_name_here>;dbUser=<db_user_with_iam_login>;Plugins=federatedAuth;iamRegion=<iam_region>;iamRoleArn=<iam_role_arn>;iamIdpArn=<iam_idp_arn>;idpEndpoint=<idp_endpoint>;idpUsername=<idp_username>;idpPassword=<idp_password>;";
        const string query = "select * from test";

        using (AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection =
               new(connectionString))
        {
            AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
            command.CommandText = query;

            try
            {
                connection.Open();
                IDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetInt32(0));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapperIamConnectionTest()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, iamUser, null, this.defaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";
        const string query = "select aurora_db_instance_identifier()";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        connection.Open();
        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine(reader.GetString(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapperIamConnectionTest()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Username={iamUser};Plugins=iam;IamRegion={iamRegion}";
        const string query = "select 1";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);
        connection.Open();
        AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
        command.CommandText = query;

        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlConnectorWrapperIamConnectionTest()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Username={iamUser};Plugins=iam;IamRegion={iamRegion}";
        const string query = "select 1";

        using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);
        connection.Open();
        AwsWrapperCommand<MySqlConnector.MySqlCommand> command = connection.CreateCommand<MySqlConnector.MySqlCommand>();
        command.CommandText = query;

        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine(reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapperSecretsManagerWithSecretIdConnectionTest()
    {
        var auroraTestUtils = AuroraTestUtils.GetUtility();
        var secretId = "PGValidSecretId";
        var secretsARN = auroraTestUtils.CreateSecrets(secretId);
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;secretsManagerSecretId={secretId};secretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
            connection.Open();
            AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = query;

            IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
        finally
        {
            auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapperSecretsManagerWithSecretIdConnectionTest()
    {
        var auroraTestUtils = AuroraTestUtils.GetUtility();
        var secretId = "MySQLValidSecretId";
        auroraTestUtils.CreateSecrets(secretId);
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;secretsManagerSecretId={secretId};secretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);
            connection.Open();
            AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
            command.CommandText = query;

            IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
            }
        }
        finally
        {
            auroraTestUtils.DeleteSecrets(secretId);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapperSecretsManagerWithSecretARNConnectionTest()
    {
        var auroraTestUtils = AuroraTestUtils.GetUtility();
        var secretId = "MySQLValidSecretARN";
        var secretsARN = auroraTestUtils.CreateSecrets(secretId);
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;secretsManagerSecretId={secretsARN};secretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);
            connection.Open();
            AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
            command.CommandText = query;

            IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
            }
        }
        finally
        {
            auroraTestUtils.DeleteSecrets(secretId);
        }
    }
}
