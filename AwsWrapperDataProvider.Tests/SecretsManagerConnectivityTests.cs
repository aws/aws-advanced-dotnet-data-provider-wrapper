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
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapperSecretsManagerWithSecretIdConnectionTest()
    {
        var auroraTestUtils = AuroraTestUtils.GetUtility();
        var secretId = "PGValidSecretId";
        var secretsARN = auroraTestUtils.CreateSecrets(secretId);
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
        const string query = "select 1";

        try
        {
            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
            AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = query;

            connection.Open();
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
    public void MySqlClientWrapperSecretsManagerWithSecretIdConnectionTest()
    {
        var auroraTestUtils = AuroraTestUtils.GetUtility();
        var secretId = "MySQLValidSecretId";
        auroraTestUtils.CreateSecrets(secretId);
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretId};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
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
        connectionString += $";Plugins=awsSecretsManager;SecretsManagerSecretId={secretsARN};SecretsManagerRegion={TestEnvironment.Env.Info.Region};";
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
