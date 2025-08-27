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

public class IamConnectivityTests : IntegrationTestBase
{
    private static readonly string IamUser = "test_john_doe";

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void IamPluginTest_WithPgDatabase()
    {
        string basicAuthConnectionString = ConnectionStringHelper.GetUrl(
            this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);

        Console.WriteLine("1. Opening initial connection...");
        using AwsWrapperConnection<NpgsqlConnection> basicAuthConnection = new(basicAuthConnectionString);
        basicAuthConnection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        // cannot proceed with test without the basic connection
        Assert.Equal(ConnectionState.Open, basicAuthConnection.State);

        Console.WriteLine("2. Creating db user and granting access via IAM...");
        try
        {
            using AwsWrapperCommand<NpgsqlCommand> createCommand = basicAuthConnection.CreateCommand<NpgsqlCommand>();
            createCommand.CommandText = $"CREATE USER {IamUser};";
            _ = createCommand.ExecuteScalar();
            Console.WriteLine("   ✓ Created db user");

            using AwsWrapperCommand<NpgsqlCommand> grantCommand = basicAuthConnection.CreateCommand<NpgsqlCommand>();
            grantCommand.CommandText = $"GRANT rds_iam TO {IamUser};";
            _ = grantCommand.ExecuteScalar();
            Console.WriteLine("   ✓ Granted access via IAM");
        }
        catch (Exception ex)
        {
            // proceed, if possible, as the user could already exist due to a badly cleaned up previous test
            Console.WriteLine($"   ⚠️ Encountered exception: {ex.Message}; proceeding anyways");
        }

        string iamConnectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, IamUser, null, this.defaultDbName);
        iamConnectionString += ";Plugins=iam;";

        if (TestEnvironment.Env.Info.Region != null)
        {
            iamConnectionString += $"IamRegion={TestEnvironment.Env.Info.Region};";
        }

        bool iamConnectionSuccessful = false;

        try
        {
            using (AwsWrapperConnection<NpgsqlConnection> connection = new(iamConnectionString))
            {
                using AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
                command.CommandText = "SELECT aurora_db_instance_identifier()";

                Console.WriteLine("3. Opening IAM connection...");
                connection.Open();
                Console.WriteLine("   ✓ Connected successfully");

                Console.WriteLine("4. Executing test query...");
                IDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0));
                    Console.WriteLine("   ✓ Test query executed successfully");
                }

                connection.Close();
                iamConnectionSuccessful = true;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ Encountered exception: {ex.Message}.");
        }

        Console.WriteLine("Cleaning up...");

        try
        {
            using AwsWrapperCommand<NpgsqlCommand> dropCommand = basicAuthConnection.CreateCommand<NpgsqlCommand>();
            dropCommand.CommandText = $"DROP USER {IamUser};";
            _ = dropCommand.ExecuteScalar();
            Console.WriteLine("   ✓ Dropped db user");

            basicAuthConnection.Close();
        }
        catch
        {
            // ignore
        }

        Assert.True(iamConnectionSuccessful);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void IamPluginTest_WithMySqlDatabase()
    {
        string basicAuthConnectionString = ConnectionStringHelper.GetUrl(
            this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);

        Console.WriteLine("1. Opening initial connection...");
        using AwsWrapperConnection<MySqlConnector.MySqlConnection> basicAuthConnection = new(basicAuthConnectionString);
        basicAuthConnection.Open();
        Console.WriteLine("   ✓ Connected successfully");

        // cannot proceed with test without the basic connection
        Assert.Equal(ConnectionState.Open, basicAuthConnection.State);

        Console.WriteLine("2. Creating db user and granting access via IAM...");
        try
        {
            using AwsWrapperCommand<MySqlConnector.MySqlCommand> createCommand =
                basicAuthConnection.CreateCommand<MySqlConnector.MySqlCommand>();
            createCommand.CommandText = $"CREATE USER '{IamUser}' IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';";
            _ = createCommand.ExecuteScalar();
            Console.WriteLine("   ✓ Created db user with IAM authentication");

            using AwsWrapperCommand<MySqlConnector.MySqlCommand> privsCommand =
                basicAuthConnection.CreateCommand<MySqlConnector.MySqlCommand>();
            privsCommand.CommandText = $"GRANT ALL PRIVILEGES ON *.* TO '{IamUser}'@'%';";
            _ = privsCommand.ExecuteScalar();
            Console.WriteLine("   ✓ Allow SSL connections to db user");
        }
        catch (Exception ex)
        {
            // proceed, if possible, as the user could already exist due to a badly cleaned up previous test
            Console.WriteLine($"   ⚠️ Encountered exception: {ex.Message}; proceeding anyways");
        }

        string iamConnectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, null, null, this.defaultDbName);
        iamConnectionString += $";Plugins=iam;Username={IamUser};";

        if (TestEnvironment.Env.Info.Region != null)
        {
            iamConnectionString += $"IamRegion={TestEnvironment.Env.Info.Region};";
        }

        bool iamConnectionSuccessful = false;

        try
        {
            using (AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(iamConnectionString))
            {
                using AwsWrapperCommand<MySqlConnector.MySqlCommand> command = connection.CreateCommand<MySqlConnector.MySqlCommand>();
                command.CommandText = "SELECT @@aurora_server_id;";

                Console.WriteLine("3. Opening IAM connection...");
                connection.Open();
                Console.WriteLine("   ✓ Connected successfully");

                Console.WriteLine("4. Executing test query...");
                IDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0));
                    Console.WriteLine("   ✓ Test query executed successfully");
                }

                connection.Close();
                iamConnectionSuccessful = true;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ Encountered exception: {ex.Message}.");
        }

        Console.WriteLine("Cleaning up...");

        try
        {
            using AwsWrapperCommand<MySqlConnector.MySqlCommand> dropCommand = basicAuthConnection.CreateCommand<MySqlConnector.MySqlCommand>();
            dropCommand.CommandText = $"DROP USER '{IamUser}'@'%';";
            _ = dropCommand.ExecuteScalar();
            Console.WriteLine("   ✓ Dropped db user");

            basicAuthConnection.Close();
        }
        catch
        {
            // ignore
        }

        Assert.True(iamConnectionSuccessful);
    }
}
