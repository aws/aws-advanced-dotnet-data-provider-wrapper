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
using System.Data.Common;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Container.Tests;

public class BasicConnectivityTests : IAsyncLifetime
{
    private readonly string defaultDbName = TestEnvironment.Env.Info.DatabaseInfo!.DefaultDbName;
    private readonly string username = TestEnvironment.Env.Info.DatabaseInfo!.Username;
    private readonly string password = TestEnvironment.Env.Info.DatabaseInfo!.Password;
    private readonly DatabaseEngine engine = TestEnvironment.Env.Info.Request!.Engine;
    private readonly string clusterEndpoint = TestEnvironment.Env.Info.DatabaseInfo!.ClusterEndpoint;
    private readonly int port = TestEnvironment.Env.Info.DatabaseInfo!.ClusterEndpointPort;

    public async ValueTask InitializeAsync()
    {
        if (TestEnvironment.Env.Info.Request!.Features.Contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED))
        {
            ProxyHelper.EnableAllConnectivity();
        }

        var deployment = TestEnvironment.Env.Info.Request.Deployment;
        if (deployment == DatabaseEngineDeployment.AURORA || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
        {
            int remainingTries = 3;
            bool success = false;

            while (remainingTries-- > 0 && !success)
            {
                try
                {
                    await TestEnvironment.CheckClusterHealthAsync(false);
                    success = true;
                }
                catch (Exception)
                {
                    switch (deployment)
                    {
                        case DatabaseEngineDeployment.AURORA:
                            await TestEnvironment.RebootAllClusterInstancesAsync();
                            break;
                        default:
                            throw new InvalidOperationException($"Unsupported deployment {deployment}");
                    }

                    Console.WriteLine($"Remaining attempts: {remainingTries}");
                }
            }

            if (!success)
            {
                throw new Exception($"Cluster {TestEnvironment.Env.Info.RdsDbName} is not healthy.");
            }

            Console.WriteLine($"Cluster {TestEnvironment.Env.Info.RdsDbName} is healthy.");
        }
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    public BasicConnectivityTests()
    {
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapperConnectionTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        const string query = "select 1";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);
        AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
        command.CommandText = query;
        connection.Open();
        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlConnectorWrapperConnectionTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        const string query = "select 1";

        using AwsWrapperConnection<MySqlConnection> connection = new(connectionString);
        AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.CommandText = query;
        connection.Open();
        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MysqlWrapperConnectionDynamicTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        connectionString = $"{connectionString};" +
            $"TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;" +
            $"TargetCommandType=MySqlConnector.MySqlCommand,MySqlConnector";

        const string query = "select 1";

        using AwsWrapperConnection connection = new(connectionString);
        IDbCommand command = connection.CreateCommand();
        command.CommandText = query;

        connection.Open();
        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MysqlWrapperConnectionWithParametersTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        const string query = "select @var1";

        using AwsWrapperConnection<MySqlConnection> connection = new(connectionString);
        AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.CommandText = query;

        DbParameter dbParameter = command.CreateParameter();
        dbParameter.ParameterName = "@var1";
        dbParameter.DbType = DbType.String;
        dbParameter.Value = "qwerty";
        command.Parameters.Add(dbParameter);

        connection.Open();
        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal("qwerty", reader.GetString(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapperConnectionTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        const string query = "select 1";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        connection.Open();
        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void OpenPgWrapperConnectionDynamicTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        connectionString = $"{connectionString}" +
            "TargetConnectionType=Npgsql.NpgsqlConnection,Npgsql;" +
            "TargetCommandType=Npgsql.NpgsqlCommand,Npgsql";

        const string query = "select 1";

        using AwsWrapperConnection connection = new(connectionString);
        IDbCommand command = connection.CreateCommand();
        command.CommandText = query;

        connection.Open();
        IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }
}
