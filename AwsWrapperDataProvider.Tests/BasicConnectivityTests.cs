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

namespace AwsWrapperDataProvider.Tests;

public class BasicConnectivityTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public void MySqlClientWrapperConnectionTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        const string query = "select 1";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);
        using AwsWrapperCommand<MySql.Data.MySqlClient.MySqlCommand> command = connection.CreateCommand<MySql.Data.MySqlClient.MySqlCommand>();
        command.CommandText = query;
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using IDataReader reader = command.ExecuteReader();
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
        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.CommandText = query;
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using IDataReader reader = command.ExecuteReader();
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
        connectionString +=
            ";TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;" +
            "TargetCommandType=MySqlConnector.MySqlCommand,MySqlConnector";

        const string query = "select 1";

        using AwsWrapperConnection connection = new(connectionString);
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using IDbCommand command = connection.CreateCommand();
        command.CommandText = query;

        using IDataReader reader = command.ExecuteReader();
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
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.CommandText = query;

        DbParameter dbParameter = command.CreateParameter();
        dbParameter.ParameterName = "@var1";
        dbParameter.DbType = DbType.String;
        dbParameter.Value = "qwerty";
        command.Parameters.Add(dbParameter);

        using IDataReader reader = command.ExecuteReader();
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
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = query;

        using IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public void PgWrapperConnectionDynamicTest()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        connectionString +=
            ";TargetConnectionType=Npgsql.NpgsqlConnection,Npgsql;" +
            "TargetCommandType=Npgsql.NpgsqlCommand,Npgsql";

        const string query = "select 1";

        using AwsWrapperConnection connection = new(connectionString);
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using IDbCommand command = connection.CreateCommand();
        command.CommandText = query;

        using IDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(1, reader.GetInt32(0));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    public async Task MySqlConnectorWrapperProxiedConnectionTest()
    {
        var instanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, instanceInfo.Host, instanceInfo.Port, this.username, this.password, this.defaultDbName);
        connectionString += ";Plugins=";
        const string query = "SELECT @@aurora_server_id";

        using AwsWrapperConnection<MySqlConnection> connection = new(connectionString);
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using (var command = connection.CreateCommand())
        {
            command.CommandText = query;
            command.ExecuteScalar();
        }

        await ProxyHelper.DisableConnectivityAsync(instanceInfo.InstanceId);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = query;
            Console.WriteLine(command.ExecuteScalar());
            //var ex = Assert.Throws<MySqlException>(command.ExecuteScalar);
            //Console.WriteLine("DbException caught:");
            //Console.WriteLine($"Message: {ex.Message}");
            //Console.WriteLine($"Error Code: {ex.ErrorCode}");
            //Console.WriteLine($"Source: {ex.Source}");
            //Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            //Console.WriteLine($"Target Site: {ex.TargetSite}");
        }

        await ProxyHelper.EnableConnectivityAsync(instanceInfo.InstanceId);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public async Task PgWrapperProxiedConnectionTest()
    {
        var instanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, instanceInfo.Host, instanceInfo.Port, this.username, this.password, this.defaultDbName);
        connectionString += ";Plugins=";
        const string query = "select aurora_db_instance_identifier()";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
        using (var command = connection.CreateCommand())
        {
            command.CommandText = query;
            command.ExecuteScalar();
        }

        await ProxyHelper.DisableConnectivityAsync(instanceInfo.InstanceId);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = query;
            Console.WriteLine(command.ExecuteScalar());
            //var ex = Assert.Throws<MySqlException>();
            //Console.WriteLine("DbException caught:");
            //Console.WriteLine($"Message: {ex.Message}");
            //Console.WriteLine($"Error Code: {ex.ErrorCode}");
            //Console.WriteLine($"Source: {ex.Source}");
            //Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            //Console.WriteLine($"Target Site: {ex.TargetSite}");
        }

        await ProxyHelper.EnableConnectivityAsync(instanceInfo.InstanceId);
    }
}
