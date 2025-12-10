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

using AwsWrapperDataProvider.Dialect.MySqlClient;
using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperConnectionTest
{
    static AwsWrapperConnectionTest()
    {
        MySqlConnectorDialectLoader.Load();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithTypeArgument_SetsTargetConnectionTypeInProperties()
    {
        AwsWrapperConnection<MySqlConnection> connection =
            new("Server=<insert_rds_instance_here>;User ID=admin;Password=<password>;Initial Catalog=test;");

        string typeString = PropertyDefinition.TargetConnectionType.GetString(connection.ConnectionProperties!)!;
        Type? targetConnectionType = Type.GetType(typeString);

        Assert.Equal(typeof(MySqlConnection), targetConnectionType);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithTargetConnectionTypeInConnectionString_SetsTargetConnectionTypeInProperties()
    {
        AwsWrapperConnection connection =
            new("Server=<insert_rds_instance_here>;User ID=admin;Password=<password>;Initial Catalog=test;" +
                "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;");

        string typeString = PropertyDefinition.TargetConnectionType.GetString(connection.ConnectionProperties!)!;
        Type? targetConnectionType = Type.GetType(typeString)!;

        Assert.Equal(typeof(MySqlConnection), targetConnectionType);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void StaleDNS_OpenAfterCreatingCommand()
    {
        var connectionString = "Server=<insert_rds_instance_here>;User ID=admin;Password=<password>;Initial Catalog=test;" +
                "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";
        Dictionary<string, string> props = new()
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "<insert_rds_instance_here>" },
        };

        // Using the mock failover plugin that always returns a new connection to simulate stale DNS
        ConfigurationProfile profile = new("mockFailover", [new MockFailoverPluginFactory()], props, new MySqlDialect(), new MySqlConnectorDialect(), new DbConnectionProvider());

        using AwsWrapperConnection<MySqlConnection> connection = new(connectionString, profile);
        Assert.Empty(connection.ActiveWrapperCommands);
        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        Assert.Single(connection.ActiveWrapperCommands);

        var originalTargetConnection = connection.TargetDbConnection;
        Assert.Same(connection.TargetDbConnection, command.TargetDbConnection);
        Assert.Same(connection.TargetDbConnection, command.TargetDbCommand!.Connection);

        connection.Open();

        var updatedTargetConnection = connection.TargetDbConnection;
        Assert.Same(connection.TargetDbConnection, command.TargetDbConnection);
        Assert.Same(connection.TargetDbConnection, command.TargetDbCommand!.Connection);

        Assert.NotSame(originalTargetConnection, updatedTargetConnection);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void StaleDNS_OpenBeforeCreatingCommand()
    {
        var connectionString = "Server=<insert_rds_instance_here>;User ID=admin;Password=<password>;Initial Catalog=test;" +
                "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";
        Dictionary<string, string> props = new()
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "<insert_rds_instance_here>" },
        };

        // Using the mock failover plugin that always returns a new connection to simulate stale DNS
        ConfigurationProfile profile = new("mockFailover", [new MockFailoverPluginFactory()], props, new MySqlDialect(), new MySqlConnectorDialect(), new DbConnectionProvider());

        using AwsWrapperConnection<MySqlConnection> connection = new(connectionString, profile);
        var originalTargetConnection = connection.TargetDbConnection;
        connection.Open();
        Assert.Empty(connection.ActiveWrapperCommands);

        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        Assert.Single(connection.ActiveWrapperCommands);

        var updatedTargetConnection = connection.TargetDbConnection;
        Assert.Same(connection.TargetDbConnection, command.TargetDbConnection);
        Assert.Same(connection.TargetDbConnection, command.TargetDbCommand!.Connection);

        Assert.NotSame(originalTargetConnection, updatedTargetConnection);
    }
}
