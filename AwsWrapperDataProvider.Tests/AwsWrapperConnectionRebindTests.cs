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
using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Covers the objects that hold a reference to the target connection and so have to follow it when a
/// plugin switches the current connection.
/// </summary>
/// <remarks>
/// The switch is produced the same way <see cref="AwsWrapperConnectionTest"/> produces it: a mock
/// failover plugin hands back a brand new target connection from OpenConnection, so calling Open()
/// drives a real connection switch through PluginService.SetCurrentConnection.
/// </remarks>
public class AwsWrapperConnectionRebindTests
{
    private const string ConnectionString =
        "Server=<insert_rds_instance_here>;User ID=admin;Password=<password>;Initial Catalog=test;" +
        "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";

    static AwsWrapperConnectionRebindTests()
    {
        MySqlConnectorDialectLoader.Load();
    }

    private static AwsWrapperConnection<MySqlConnection> NewConnectionThatSwitchesOnOpen()
    {
        Dictionary<string, string> props = new()
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "<insert_rds_instance_here>" },
        };

        ConfigurationProfile profile = new(
            "mockFailover",
            [new MockFailoverPluginFactory()],
            props,
            new MySqlDialect(),
            new MySqlConnectorDialect(),
            new DbConnectionProvider());

        return new AwsWrapperConnection<MySqlConnection>(ConnectionString, profile);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateBatch_RegistersBatch_AndDisposeUnregistersIt()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();
        Assert.Empty(connection.ActiveWrapperBatches);

        AwsWrapperBatch batch = connection.CreateBatch();
        Assert.Single(connection.ActiveWrapperBatches);

        batch.Dispose();
        Assert.Empty(connection.ActiveWrapperBatches);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionSwitch_RepointsBatchCreatedBeforeTheSwitch()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperBatch batch = connection.CreateBatch();
        var originalTargetConnection = connection.TargetDbConnection;
        Assert.Same(originalTargetConnection, batch.TargetDbBatch.Connection);

        connection.Open();

        // The batch must be looking at the connection the wrapper is now on. Before this fix it was
        // still holding the pre-switch connection, which SetCurrentConnection had already disposed.
        var updatedTargetConnection = connection.TargetDbConnection;
        Assert.NotSame(originalTargetConnection, updatedTargetConnection);
        Assert.Same(updatedTargetConnection, batch.TargetDbBatch.Connection);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionSwitch_RepointsBatchCreatedAfterTheSwitch()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();
        connection.Open();

        using AwsWrapperBatch batch = connection.CreateBatch();

        Assert.Same(connection.TargetDbConnection, batch.TargetDbBatch.Connection);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionSwitch_KeepsCommandParametersIntact()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.CommandText = "SELECT * FROM users WHERE id = @id AND name = @name";
        command.CommandTimeout = 42;

        var idParameter = command.CreateParameter();
        idParameter.ParameterName = "@id";
        idParameter.DbType = DbType.Int32;
        idParameter.Value = 7;
        command.Parameters.Add(idParameter);

        var nameParameter = command.CreateParameter();
        nameParameter.ParameterName = "@name";
        nameParameter.DbType = DbType.String;
        nameParameter.Value = "alice";
        command.Parameters.Add(nameParameter);

        var originalTargetConnection = connection.TargetDbConnection;

        connection.Open();

        // Re-pointing a command keeps the same target command object, so everything the application
        // set on it survives. This is what lets the wrapper reroute a command without re-creating it
        // and replaying its parameters, so it is asserted rather than assumed.
        Assert.NotSame(originalTargetConnection, connection.TargetDbConnection);
        Assert.Same(connection.TargetDbConnection, command.TargetDbCommand!.Connection);

        Assert.Equal("SELECT * FROM users WHERE id = @id AND name = @name", command.CommandText);
        Assert.Equal(42, command.CommandTimeout);
        Assert.Equal(2, command.Parameters.Count);

        Assert.Equal("@id", command.Parameters[0].ParameterName);
        Assert.Equal(DbType.Int32, command.Parameters[0].DbType);
        Assert.Equal(7, command.Parameters[0].Value);

        Assert.Equal("@name", command.Parameters[1].ParameterName);
        Assert.Equal(DbType.String, command.Parameters[1].DbType);
        Assert.Equal("alice", command.Parameters[1].Value);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ReassigningConnection_MovesCommandRegistrationToTheNewConnection()
    {
        using AwsWrapperConnection<MySqlConnection> first = NewConnectionThatSwitchesOnOpen();
        using AwsWrapperConnection<MySqlConnection> second = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperCommand<MySqlCommand> command = first.CreateCommand<MySqlCommand>();
        Assert.Single(first.ActiveWrapperCommands);
        Assert.Empty(second.ActiveWrapperCommands);

        command.Connection = second;

        // A command that has been moved must not be re-pointed by the connection it left, otherwise a
        // switch on the old connection would drag it back.
        Assert.Empty(first.ActiveWrapperCommands);
        Assert.Single(second.ActiveWrapperCommands);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ReassigningSameConnection_DoesNotRegisterTwice()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        command.Connection = connection;
        command.Connection = connection;

        Assert.Single(connection.ActiveWrapperCommands);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ClearingConnection_UnregistersCommand()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        Assert.Single(connection.ActiveWrapperCommands);

        command.Connection = null;

        Assert.Empty(connection.ActiveWrapperCommands);
    }

    /// <summary>
    /// Stands in for an application that derives from the wrapper connection. Everything the command and
    /// batch setters use is inherited and non-virtual, so a subclass is as valid a target as the two
    /// forms the wrapper creates itself.
    /// </summary>
    private sealed class DerivedWrapperConnection : AwsWrapperConnection
    {
        internal DerivedWrapperConnection(string connectionString, ConfigurationProfile profile)
            : base(connectionString, profile)
        {
        }
    }

    public static TheoryData<string> ConnectionForms => new()
    {
        "nonGeneric",
        "generic",
        "subclass",
    };

    private static AwsWrapperConnection NewConnectionOfForm(string form)
    {
        Dictionary<string, string> props = new()
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "<insert_rds_instance_here>" },
        };

        ConfigurationProfile profile = new(
            "mockFailover",
            [new MockFailoverPluginFactory()],
            props,
            new MySqlDialect(),
            new MySqlConnectorDialect(),
            new DbConnectionProvider());

        return form switch
        {
            "nonGeneric" => new AwsWrapperConnection(ConnectionString, profile),
            "generic" => new AwsWrapperConnection<MySqlConnection>(ConnectionString, profile),
            "subclass" => new DerivedWrapperConnection(ConnectionString, profile),
            _ => throw new ArgumentOutOfRangeException(nameof(form), form, null),
        };
    }

    [Theory]
    [MemberData(nameof(ConnectionForms))]
    [Trait("Category", "Unit")]
    public void AssigningCommandConnection_AcceptsEveryWrapperConnectionForm(string form)
    {
        using AwsWrapperConnection target = NewConnectionOfForm(form);
        using AwsWrapperConnection<MySqlConnection> origin = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperCommand<MySqlCommand> command = origin.CreateCommand<MySqlCommand>();
        command.Connection = target;

        Assert.Same(target, command.Connection);
        Assert.Single(target.ActiveWrapperCommands);
        Assert.Empty(origin.ActiveWrapperCommands);
    }

    [Theory]
    [MemberData(nameof(ConnectionForms))]
    [Trait("Category", "Unit")]
    public void AssigningBatchConnection_AcceptsEveryWrapperConnectionForm(string form)
    {
        using AwsWrapperConnection target = NewConnectionOfForm(form);
        using AwsWrapperConnection<MySqlConnection> origin = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperBatch batch = origin.CreateBatch();
        batch.Connection = target;

        Assert.Same(target, batch.Connection);
        Assert.Single(target.ActiveWrapperBatches);
        Assert.Empty(origin.ActiveWrapperBatches);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void AssigningANonWrapperConnection_IsRejectedByBothCommandAndBatch()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
        using AwsWrapperBatch batch = connection.CreateBatch();

        Assert.Throws<InvalidOperationException>(() => command.Connection = new MySqlConnection());
        Assert.Throws<InvalidOperationException>(() => batch.Connection = new MySqlConnection());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionSwitch_LeavesAnAttachedTransactionAlone()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        DbTransaction targetTransaction = new Mock<DbTransaction>().Object;
        var targetBatch = new Mock<DbBatch>();
        targetBatch.SetupAllProperties();
        targetBatch.Object.Transaction = targetTransaction;

        var batch = new AwsWrapperBatch(targetBatch.Object, connection, connection.PluginManager!);
        batch.SetCurrentConnection(new MySqlConnection());

        // Detaching it would let the next execute succeed under autocommit on the new connection, so an
        // application still holding the transaction would have its statements committed rather than
        // failing. Re-pointing covers the connection only; the transaction is the switching plugin's
        // business.
        Assert.Same(targetTransaction, targetBatch.Object.Transaction);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DisposeAsync_UnregistersBatchFromConnection()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        AwsWrapperBatch batch = connection.CreateBatch();
        Assert.Single(connection.ActiveWrapperBatches);

        await batch.DisposeAsync();

        Assert.Empty(connection.ActiveWrapperBatches);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ConnectionSwitch_DoesNotThrowWhenCommandsAreCreatedConcurrently()
    {
        using AwsWrapperConnection<MySqlConnection> connection = NewConnectionThatSwitchesOnOpen();

        // The switch iterates the registered wrapper objects while the application may still be
        // creating and disposing them, which throws if the live list is iterated directly. The two
        // loops are deliberately unsynchronised and unequal in length so that they overlap wherever
        // the scheduler happens to interleave them.
        Task creator = Task.Run(
            () =>
            {
                for (int i = 0; i < 500; i++)
                {
                    using AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
                }
            },
            TestContext.Current.CancellationToken);

        for (int i = 0; i < 100; i++)
        {
            connection.Open();
        }

        await creator;
    }
}
