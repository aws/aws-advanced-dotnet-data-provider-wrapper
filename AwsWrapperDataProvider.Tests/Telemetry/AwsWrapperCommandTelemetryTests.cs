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
using Apps72.Dev.Data.DbMocker;
using AwsWrapperDataProvider.Dialect.MySqlClient;
using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests.Telemetry;

/// <summary>
/// Tests that verify <see cref="AwsWrapperCommand"/>'s execute methods each
/// open a top-level telemetry trace with the expected span name and
/// attributes, and that the exception path records the exception, sets
/// success=false, and closes the context before rethrowing.
/// </summary>
public class AwsWrapperCommandTelemetryTests
{
    private const string ConnectionString =
        "Server=localhost;User ID=admin;Password=<password>;Port=3306;Initial Catalog=test;" +
        "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";

    static AwsWrapperCommandTelemetryTests()
    {
        MySqlConnectorDialectLoader.Load();
        MySqlClientDialectLoader.Load();
    }

    private static ConfigurationProfile CreateProfile(Dictionary<string, string> props)
        => new(
            "awsWrapperCommandTelemetry",
            [],
            props,
            new MySqlDialect(),
            new MySqlConnectorDialect(),
            new DbConnectionProvider());

    private static Dictionary<string, string> EnabledProps(string backendName) => new()
    {
        { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
        { "Host", "localhost" },
        { "Port", "3306" },
        { "Username", "admin" },
        { "EnableTelemetry", "true" },
        { "TelemetryTracesBackend", backendName },
        { "TelemetrySubmitTopLevel", "true" },
    };

    /// <summary>
    /// Setup for a single test case: registers a mock telemetry factory,
    /// builds an <see cref="AwsWrapperConnection{MySqlConnection}"/> (without
    /// opening it), and creates an <see cref="AwsWrapperCommand"/> bound to
    /// a DbMocker command so that the execute methods have a working
    /// in-memory target.
    ///
    /// <para>The factory routes the top-level command span (name = <paramref
    /// name="topLevelSpanName"/> supplied to the harness ctor) to the context
    /// exposed as <see cref="MockContext"/>; every other span (per-plugin
    /// nested spans opened by <c>ConnectionPluginManager</c>) goes to a
    /// throw-away context so lifecycle assertions on <c>MockContext</c>
    /// aren't polluted by those spans.</para>
    /// </summary>
    private sealed class TestHarness : IDisposable
    {
        public string BackendName { get; }

        public Mock<ITelemetryFactory> MockFactory { get; } = new();

        public Mock<ITelemetryContext> MockContext { get; } = new();

        private Mock<ITelemetryContext> OtherContext { get; } = new();

        public AwsWrapperConnection<MySqlConnection> Connection { get; }

        public MockDbConnection MockedTarget { get; } = new();

        public AwsWrapperCommand Command { get; }

        public TestHarness(Action<MockDbConnection> configureMock, string topLevelSpanName)
        {
            this.BackendName = "T8MOCK" + Guid.NewGuid().ToString("N");
            this.MockFactory
                .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
                .Returns<string, TelemetryTraceLevel>((name, _) =>
                    name == topLevelSpanName ? this.MockContext.Object : this.OtherContext.Object);
            DefaultTelemetryFactory.RegisterTelemetryFactory(this.BackendName, this.MockFactory.Object);

            configureMock(this.MockedTarget);

            // Constructing AwsWrapperConnection runs InitializeConnection,
            // which creates the PluginService (and wires TelemetryFactory to
            // the registered mock backend). No Open() required because we
            // bypass the target connection by binding the wrapped command
            // directly to a DbMocker MockDbCommand.
            this.Connection = new AwsWrapperConnection<MySqlConnection>(
                ConnectionString,
                CreateProfile(EnabledProps(this.BackendName)));

            DbCommand mockedCmd = this.MockedTarget.CreateCommand();
            this.Command = new AwsWrapperCommand(mockedCmd, this.Connection);
        }

        public void Dispose()
        {
            this.Command.Dispose();
            this.Connection.Dispose();
            this.MockedTarget.Dispose();
            DefaultTelemetryFactory.UnregisterTelemetryFactory(this.BackendName);
        }
    }

    private static void VerifySuccess(
        Mock<ITelemetryFactory> mockFactory,
        Mock<ITelemetryContext> mockContext,
        string expectedSpanName)
    {
        mockFactory.Verify(
            f => f.OpenTelemetryContext(expectedSpanName, TelemetryTraceLevel.TopLevel),
            Times.Once);

        // Matches JDBC's jdbcCall and Go's sqlCall: the command span carries a
        // `dbCall` attribute equal to the span name.
        mockContext.Verify(
            c => c.SetAttribute(WrapperUtils.DbCallAttribute, expectedSpanName),
            Times.Once);

        mockContext.Verify(c => c.SetSuccess(true), Times.Once);
        mockContext.Verify(c => c.SetException(It.IsAny<Exception>()), Times.Never);
        mockContext.Verify(c => c.CloseContext(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ExecuteNonQuery_OnSuccess_CreatesTopLevelSpan()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ReturnsScalar(42),
            "DbCommand.ExecuteNonQuery");
        harness.Command.CommandText = "INSERT INTO t VALUES (1);";

        int affected = harness.Command.ExecuteNonQuery();

        Assert.Equal(42, affected);
        VerifySuccess(harness.MockFactory, harness.MockContext, "DbCommand.ExecuteNonQuery");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ExecuteNonQueryAsync_OnSuccess_CreatesTopLevelSpan()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ReturnsScalar(7),
            "DbCommand.ExecuteNonQueryAsync");
        harness.Command.CommandText = "UPDATE t SET v=1;";

        int affected = await harness.Command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        Assert.Equal(7, affected);
        VerifySuccess(harness.MockFactory, harness.MockContext, "DbCommand.ExecuteNonQueryAsync");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ExecuteScalar_OnSuccess_CreatesTopLevelSpan()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ReturnsScalar("hello"),
            "DbCommand.ExecuteScalar");
        harness.Command.CommandText = "SELECT 'hello';";

        object? value = harness.Command.ExecuteScalar();

        Assert.Equal("hello", value);
        VerifySuccess(harness.MockFactory, harness.MockContext, "DbCommand.ExecuteScalar");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ExecuteScalarAsync_OnSuccess_CreatesTopLevelSpan()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ReturnsScalar("async-hi"),
            "DbCommand.ExecuteScalarAsync");
        harness.Command.CommandText = "SELECT 'async-hi';";

        object? value = await harness.Command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

        Assert.Equal("async-hi", value);
        VerifySuccess(harness.MockFactory, harness.MockContext, "DbCommand.ExecuteScalarAsync");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ExecuteReader_OnSuccess_CreatesTopLevelSpan()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("id").AddRow(1)),
            "DbCommand.ExecuteReader");
        harness.Command.CommandText = "SELECT id FROM t;";

        using DbDataReader reader = harness.Command.ExecuteReader();

        // Do NOT call reader.Read() here: Read() now has its own top-level
        // telemetry span via WrapperUtils (which is the whole point of the
        // refactor), and the shared mock context would see multiple
        // SetSuccess calls. This test verifies the ExecuteReader span only.
        Assert.NotNull(reader);
        VerifySuccess(harness.MockFactory, harness.MockContext, "DbCommand.ExecuteReader");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ExecuteReaderAsync_OnSuccess_CreatesTopLevelSpan()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("id").AddRow(1)),
            "DbCommand.ExecuteReaderAsync");
        harness.Command.CommandText = "SELECT id FROM t;";

        await using DbDataReader reader =
            await harness.Command.ExecuteReaderAsync(TestContext.Current.CancellationToken);

        // See note in ExecuteReader_OnSuccess_CreatesTopLevelSpan — we verify
        // only the ExecuteReaderAsync span; row reading has its own span now.
        Assert.NotNull(reader);
        VerifySuccess(harness.MockFactory, harness.MockContext, "DbCommand.ExecuteReaderAsync");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ExecuteNonQuery_OnException_RecordsExceptionSetsSuccessFalseAndCloses()
    {
        using TestHarness harness = new(
            mock => mock.Mocks.WhenAny().ThrowsException(new InvalidOperationException("boom")),
            "DbCommand.ExecuteNonQuery");
        harness.Command.CommandText = "THROW;";

        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(
            () => harness.Command.ExecuteNonQuery());
        Assert.Equal("boom", thrown.Message);

        harness.MockFactory.Verify(
            f => f.OpenTelemetryContext("DbCommand.ExecuteNonQuery", TelemetryTraceLevel.TopLevel),
            Times.Once);
        harness.MockContext.Verify(
            c => c.SetException(It.Is<InvalidOperationException>(e => e.Message == "boom")),
            Times.Once);
        harness.MockContext.Verify(c => c.SetSuccess(false), Times.Once);
        harness.MockContext.Verify(c => c.SetSuccess(true), Times.Never);
        harness.MockContext.Verify(c => c.CloseContext(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Execute_WithTelemetryDisabled_StillWorks()
    {
        // No EnableTelemetry key → PluginService uses NullTelemetryFactory.
        Dictionary<string, string> props = new()
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "localhost" },
        };
        using AwsWrapperConnection<MySqlConnection> connection =
            new(ConnectionString, CreateProfile(props));

        using MockDbConnection mocked = new();
        mocked.Mocks.WhenAny().ReturnsScalar(13);
        using AwsWrapperCommand command = new(mocked.CreateCommand(), connection);
        command.CommandText = "SELECT 1;";

        Exception? caught = Record.Exception(() => command.ExecuteScalar());
        Assert.Null(caught);
    }
}
