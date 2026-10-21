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
using System.Diagnostics.CodeAnalysis;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;
using Moq.Protected;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Covers how the caller's <see cref="CancellationToken"/> travels from each async wrapper entry point to the target
/// driver. An already-cancelled token must fail the call before any plugin runs, and a live token must reach the
/// driver call that does the I/O.
/// </summary>
public class CancellationTokenTests
{
    private const string SavepointName = "test_savepoint";
    private const string OpenAsyncMethod = "DbConnection.OpenAsync";
    private const string FakeDriverConnectionString = "Host=fake-host;Port=5432;Database=test;Username=user;Password=password;";

    private readonly Mock<ConnectionPluginManager> mockPluginManager;
    private readonly Mock<DbCommand> mockTargetCommand = new();
    private readonly Mock<DbBatch> mockTargetBatch = new();
    private readonly Mock<DbDataReader> mockTargetReader = new();
    private readonly Mock<DbTransaction> mockTargetTransaction = new();
    private readonly AwsWrapperCommand command;
    private readonly AwsWrapperBatch batch;
    private readonly AwsWrapperDataReader reader;
    private readonly AwsWrapperTransaction transaction;

    public CancellationTokenTests()
    {
        NpgsqlDialectLoader.Load();
        AwsWrapperConnection<NpgsqlConnection> connection = new("Host=192.0.0.1;Database=test;Username=user;Password=password;");

        this.mockPluginManager = new Mock<ConnectionPluginManager>(
            new Mock<IConnectionProvider>().Object,
            new Mock<IConnectionProvider>().Object,
            new List<IConnectionPlugin> { new TestPluginOne([]) },
            connection)
        {
            CallBase = true,
        };

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.SetupGet(s => s.CurrentTransaction).Returns(this.mockTargetTransaction.Object);

        this.command = new AwsWrapperCommand(this.mockTargetCommand.Object, connection, this.mockPluginManager.Object);
        this.batch = new AwsWrapperBatch(this.mockTargetBatch.Object, connection, this.mockPluginManager.Object);
        this.reader = new AwsWrapperDataReader(this.mockTargetReader.Object, this.mockPluginManager.Object);
        this.transaction = new AwsWrapperTransaction(connection, mockPluginService.Object, this.mockPluginManager.Object);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbCommand.ExecuteNonQueryAsync")]
    [InlineData("DbCommand.ExecuteScalarAsync")]
    [InlineData("DbCommand.ExecuteReaderAsync")]
    [InlineData("DbCommand.PrepareAsync")]
    [InlineData("DbBatch.ExecuteNonQueryAsync")]
    [InlineData("DbBatch.ExecuteScalarAsync")]
    [InlineData("DbBatch.ExecuteReaderAsync")]
    [InlineData("DbBatch.PrepareAsync")]
    [InlineData("DbDataReader.ReadAsync")]
    [InlineData("DbDataReader.NextResultAsync")]
    [InlineData("DbDataReader.IsDBNullAsync")]
    [InlineData("DbTransaction.CommitAsync")]
    [InlineData("DbTransaction.RollbackAsync")]
    [InlineData("DbTransaction.SaveAsync")]
    [InlineData("DbTransaction.RollbackAsync(savepoint)")]
    [InlineData("DbTransaction.ReleaseAsync")]
    public async Task ExecuteEntryPoint_PreCancelledToken_ReturnsCanceledTaskWithoutRunningPlugins(string method)
    {
        WrapperCall call = this.GetExecuteCall(method);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        Task task = call.Invoke(cts.Token);

        Assert.True(task.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        this.mockPluginManager.Verify(
            p => p.Execute(It.IsAny<object>(), It.IsAny<string>(), It.IsAny<ADONetDelegate<It.IsAnyType>>(), It.IsAny<object[]>()),
            Times.Never);
        call.VerifyTarget(_ => true, Times.Never());
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbCommand.ExecuteNonQueryAsync")]
    [InlineData("DbCommand.ExecuteScalarAsync")]
    [InlineData("DbCommand.ExecuteReaderAsync")]
    [InlineData("DbCommand.PrepareAsync")]
    [InlineData("DbBatch.ExecuteNonQueryAsync")]
    [InlineData("DbBatch.ExecuteScalarAsync")]
    [InlineData("DbBatch.ExecuteReaderAsync")]
    [InlineData("DbBatch.PrepareAsync")]
    [InlineData("DbDataReader.ReadAsync")]
    [InlineData("DbDataReader.NextResultAsync")]
    [InlineData("DbDataReader.IsDBNullAsync")]
    [InlineData("DbTransaction.CommitAsync")]
    [InlineData("DbTransaction.RollbackAsync")]
    [InlineData("DbTransaction.SaveAsync")]
    [InlineData("DbTransaction.RollbackAsync(savepoint)")]
    [InlineData("DbTransaction.ReleaseAsync")]
    public async Task ExecuteEntryPoint_PassesCallersTokenToTargetDriver(string method)
    {
        WrapperCall call = this.GetExecuteCall(method);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        CancellationToken token = cts.Token;

        await call.Invoke(token);

        call.VerifyTarget(t => t == token, Times.Once());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_PreCancelledToken_ReturnsCanceledTaskWithoutRunningPluginsOrDriver()
    {
        List<string> calls = [];
        FakeDriver driver = new();
        using AwsWrapperConnection connection = CreateFakeDriverConnection(driver, calls);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        Task task = connection.OpenAsync(cts.Token);

        Assert.True(task.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.Empty(calls);
        Assert.DoesNotContain(driver.Connections, c => c.Tokens.ContainsKey(OpenAsyncMethod));
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_PassesCallersTokenThroughPluginsToDriver()
    {
        List<string> calls = [];
        FakeDriver driver = new();
        using AwsWrapperConnection connection = CreateFakeDriverConnection(driver, calls);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        await connection.OpenAsync(cts.Token);

        // The token has to pass through a plugin that does not know about it to reach the driver.
        Assert.Contains("TestPluginOne:before open", calls);
        FakeDriverConnection opened = Assert.Single(driver.Connections, c => c.Tokens.ContainsKey(OpenAsyncMethod));
        Assert.Equal(cts.Token, opened.Tokens[OpenAsyncMethod]);
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_CancelledWhileDriverIsConnecting_ThrowsOperationCanceledException()
    {
        TaskCompletionSource connecting = new(TaskCreationOptions.RunContinuationsAsynchronously);
        FakeDriver driver = new()
        {
            // Bounded, so that a token that never reaches the driver fails the test instead of hanging it.
            OnOpenAsync = async token =>
            {
                connecting.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(10), token);
            },
        };
        using AwsWrapperConnection connection = CreateFakeDriverConnection(driver, []);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        Task open = connection.OpenAsync(cts.Token);
        await connecting.Task.WaitAsync(TestContext.Current.CancellationToken);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => open);
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_DriverFailsAfterCancellation_ReportsCancellationInsteadOfDriverError()
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        FakeDriver driver = new()
        {
            // A cancelled connect can surface from the driver as a connection error rather than a cancellation.
            // SQLSTATE 08001 is exactly what the plugins above would otherwise treat as an unreachable host.
            OnOpenAsync = async _ =>
            {
                await cts.CancelAsync();
                throw new FakeDbException("08001");
            },
        };
        using AwsWrapperConnection connection = CreateFakeDriverConnection(driver, []);

        OperationCanceledException exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connection.OpenAsync(cts.Token));

        Assert.Equal(cts.Token, exception.CancellationToken);

        // Plugins classify by walking inner exceptions, so the driver error must not be chained.
        Assert.Null(exception.InnerException);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_DriverFailsWithoutCancellation_SurfacesDriverError()
    {
        FakeDbException driverError = new("08001");
        FakeDriver driver = new() { OnOpenAsync = _ => Task.FromException(driverError) };
        using AwsWrapperConnection connection = CreateFakeDriverConnection(driver, []);

        FakeDbException exception = await Assert.ThrowsAsync<FakeDbException>(() => connection.OpenAsync(TestContext.Current.CancellationToken));

        Assert.Same(driverError, exception);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_ReopenAfterFailedPing_PassesCallersTokenToDriver()
    {
        // The first ping fails, so the default plugin discards that connection and opens a second one.
        FakeDriver driver = new(false, true);
        using AwsWrapperConnection connection = CreateFakeDriverConnection(driver, []);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        await connection.OpenAsync(cts.Token);

        List<CancellationToken> openTokens = driver.Connections
            .Where(c => c.Tokens.ContainsKey(OpenAsyncMethod))
            .Select(c => c.Tokens[OpenAsyncMethod])
            .ToList();
        Assert.Equal(2, openTokens.Count);
        Assert.All(openTokens, t => Assert.Equal(cts.Token, t));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbConnection.ChangeDatabaseAsync")]
    [InlineData("DbConnection.GetSchemaAsync")]
    [InlineData("DbConnection.GetSchemaAsync(collection)")]
    [InlineData("DbConnection.GetSchemaAsync(collection, restrictions)")]
    [InlineData("DbConnection.BeginTransactionAsync")]
    public async Task ConnectionEntryPoint_PreCancelledToken_ReturnsCanceledTaskWithoutRunningPlugins(string method)
    {
        List<string> calls = [];
        using AwsWrapperConnection connection = CreateFakeDriverConnection(new FakeDriver(), calls);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        calls.Clear();
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        Task task = InvokeConnectionMethod(connection, method, cts.Token);

        Assert.True(task.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.Empty(calls);
        Assert.False(((FakeDriverConnection)connection.TargetDbConnection!).Tokens.ContainsKey(method));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbConnection.ChangeDatabaseAsync")]
    [InlineData("DbConnection.GetSchemaAsync")]
    [InlineData("DbConnection.GetSchemaAsync(collection)")]
    [InlineData("DbConnection.GetSchemaAsync(collection, restrictions)")]
    [InlineData("DbConnection.BeginTransactionAsync")]
    public async Task ConnectionEntryPoint_PassesCallersTokenToTargetDriver(string method)
    {
        using AwsWrapperConnection connection = CreateFakeDriverConnection(new FakeDriver(), []);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        await InvokeConnectionMethod(connection, method, cts.Token);

        Assert.Equal(cts.Token, ((FakeDriverConnection)connection.TargetDbConnection!).Tokens[method]);
    }

    private static Task InvokeConnectionMethod(AwsWrapperConnection connection, string method, CancellationToken token) => method switch
    {
        "DbConnection.ChangeDatabaseAsync" => connection.ChangeDatabaseAsync("other", token),
        "DbConnection.GetSchemaAsync" => connection.GetSchemaAsync(token),
        "DbConnection.GetSchemaAsync(collection)" => connection.GetSchemaAsync("Tables", token),
        "DbConnection.GetSchemaAsync(collection, restrictions)" => connection.GetSchemaAsync("Tables", [null], token),
        "DbConnection.BeginTransactionAsync" => connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token).AsTask(),
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, null),
    };

    /// <summary>
    /// Builds a wrapper connection over <see cref="FakeDriverConnection"/>, with <see cref="TestPluginOne"/> recording
    /// into <paramref name="calls"/> ahead of the default plugin.
    /// </summary>
    private static AwsWrapperConnection CreateFakeDriverConnection(FakeDriver driver, List<string> calls)
    {
        ConfigurationProfile profile = ConfigurationProfileBuilder.Get()
            .WithName("cancellation-token-tests")
            .WithDialect(new FakeDriverDatabaseDialect())
            .WithTargetConnectionDialect(driver)
            .WithPluginFactories([new RecordingPluginFactory(calls)])
            .Build();

        return new AwsWrapperConnection(typeof(FakeDriverConnection), FakeDriverConnectionString, profile);
    }

    private WrapperCall GetExecuteCall(string method) => method switch
    {
        "DbCommand.ExecuteNonQueryAsync" => new(
            token => this.command.ExecuteNonQueryAsync(token),
            (matches, times) => this.mockTargetCommand.Verify(c => c.ExecuteNonQueryAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbCommand.ExecuteScalarAsync" => new(
            token => this.command.ExecuteScalarAsync(token),
            (matches, times) => this.mockTargetCommand.Verify(c => c.ExecuteScalarAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbCommand.ExecuteReaderAsync" => new(
            token => this.command.ExecuteReaderAsync(CommandBehavior.Default, token),
            (matches, times) => this.mockTargetCommand.Protected().Verify<Task<DbDataReader>>(
                "ExecuteDbDataReaderAsync", times, ItExpr.IsAny<CommandBehavior>(), ItExpr.Is<CancellationToken>(t => matches(t)))),
        "DbCommand.PrepareAsync" => new(
            token => this.command.PrepareAsync(token),
            (matches, times) => this.mockTargetCommand.Verify(c => c.PrepareAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbBatch.ExecuteNonQueryAsync" => new(
            token => this.batch.ExecuteNonQueryAsync(token),
            (matches, times) => this.mockTargetBatch.Verify(b => b.ExecuteNonQueryAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbBatch.ExecuteScalarAsync" => new(
            token => this.batch.ExecuteScalarAsync(token),
            (matches, times) => this.mockTargetBatch.Verify(b => b.ExecuteScalarAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbBatch.ExecuteReaderAsync" => new(
            token => this.batch.ExecuteReaderAsync(CommandBehavior.Default, token),
            (matches, times) => this.mockTargetBatch.Protected().Verify<Task<DbDataReader>>(
                "ExecuteDbDataReaderAsync", times, ItExpr.IsAny<CommandBehavior>(), ItExpr.Is<CancellationToken>(t => matches(t)))),
        "DbBatch.PrepareAsync" => new(
            token => this.batch.PrepareAsync(token),
            (matches, times) => this.mockTargetBatch.Verify(b => b.PrepareAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbDataReader.ReadAsync" => new(
            token => this.reader.ReadAsync(token),
            (matches, times) => this.mockTargetReader.Verify(r => r.ReadAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbDataReader.NextResultAsync" => new(
            token => this.reader.NextResultAsync(token),
            (matches, times) => this.mockTargetReader.Verify(r => r.NextResultAsync(It.Is<CancellationToken>(t => matches(t))), times)),
        "DbDataReader.IsDBNullAsync" => new(
            token => this.reader.IsDBNullAsync(0, token),
            (matches, times) => this.mockTargetReader.Verify(r => r.IsDBNullAsync(0, It.Is<CancellationToken>(t => matches(t))), times)),
        "DbTransaction.CommitAsync" => new(
            token => this.transaction.CommitAsync(token),
            (matches, times) => this.mockTargetTransaction.Verify(t => t.CommitAsync(It.Is<CancellationToken>(c => matches(c))), times)),
        "DbTransaction.RollbackAsync" => new(
            token => this.transaction.RollbackAsync(token),
            (matches, times) => this.mockTargetTransaction.Verify(t => t.RollbackAsync(It.Is<CancellationToken>(c => matches(c))), times)),
        "DbTransaction.SaveAsync" => new(
            token => this.transaction.SaveAsync(SavepointName, token),
            (matches, times) => this.mockTargetTransaction.Verify(t => t.SaveAsync(SavepointName, It.Is<CancellationToken>(c => matches(c))), times)),
        "DbTransaction.RollbackAsync(savepoint)" => new(
            token => this.transaction.RollbackAsync(SavepointName, token),
            (matches, times) => this.mockTargetTransaction.Verify(t => t.RollbackAsync(SavepointName, It.Is<CancellationToken>(c => matches(c))), times)),
        "DbTransaction.ReleaseAsync" => new(
            token => this.transaction.ReleaseAsync(SavepointName, token),
            (matches, times) => this.mockTargetTransaction.Verify(t => t.ReleaseAsync(SavepointName, It.Is<CancellationToken>(c => matches(c))), times)),
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, null),
    };

    /// <summary>
    /// An async wrapper entry point, paired with a check of how often the target driver object was called with a
    /// token that satisfies a predicate.
    /// </summary>
    private sealed record WrapperCall(Func<CancellationToken, Task> Invoke, Action<Func<CancellationToken, bool>, Times> VerifyTarget);

    private sealed class RecordingPluginFactory(List<string> calls) : IConnectionPluginFactory
    {
        public IConnectionPlugin GetInstance(FullServicesContainer servicesContainer, Dictionary<string, string> props) => new TestPluginOne(calls);
    }

    /// <summary>
    /// Database dialect for the fake driver. It accepts any connection, so dialect detection does not run queries
    /// against <see cref="FakeDriverConnection"/>.
    /// </summary>
    private sealed class FakeDriverDatabaseDialect : PgDialect
    {
        public override IList<Type> DialectUpdateCandidates { get; } = [];

        public override Task<bool> IsDialect(DbConnection conn) => Task.FromResult(true);
    }

    /// <summary>
    /// Target driver dialect that hands out a new <see cref="FakeDriverConnection"/> for every connection the wrapper
    /// creates, and answers pings from <paramref name="pingResults"/> before reporting every connection alive.
    /// </summary>
    private sealed class FakeDriver(params bool[] pingResults) : GenericTargetConnectionDialect(typeof(FakeDriverConnection))
    {
        private readonly Queue<bool> pingResults = new(pingResults);

        public Func<CancellationToken, Task> OnOpenAsync { get; init; } = _ => Task.CompletedTask;

        public List<FakeDriverConnection> Connections { get; } = [];

        public override DbConnection CreateConnection(Type connectionType, string connectionString, Dictionary<string, string> props)
        {
            FakeDriverConnection connection = new(this.OnOpenAsync);
            this.Connections.Add(connection);
            return connection;
        }

        public override (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection)
        {
            return (this.pingResults.Count == 0 || this.pingResults.Dequeue(), null);
        }
    }

    /// <summary>
    /// Stands in for a target driver connection and records the token each async method receives, keyed by the
    /// wrapper method that should forward it.
    /// </summary>
    private sealed class FakeDriverConnection(Func<CancellationToken, Task> onOpenAsync) : DbConnection
    {
        private ConnectionState state = ConnectionState.Closed;

        public Dictionary<string, CancellationToken> Tokens { get; } = [];

        [AllowNull]
        public override string ConnectionString { get; set; } = string.Empty;

        public override string Database => "test";

        public override string DataSource => "fake-host";

        public override string ServerVersion => "1.0";

        public override ConnectionState State => this.state;

        public override void ChangeDatabase(string databaseName)
        {
        }

        public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            this.Tokens["DbConnection.ChangeDatabaseAsync"] = cancellationToken;
            return Task.CompletedTask;
        }

        public override void Close() => this.state = ConnectionState.Closed;

        public override void Open() => this.state = ConnectionState.Open;

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            this.Tokens[OpenAsyncMethod] = cancellationToken;
            await onOpenAsync(cancellationToken);
            this.state = ConnectionState.Open;
        }

        public override Task<DataTable> GetSchemaAsync(CancellationToken cancellationToken = default)
        {
            this.Tokens["DbConnection.GetSchemaAsync"] = cancellationToken;
            return Task.FromResult(new DataTable());
        }

        public override Task<DataTable> GetSchemaAsync(string collectionName, CancellationToken cancellationToken = default)
        {
            this.Tokens["DbConnection.GetSchemaAsync(collection)"] = cancellationToken;
            return Task.FromResult(new DataTable());
        }

        public override Task<DataTable> GetSchemaAsync(string collectionName, string?[] restrictionValues, CancellationToken cancellationToken = default)
        {
            this.Tokens["DbConnection.GetSchemaAsync(collection, restrictions)"] = cancellationToken;
            return Task.FromResult(new DataTable());
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new Mock<DbTransaction>().Object;

        protected override ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            this.Tokens["DbConnection.BeginTransactionAsync"] = cancellationToken;
            return ValueTask.FromResult(new Mock<DbTransaction>().Object);
        }

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeDbException(string sqlState) : DbException
    {
        public override string SqlState => sqlState;
    }
}
