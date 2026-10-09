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
using System.Net.Sockets;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// A cancelled call must surface as a cancellation and must not start a failover, even when the driver chains an
/// exception inside the cancellation that the failover plugin would otherwise read as a network failure.
/// </summary>
public class FailoverPluginCancellationTests
{
    private const string MethodName = "DbCommand.ExecuteNonQueryAsync";

    private static readonly HostSpec Writer = new("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, "instance-1", HostRole.Writer, HostAvailability.Available);

    private readonly Mock<IPluginService> mockPluginService = new();
    private readonly Mock<IHostListProviderService> mockHostListProviderService = new();
    private readonly Mock<DbConnection> mockConnection = new();
    private readonly Dictionary<string, string> props = new()
    {
        { PropertyDefinition.EnableConnectFailover.Name, "true" },
    };

    private readonly FailoverPlugin plugin;

    public FailoverPluginCancellationTests()
    {
        // Classify exceptions exactly as the PostgreSQL dialect does, so an exception chained inside a cancellation
        // is seen the way the plugin sees it in production.
        PgExceptionHandler exceptionHandler = new();
        this.mockPluginService.Setup(s => s.IsNetworkException(It.IsAny<Exception>())).Returns<Exception>(exceptionHandler.IsNetworkException);
        this.mockPluginService.SetupGet(s => s.TelemetryFactory).Returns(NullTelemetryFactory.Instance);
        this.mockPluginService.SetupGet(s => s.AllHosts).Returns([Writer]);
        this.mockPluginService.Setup(s => s.GetHosts()).Returns([Writer]);
        this.mockPluginService.SetupGet(s => s.CurrentHostSpec).Returns(Writer);
        this.mockPluginService.SetupGet(s => s.CurrentConnection).Returns(this.mockConnection.Object);

        // A failover gets as far as connecting to the writer, which fails here, so a test that expects a failover
        // sees FailoverFailedException.
        this.mockPluginService
            .Setup(s => s.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()))
            .ThrowsAsync(new InvalidOperationException("No new connection in this test."));

        this.mockConnection.SetupGet(c => c.State).Returns(ConnectionState.Open);
        this.mockHostListProviderService.SetupGet(s => s.InitialConnectionHostSpec).Returns(Writer);

        this.plugin = new FailoverPlugin(this.mockPluginService.Object, this.props);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_CancelledCallWithTimeoutInside_RethrowsWithoutFailingOver()
    {
        await this.OpenInitialConnectionAsync();

        // What Npgsql throws when the server never answers its cancel request: the cancellation, with the read
        // timeout chained inside.
        OperationCanceledException cancellation = new("Query was cancelled", new TimeoutException("Timeout during reading attempt"), new CancellationToken(true));

        OperationCanceledException thrown = await Assert.ThrowsAsync<OperationCanceledException>(
            () => this.plugin.Execute(this.mockConnection.Object, MethodName, () => Task.FromException<int>(cancellation)));

        Assert.Same(cancellation, thrown);
        this.VerifyNoFailover();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_TimeoutWithoutCancellation_StartsFailover()
    {
        await this.OpenInitialConnectionAsync();

        await Assert.ThrowsAsync<FailoverFailedException>(
            () => this.plugin.Execute(this.mockConnection.Object, MethodName, () => Task.FromException<int>(new TimeoutException("Timeout during reading attempt"))));

        this.mockPluginService.Verify(s => s.SetAvailability(Writer, HostAvailability.Unavailable), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_CancelNotAcknowledgedInTransaction_NextCallReportsTransactionLost()
    {
        await this.OpenInitialConnectionAsync();
        this.AllowFailoverToSucceed();
        this.mockPluginService.SetupProperty(s => s.CurrentTransaction, new Mock<DbTransaction>().Object);
        OperationCanceledException cancellation = new("Query was cancelled", new TimeoutException("Timeout during reading attempt"), new CancellationToken(true));

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => this.plugin.Execute(this.mockConnection.Object, MethodName, () => Task.FromException<int>(cancellation)));

        // Npgsql breaks the connection when its cancel request goes unanswered, so the next call finds it closed,
        // and the transaction is gone with it.
        this.mockConnection.SetupGet(c => c.State).Returns(ConnectionState.Closed);

        await Assert.ThrowsAsync<TransactionStateUnknownException>(
            () => this.plugin.Execute(this.mockConnection.Object, MethodName, () => Task.FromResult(1)));

        Assert.Null(this.mockPluginService.Object.CurrentTransaction);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ConnectionFoundClosedOutsideTransaction_ThrowsFailoverSuccessException()
    {
        await this.OpenInitialConnectionAsync();
        this.AllowFailoverToSucceed();
        this.mockConnection.SetupGet(c => c.State).Returns(ConnectionState.Closed);

        await Assert.ThrowsAsync<FailoverSuccessException>(
            () => this.plugin.Execute(this.mockConnection.Object, MethodName, () => Task.FromResult(1)));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_CancelledWithSocketErrorInside_RethrowsWithoutFailingOver()
    {
        await this.InitHostProviderAsync();
        OperationCanceledException cancellation = new("The operation was canceled.", new SocketException((int)SocketError.OperationAborted), new CancellationToken(true));

        OperationCanceledException thrown = await Assert.ThrowsAsync<OperationCanceledException>(
            () => this.plugin.OpenConnection(Writer, this.props, true, () => Task.FromException<DbConnection>(cancellation), true));

        Assert.Same(cancellation, thrown);
        this.VerifyNoFailover();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_SocketErrorWithoutCancellation_StartsFailover()
    {
        await this.InitHostProviderAsync();

        await Assert.ThrowsAsync<FailoverFailedException>(
            () => this.plugin.OpenConnection(Writer, this.props, true, () => Task.FromException<DbConnection>(new SocketException((int)SocketError.ConnectionRefused)), true));

        this.mockPluginService.Verify(s => s.SetAvailability(Writer, HostAvailability.Unavailable), Times.Once);
    }

    /// <summary>
    /// Lets a failover reconnect to the writer, so a test can see which exception reports the outcome.
    /// </summary>
    private void AllowFailoverToSucceed()
    {
        this.mockPluginService
            .Setup(s => s.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()))
            .ReturnsAsync(new Mock<DbConnection>().Object);
        this.mockPluginService.Setup(s => s.GetHostRole(It.IsAny<DbConnection>())).ReturnsAsync(HostRole.Writer);
    }

    private Task InitHostProviderAsync()
    {
        return this.plugin.InitHostProvider("test-url", this.props, this.mockHostListProviderService.Object, () => Task.CompletedTask);
    }

    /// <summary>
    /// Opens the initial connection first, as production does, because that is when the plugin resolves its
    /// failover mode.
    /// </summary>
    private async Task OpenInitialConnectionAsync()
    {
        await this.InitHostProviderAsync();
        await this.plugin.OpenConnection(Writer, this.props, true, () => Task.FromResult(this.mockConnection.Object), true);
    }

    private void VerifyNoFailover()
    {
        this.mockPluginService.Verify(s => s.SetAvailability(It.IsAny<HostSpec>(), HostAvailability.Unavailable), Times.Never);
        this.mockPluginService.Verify(
            s => s.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Never);
    }
}
