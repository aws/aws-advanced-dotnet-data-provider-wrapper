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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.ConnectionLifetime;
using AwsWrapperDataProvider.Driver.Plugins.ConnectionLifetime.Exceptions;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.ConnectionLifetime;

public class ConnectionLifetimePluginTests
{
    private const long LifetimeMs = 1000;
    private const string NetworkMethod = "DbCommand.ExecuteNonQuery";

    private readonly Mock<IPluginService> mockPluginService = new();
    private readonly Mock<DbConnection> mockInitialConnection = new();
    private readonly Mock<DbConnection> mockReplacementConnection = new();
    private readonly ManualTimeProvider timeProvider = new();
    private readonly HostSpec originalHostSpec = new HostSpecBuilder()
        .WithHost("my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com")
        .WithPort(5432)
        .Build();

    private readonly HostSpec routedHostSpec = new HostSpecBuilder()
        .WithHost("instance-2.xyz.us-east-1.rds.amazonaws.com")
        .WithPort(5432)
        .WithRole(HostRole.Reader)
        .Build();

    private DbConnection? currentConnection;
    private DbTransaction? currentTransaction;
    private int pendingCallCount;

    public ConnectionLifetimePluginTests()
    {
        this.mockInitialConnection.SetupGet(x => x.State).Returns(ConnectionState.Open);
        this.mockReplacementConnection.SetupGet(x => x.State).Returns(ConnectionState.Open);
        this.mockPluginService.SetupGet(x => x.CurrentConnection).Returns(() => this.currentConnection);
        this.mockPluginService.SetupGet(x => x.CurrentTransaction).Returns(() => this.currentTransaction);
        this.mockPluginService.SetupGet(x => x.OriginalHostSpec).Returns(this.originalHostSpec);
        this.mockPluginService.SetupGet(x => x.CurrentHostSpec).Returns(this.routedHostSpec);
        this.mockPluginService
            .Setup(x => x.SetCurrentConnection(It.IsAny<DbConnection?>(), It.IsAny<HostSpec?>()))
            .Callback<DbConnection?, HostSpec?>((connection, _) => this.currentConnection = connection);
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockReplacementConnection.Object);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("abc")]
    [InlineData("1.5")]
    [InlineData("99999999999999999999")]
    public void Constructor_WithInvalidLifetime_Throws(string? lifetimeMs)
    {
        Dictionary<string, string> props = [];
        if (lifetimeMs != null)
        {
            props[PropertyDefinition.ConnectionLifetimeMs.Name] = lifetimeMs;
        }

        Assert.Throws<ArgumentException>(() => new ConnectionLifetimePlugin(this.mockPluginService.Object, props));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("")]
    [InlineData("restart")]
    [InlineData("0")]
    public void Constructor_WithInvalidAction_Throws(string action)
    {
        Dictionary<string, string> props = Props(action);

        Assert.Throws<ArgumentException>(() => new ConnectionLifetimePlugin(this.mockPluginService.Object, props));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Factory_WithMissingLifetime_Throws()
    {
        FullServicesContainer container = ServiceUtility.CreateMonitorContainer(null, this.mockPluginService.Object, []);

        Assert.Throws<ArgumentException>(() => new ConnectionLifetimePluginFactory().GetInstance(container, []));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SubscribedMethods_ExcludeLifecycleAndLocalCalls()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");

        Assert.Contains(NetworkMethod, plugin.SubscribedMethods);
        Assert.Contains("DbConnection.Open", plugin.SubscribedMethods);
        Assert.DoesNotContain("DbConnection.Close", plugin.SubscribedMethods);
        Assert.DoesNotContain("DbConnection.CloseAsync", plugin.SubscribedMethods);
        Assert.DoesNotContain("DbConnection.CreateCommand", plugin.SubscribedMethods);
        Assert.DoesNotContain("DbConnection.ChangeDatabase", plugin.SubscribedMethods);
        Assert.DoesNotContain("DbConnection.GetSchema", plugin.SubscribedMethods);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_NonNetworkMethodThroughPluginManager_IgnoresExpiredLifetime()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);

        ConnectionPluginManager manager = new(
            Mock.Of<IConnectionProvider>(),
            null,
            [plugin, new DefaultConnectionPlugin(this.mockPluginService.Object, Mock.Of<IConnectionProvider>(), null)],
            null!);

        int result = await manager.Execute(this.currentConnection!, "DbConnection.Close", this.PendingCall);

        Assert.Equal(1, result);
        Assert.Equal(1, this.pendingCallCount);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_CloseAction_ClosesConnectionAndFailsPendingCall()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);

        ConnectionLifetimeException exception = await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.Equal(typeof(ConnectionLifetimeException), exception.GetType());
        Assert.Equal("08003", exception.SqlState);
        Assert.Equal("08003", ((DbException)exception).SqlState);
        Assert.Equal(0, this.pendingCallCount);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Once);
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Never);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("CLOSE")]
    [InlineData(" Close ")]
    public async Task Execute_CloseActionIgnoringCaseAndWhitespace_Closes(string action)
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin(action);
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);

        await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_AfterClose_EveryNetworkCallFailsUntilApplicationOpensAgain()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        // The plugin closed the connection, so it now reports Closed; the lifetime error still wins.
        this.mockInitialConnection.SetupGet(x => x.State).Returns(ConnectionState.Closed);
        ConnectionLifetimeException exception = await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection!, "DbCommand.ExecuteReaderAsync", this.PendingCall));

        Assert.Equal("08003", exception.SqlState);
        Assert.Equal(0, this.pendingCallCount);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Once);

        // Close and Open on the same wrapper connection installs a new physical connection with a new lifetime.
        DbConnection reopened = await plugin.OpenConnection(
            this.originalHostSpec, [], true, () => Task.FromResult(this.mockReplacementConnection.Object), true);
        this.currentConnection = reopened;

        Assert.Equal(1, await plugin.Execute(this.currentConnection, NetworkMethod, this.PendingCall));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ExpiryBoundary_IsInclusive()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");
        await this.OpenInitialConnection(plugin);

        this.timeProvider.AdvanceMs(LifetimeMs - 1);
        Assert.Equal(1, await plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        this.timeProvider.AdvanceMs(1);
        await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(1, this.pendingCallCount);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_DuringTransaction_DefersActionWithoutResettingTimer()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        this.currentTransaction = Mock.Of<DbTransaction>();

        Assert.Equal(1, await plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(2, await plugin.Execute(this.currentConnection!, "DbTransaction.Commit", this.PendingCall));
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Never);

        // The transaction ended; no extra time passes, so the deferred action runs on the next call.
        this.currentTransaction = null;
        await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(2, this.pendingCallCount);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("DbDataReader.Read")]
    [InlineData("DbDataReader.NextResultAsync")]
    public async Task Execute_ReaderCall_DefersActionWithoutResettingTimer(string readerMethod)
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);

        Assert.Equal(1, await plugin.Execute(this.currentConnection!, readerMethod, this.PendingCall));
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Never);

        // The next call that starts new work runs the deferred action.
        await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(1, this.pendingCallCount);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("reconnect")]
    [InlineData("close")]
    public async Task Execute_ConnectionClosedByApplication_DoesNotReopenOrClose(string action)
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin(action);
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        this.mockInitialConnection.SetupGet(x => x.State).Returns(ConnectionState.Closed);

        Assert.Equal(1, await plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.Same(this.mockInitialConnection.Object, this.currentConnection);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Never);
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Never);
        this.mockPluginService.Verify(
            x => x.SetCurrentConnection(It.IsAny<DbConnection?>(), It.IsAny<HostSpec?>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ReconnectAction_ReconnectsToOriginalHostAndFailsPendingCall()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("Reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);

        ConnectionLifetimeSuccessException exception = await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.Equal("08S02", exception.SqlState);
        Assert.Equal(0, this.pendingCallCount);
        this.mockPluginService.Verify(
            x => x.OpenConnection(this.originalHostSpec, It.IsAny<Dictionary<string, string>>(), plugin, It.IsAny<bool>()),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.SetCurrentConnection(this.mockReplacementConnection.Object, this.originalHostSpec),
            Times.Once);
        Assert.Same(this.mockReplacementConnection.Object, this.currentConnection);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ReconnectRoutedByAnotherPlugin_InstallsRoutedHost()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        this.mockPluginService.SetupProperty(x => x.RoutedHostSpec, this.routedHostSpec);

        await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        this.mockPluginService.Verify(
            x => x.OpenConnection(this.originalHostSpec, It.IsAny<Dictionary<string, string>>(), plugin, It.IsAny<bool>()),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.SetCurrentConnection(this.mockReplacementConnection.Object, this.routedHostSpec),
            Times.Once);
        Assert.Null(this.mockPluginService.Object.RoutedHostSpec);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_AfterSuccessfulReconnect_LifetimeRestartsFromReplacement()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        this.timeProvider.AdvanceMs(LifetimeMs - 1);
        Assert.Equal(1, await plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        this.timeProvider.AdvanceMs(1);
        await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ReconnectOpenFails_PreservesCauseAndDoesNotResetTimer()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        InvalidOperationException openFailure = new("open failed");
        this.mockPluginService
            .Setup(x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()))
            .ThrowsAsync(openFailure);

        ConnectionLifetimeFailedException exception = await Assert.ThrowsAsync<ConnectionLifetimeFailedException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.Equal("08001", exception.SqlState);
        Assert.Same(openFailure, exception.InnerException);
        Assert.Same(this.mockInitialConnection.Object, this.currentConnection);

        // The timer was not reset, so the next call tries to reconnect again.
        await Assert.ThrowsAsync<ConnectionLifetimeFailedException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(0, this.pendingCallCount);
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Exactly(2));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_WithoutOriginalHost_FailsReconnect()
    {
        this.mockPluginService.SetupGet(x => x.OriginalHostSpec).Returns((HostSpec?)null);
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);

        ConnectionLifetimeFailedException exception = await Assert.ThrowsAsync<ConnectionLifetimeFailedException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        this.mockPluginService.Verify(
            x => x.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin?>(), It.IsAny<bool>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ReplacementNotInstalled_DisposesReplacement()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        InvalidOperationException installFailure = new("install failed");
        this.mockPluginService
            .Setup(x => x.SetCurrentConnection(It.IsAny<DbConnection?>(), It.IsAny<HostSpec?>()))
            .Throws(installFailure);

        ConnectionLifetimeFailedException exception = await Assert.ThrowsAsync<ConnectionLifetimeFailedException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.Same(installFailure, exception.InnerException);
        Assert.Same(this.mockInitialConnection.Object, this.currentConnection);
        this.mockReplacementConnection.Verify(x => x.DisposeAsync(), Times.Once);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Never);
        Assert.Equal(1, plugin.TrackedConnectionCount);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ReplacementPartiallyInstalled_ClosesBothAndBecomesTerminal()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        InvalidOperationException installFailure = new("rebind failed");
        this.mockPluginService
            .Setup(x => x.SetCurrentConnection(It.IsAny<DbConnection?>(), It.IsAny<HostSpec?>()))
            .Callback<DbConnection?, HostSpec?>((connection, _) => this.currentConnection = connection)
            .Throws(installFailure);

        ConnectionLifetimeFailedException exception = await Assert.ThrowsAsync<ConnectionLifetimeFailedException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));

        Assert.Same(installFailure, exception.InnerException);
        this.mockReplacementConnection.Verify(x => x.CloseAsync(), Times.Once);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Once);
        Assert.Equal(0, plugin.TrackedConnectionCount);

        ConnectionLifetimeException terminal = await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal("08003", terminal.SqlState);
        Assert.Equal(0, this.pendingCallCount);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_TrackedConnections_DropClosedAndReplacedConnections()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("reconnect");
        await this.OpenInitialConnection(plugin);
        Assert.Equal(1, plugin.TrackedConnectionCount);

        this.timeProvider.AdvanceMs(LifetimeMs);
        await Assert.ThrowsAsync<ConnectionLifetimeSuccessException>(
            () => plugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(1, plugin.TrackedConnectionCount);

        ConnectionLifetimePlugin closingPlugin = this.CreatePlugin("close");
        await this.OpenInitialConnection(closingPlugin);
        this.timeProvider.AdvanceMs(LifetimeMs);
        await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => closingPlugin.Execute(this.currentConnection!, NetworkMethod, this.PendingCall));
        Assert.Equal(0, closingPlugin.TrackedConnectionCount);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ConnectionSwitchedByAnotherPlugin_TracksEachConnectionSeparately()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");
        await this.OpenInitialConnection(plugin);

        // Another plugin opens a connection through the chain and installs it later.
        this.timeProvider.AdvanceMs(400);
        Mock<DbConnection> openedThroughChain = new();
        openedThroughChain.SetupGet(x => x.State).Returns(ConnectionState.Open);
        await plugin.OpenConnection(this.routedHostSpec, [], false, () => Task.FromResult(openedThroughChain.Object), true);
        this.timeProvider.AdvanceMs(400);
        this.currentConnection = openedThroughChain.Object;
        Assert.Equal(1, await plugin.Execute(this.currentConnection, NetworkMethod, this.PendingCall));

        // The lifetime counts from when that connection was opened, not from when it became current.
        this.timeProvider.AdvanceMs(LifetimeMs - 400);
        await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection, NetworkMethod, this.PendingCall));
        openedThroughChain.Verify(x => x.CloseAsync(), Times.Once);
        this.mockInitialConnection.Verify(x => x.CloseAsync(), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Execute_ConnectionNotOpenedThroughChain_LifetimeStartsWhenFirstSeen()
    {
        ConnectionLifetimePlugin plugin = this.CreatePlugin("close");
        this.currentConnection = this.mockInitialConnection.Object;

        Assert.Equal(1, await plugin.Execute(this.currentConnection, NetworkMethod, this.PendingCall));

        this.timeProvider.AdvanceMs(LifetimeMs - 1);
        Assert.Equal(2, await plugin.Execute(this.currentConnection, NetworkMethod, this.PendingCall));

        // A host change on the same connection does not restart its lifetime.
        this.mockPluginService.SetupGet(x => x.CurrentHostSpec).Returns(this.originalHostSpec);
        this.timeProvider.AdvanceMs(1);
        await Assert.ThrowsAsync<ConnectionLifetimeException>(
            () => plugin.Execute(this.currentConnection, NetworkMethod, this.PendingCall));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void LifetimeProperties_AreNotPassedToTargetDriver()
    {
        Assert.True(PropertyDefinition.IsInternalWrapperPropertyKey(PropertyDefinition.ConnectionLifetimeMs.Name));
        Assert.True(PropertyDefinition.IsInternalWrapperPropertyKey(PropertyDefinition.ConnectionLifetimeAction.Name));
    }

    private static Dictionary<string, string> Props(string action)
    {
        return new Dictionary<string, string>
        {
            { PropertyDefinition.ConnectionLifetimeMs.Name, LifetimeMs.ToString() },
            { PropertyDefinition.ConnectionLifetimeAction.Name, action },
        };
    }

    private ConnectionLifetimePlugin CreatePlugin(string action)
    {
        return new ConnectionLifetimePlugin(this.mockPluginService.Object, Props(action), this.timeProvider);
    }

    private async Task OpenInitialConnection(ConnectionLifetimePlugin plugin)
    {
        this.currentConnection = await plugin.OpenConnection(
            this.originalHostSpec, [], true, () => Task.FromResult(this.mockInitialConnection.Object), true);
    }

    private Task<int> PendingCall()
    {
        return Task.FromResult(++this.pendingCallCount);
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private long timestamp = 1_000_000;

        public override long TimestampFrequency => 1000;

        public override long GetTimestamp() => this.timestamp;

        public void AdvanceMs(long milliseconds) => this.timestamp += milliseconds;
    }
}
