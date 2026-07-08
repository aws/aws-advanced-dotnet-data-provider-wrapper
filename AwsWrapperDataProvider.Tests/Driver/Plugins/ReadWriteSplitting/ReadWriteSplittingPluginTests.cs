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

using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;
using AwsWrapperDataProvider.Properties;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.ReadWriteSplitting;

public class ReadWriteSplittingPluginTests
{
    private const int TestPort = 5432;

    private readonly Mock<IPluginService> mockPluginService;
    private readonly Mock<IHostListProviderService> mockHostListProviderService;
    private readonly Mock<IDialect> mockDialect;
    private readonly Mock<DbConnection> mockWriterConn;
    private readonly Mock<DbConnection> mockReaderConn1;
    private readonly Dictionary<string, string> props;

    private readonly HostSpec writerHostSpec;
    private readonly HostSpec readerHostSpec1;
    private readonly HostSpec readerHostSpec2;
    private readonly List<HostSpec> defaultHosts;
    private readonly List<HostSpec> singleReaderTopology;

    public ReadWriteSplittingPluginTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.mockHostListProviderService = new Mock<IHostListProviderService>();
        this.mockDialect = new Mock<IDialect>();
        this.mockWriterConn = new Mock<DbConnection>();
        this.mockReaderConn1 = new Mock<DbConnection>();
        this.props = new Dictionary<string, string>();

        this.writerHostSpec = new HostSpec("instance-0", TestPort, HostRole.Writer, HostAvailability.Available);
        this.readerHostSpec1 = new HostSpec("instance-1", TestPort, HostRole.Reader, HostAvailability.Available);
        this.readerHostSpec2 = new HostSpec("instance-2", TestPort, HostRole.Reader, HostAvailability.Available);

        this.defaultHosts = [this.writerHostSpec, this.readerHostSpec1, this.readerHostSpec2];
        this.singleReaderTopology = [this.writerHostSpec, this.readerHostSpec1];

        this.MockDefaultBehavior();
    }

    private void MockDefaultBehavior()
    {
        // The current connection is an open writer connection (the read/write split starts from the writer).
        this.mockWriterConn.Setup(x => x.State).Returns(System.Data.ConnectionState.Open);
        this.mockReaderConn1.Setup(x => x.State).Returns(System.Data.ConnectionState.Open);

        this.mockPluginService.Setup(x => x.CurrentConnection).Returns(this.mockWriterConn.Object);
        this.mockPluginService.Setup(x => x.CurrentHostSpec).Returns(this.writerHostSpec);
        this.mockPluginService.Setup(x => x.CurrentTransaction).Returns((DbTransaction?)null);
        this.mockPluginService.Setup(x => x.AllHosts).Returns(this.defaultHosts);
        this.mockPluginService.Setup(x => x.GetHosts()).Returns(this.defaultHosts);
        this.mockPluginService.Setup(x => x.Dialect).Returns(this.mockDialect.Object);
        this.mockPluginService.Setup(x => x.AcceptsStrategy(It.IsAny<string>())).Returns(true);
        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), HostRole.Reader, It.IsAny<string>()))
            .Returns(this.readerHostSpec1);
        this.mockPluginService
            .Setup(x => x.OpenConnection(this.readerHostSpec1, It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ReturnsAsync(this.mockReaderConn1.Object);

        // Refreshing the host list is a no-op in these tests.
        this.mockPluginService.Setup(x => x.RefreshHostListAsync()).Returns(Task.CompletedTask);

        // The dialect recognizes the command as a "set read only = true" statement so that Execute routes
        // through SwitchConnectionIfRequired.
        this.mockDialect.Setup(x => x.DoesStatementSetReadOnly(It.IsAny<string>())).Returns((true, true));
    }

    private ReadWriteSplittingPlugin CreatePlugin()
    {
        var plugin = new ReadWriteSplittingPlugin(this.mockPluginService.Object, this.props);
        plugin.InitHostProvider(
            string.Empty,
            this.props,
            this.mockHostListProviderService.Object,
            () => Task.CompletedTask);
        return plugin;
    }

    /// <summary>
    /// Drives the plugin the way the wrapper does: a SET READ ONLY command flows through Execute, which
    /// invokes SwitchConnectionIfRequired(true).
    /// </summary>
    private Task InvokeSetReadOnlyTrue(ReadWriteSplittingPlugin plugin)
    {
        var mockCommand = new Mock<DbCommand>();
        mockCommand.Setup(x => x.CommandText).Returns("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");

        var methodFunc = new Mock<ADONetDelegate<object>>();
        methodFunc.Setup(x => x.Invoke()).ReturnsAsync(new object());

        return plugin.Execute(mockCommand.Object, "ExecuteNonQuery", methodFunc.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task SetReadOnlyTrue_ReaderConnectionFailed_TriesEachReaderOnceThenFallsBack()
    {
        this.mockPluginService.Setup(x => x.AllHosts).Returns(this.singleReaderTopology);
        this.mockPluginService.Setup(x => x.GetHosts()).Returns(this.singleReaderTopology);
        this.mockPluginService
            .Setup(x => x.OpenConnection(this.readerHostSpec1, It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ThrowsAsync(new TestDbException());

        // The selector returns the reader from the local candidate list until it is removed after the failed
        // attempt, after which the selector reports no eligible reader (the .NET selectors throw for this).
        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), HostRole.Reader, It.IsAny<string>()))
            .Returns((IList<HostSpec> candidates, HostRole _, string _) =>
                candidates.Contains(this.readerHostSpec1)
                    ? this.readerHostSpec1
                    : throw new InvalidOperationException(string.Format(Resources.Error_NoHostsMatching, HostRole.Reader)));

        var plugin = this.CreatePlugin();

        // The current writer connection is usable, so the failed reader switch falls back to it without throwing.
        await this.InvokeSetReadOnlyTrue(plugin);

        // The failed reader should be tried exactly once (it is removed from the local candidate list after the
        // failure), and no global availability state should be changed.
        this.mockPluginService.Verify(
            x => x.OpenConnection(this.readerHostSpec1, It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()),
            Times.Once);
        this.mockPluginService.Verify(
            x => x.SetAvailability(It.IsAny<HostSpec>(), It.IsAny<HostAvailability>()),
            Times.Never);

        // The current writer connection is kept as a fallback (no switch to a reader).
        this.mockPluginService.Verify(
            x => x.SetCurrentConnection(It.IsAny<DbConnection>(), It.IsAny<HostSpec>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task SetReadOnlyTrue_ReaderLoginFailed_RethrowsAndDoesNotChangeAvailability()
    {
        // Use the full topology and a selector that always returns reader1, so that absent the login
        // showstopper the failed reader would be removed and re-selected, producing further attempts. The
        // single-attempt assertion below is therefore meaningful.
        this.mockPluginService
            .Setup(x => x.OpenConnection(this.readerHostSpec1, It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()))
            .ThrowsAsync(new TestDbException());

        // The failure is a login/authentication failure, which does not indicate the host is unreachable.
        this.mockPluginService.Setup(x => x.IsLoginException(It.IsAny<Exception>())).Returns(true);

        var plugin = this.CreatePlugin();

        // A login failure is a showstopper: OpenNewReaderConnection rethrows the exception immediately instead
        // of removing the reader and retrying, so only a single connection attempt is made. The outer switch
        // then falls back to the usable writer connection (no exception surfaces here).
        await this.InvokeSetReadOnlyTrue(plugin);

        // Only a single connection attempt is made before rethrowing. Without the login-exception showstopper,
        // the reader would be removed and re-selected, producing a second attempt.
        this.mockPluginService.Verify(
            x => x.OpenConnection(this.readerHostSpec1, It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()),
            Times.Once);

        // No global availability state should be changed.
        this.mockPluginService.Verify(
            x => x.SetAvailability(It.IsAny<HostSpec>(), It.IsAny<HostAvailability>()),
            Times.Never);

        // No switch to a reader connection occurred.
        this.mockPluginService.Verify(
            x => x.SetCurrentConnection(It.IsAny<DbConnection>(), It.IsAny<HostSpec>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task SetReadOnlyTrue_NoEligibleReader_FallsBackToWriter()
    {
        this.mockPluginService.Setup(x => x.AllHosts).Returns(this.singleReaderTopology);
        this.mockPluginService.Setup(x => x.GetHosts()).Returns(this.singleReaderTopology);

        // No eligible reader is available: the selector reports it (the .NET selectors throw in this case).
        this.mockPluginService
            .Setup(x => x.GetHostSpecByStrategy(It.IsAny<IList<HostSpec>>(), HostRole.Reader, It.IsAny<string>()))
            .Throws(() => new InvalidOperationException(string.Format(Resources.Error_NoHostsMatching, HostRole.Reader)));

        var plugin = this.CreatePlugin();

        // With no eligible reader and a usable writer connection, the switch falls back to the writer.
        await this.InvokeSetReadOnlyTrue(plugin);

        // No connection attempt should be made when there is no eligible reader.
        this.mockPluginService.Verify(
            x => x.OpenConnection(this.readerHostSpec1, It.IsAny<Dictionary<string, string>>(), It.IsAny<IConnectionPlugin>(), It.IsAny<bool>()),
            Times.Never);
        this.mockPluginService.Verify(
            x => x.SetAvailability(It.IsAny<HostSpec>(), It.IsAny<HostAvailability>()),
            Times.Never);
        this.mockPluginService.Verify(
            x => x.SetCurrentConnection(It.IsAny<DbConnection>(), It.IsAny<HostSpec>()),
            Times.Never);
    }

    /// <summary>
    /// Minimal concrete <see cref="DbException"/> for simulating a non-login reader connection failure.
    /// (<see cref="DbException"/> is abstract-constructible but we want an explicit, unambiguous type here.)
    /// </summary>
    private sealed class TestDbException : DbException
    {
        public TestDbException()
            : base("Simulated reader connection failure.")
        {
        }
    }
}
