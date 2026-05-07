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
using AwsWrapperDataProvider.Dialect.MySqlClient;
using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests.Telemetry;

/// <summary>
/// Tests that verify <see cref="AwsWrapperConnection.Open"/> and
/// <see cref="AwsWrapperConnection.OpenAsync(CancellationToken)"/> create a
/// top-level trace with the expected span name and attributes, and that the
/// exception path records the exception, success=false, and closes the
/// context before rethrowing.
/// </summary>
public class AwsWrapperConnectionTelemetryTests
{
    private const string ConnectionString =
        "Server=localhost;User ID=admin;Password=<password>;Port=3306;Initial Catalog=test;" +
        "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";

    private const string HostOnlyConnectionString =
        "Server=localhost;User ID=admin;Password=<password>;Initial Catalog=test;" +
        "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";

    static AwsWrapperConnectionTelemetryTests()
    {
        MySqlConnectorDialectLoader.Load();
        MySqlClientDialectLoader.Load();
    }

    private static ConfigurationProfile CreateProfile(
        Dictionary<string, string> props,
        IConnectionPluginFactory? pluginFactory = null)
    {
        return new ConfigurationProfile(
            "awsWrapperConnectionTelemetry",
            [pluginFactory ?? new MockFailoverPluginFactory()],
            props,
            new MySqlDialect(),
            new MySqlConnectorDialect(),
            new DbConnectionProvider());
    }

    private static Dictionary<string, string> BaseProps(string backendName)
    {
        return new Dictionary<string, string>
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "localhost" },
            { "Port", "3306" },
            { "Username", "admin" },
            { "EnableTelemetry", "true" },
            { "TelemetryTracesBackend", backendName },
            { "TelemetrySubmitTopLevel", "true" },
        };
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Open_OnSuccess_CreatesTopLevelSpanWithAttributesAndSuccessTrue()
    {
        string backendName = "T7MOCK" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryContext> mockContext = new();
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            using AwsWrapperConnection<MySqlConnection> connection =
                new(ConnectionString, CreateProfile(BaseProps(backendName)));

            connection.Open();

            mockFactory.Verify(
                f => f.OpenTelemetryContext("DbConnection.Open", TelemetryTraceLevel.TopLevel),
                Times.Once);

            // Attributes set from connection properties + dialect.
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.DbSystemKey, "mysql"), Times.Once);
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.DbUserKey, "admin"), Times.Once);
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.NetPeerNameKey, "localhost"), Times.Once);
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.NetPeerPortKey, "3306"), Times.Once);

            // Success path.
            mockContext.Verify(c => c.SetSuccess(true), Times.Once);
            mockContext.Verify(c => c.SetException(It.IsAny<Exception>()), Times.Never);
            mockContext.Verify(c => c.CloseContext(), Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenAsync_OnSuccess_CreatesTopLevelSpan()
    {
        string backendName = "T7MOCKASYNC" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryContext> mockContext = new();
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            await using AwsWrapperConnection<MySqlConnection> connection =
                new(ConnectionString, CreateProfile(BaseProps(backendName)));

            await connection.OpenAsync(TestContext.Current.CancellationToken);

            // Both Open and OpenAsync funnel through OpenInternal; the
            // recorded span name and level are identical.
            mockFactory.Verify(
                f => f.OpenTelemetryContext("DbConnection.Open", TelemetryTraceLevel.TopLevel),
                Times.Once);
            mockContext.Verify(c => c.SetSuccess(true), Times.Once);
            mockContext.Verify(c => c.CloseContext(), Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Open_OnException_RecordsExceptionSetsSuccessFalseAndCloses()
    {
        string backendName = "T7MOCKERR" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryContext> mockContext = new();
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            ThrowingOpenConnectionPluginFactory throwingFactory = new();
            using AwsWrapperConnection<MySqlConnection> connection =
                new(ConnectionString, CreateProfile(BaseProps(backendName), throwingFactory));

            InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(
                () => connection.Open());
            Assert.Equal("boom", thrown.Message);

            mockFactory.Verify(
                f => f.OpenTelemetryContext("DbConnection.Open", TelemetryTraceLevel.TopLevel),
                Times.Once);
            mockContext.Verify(c => c.SetException(It.Is<InvalidOperationException>(e => e.Message == "boom")), Times.Once);
            mockContext.Verify(c => c.SetSuccess(false), Times.Once);
            mockContext.Verify(c => c.SetSuccess(true), Times.Never);
            mockContext.Verify(c => c.CloseContext(), Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Open_WithTelemetryDisabled_StillWorks_AndUsesNullContext()
    {
        // EnableTelemetry is missing → PluginService uses NullTelemetryFactory.
        // Open() must succeed (and the null context must record no interactions).
        Dictionary<string, string> props = new()
        {
            { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
            { "Host", "localhost" },
        };

        using AwsWrapperConnection<MySqlConnection> connection =
            new(HostOnlyConnectionString, CreateProfile(props));

        Exception? caught = Record.Exception(() => connection.Open());
        Assert.Null(caught);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Open_OmitsAttributesWhenSourceValuesAreMissing()
    {
        string backendName = "T7MOCKNOATTR" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryContext> mockContext = new();
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
                { "Host", "localhost" },
                { "EnableTelemetry", "true" },
                { "TelemetryTracesBackend", backendName },
                { "TelemetrySubmitTopLevel", "true" },
            };

            using AwsWrapperConnection<MySqlConnection> connection =
                new(HostOnlyConnectionString, CreateProfile(props));
            connection.Open();

            // Host + db.system are present; Username/Port were not provided.
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.DbSystemKey, "mysql"), Times.Once);
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.NetPeerNameKey, "localhost"), Times.Once);
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.DbUserKey, It.IsAny<string>()), Times.Never);
            mockContext.Verify(c => c.SetAttribute(TelemetryAttributes.NetPeerPortKey, It.IsAny<string>()), Times.Never);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    /// <summary>
    /// Failover-plugin factory whose <c>OpenConnection</c> always throws
    /// <see cref="InvalidOperationException"/> with message <c>"boom"</c>.
    /// Used to drive <see cref="AwsWrapperConnection.Open"/> down the
    /// exception path.
    /// </summary>
    private sealed class ThrowingOpenConnectionPluginFactory : IConnectionPluginFactory
    {
        public IConnectionPlugin GetInstance(IPluginService pluginService, Dictionary<string, string> props)
        {
            Mock<FailoverPlugin> mock = new(pluginService, props) { CallBase = true };
            mock.Setup(m => m.OpenConnection(
                    It.IsAny<HostSpec>(),
                    It.IsAny<Dictionary<string, string>>(),
                    It.IsAny<bool>(),
                    It.IsAny<ADONetDelegate<DbConnection>>(),
                    It.IsAny<bool>()))
                .ThrowsAsync(new InvalidOperationException("boom"));
            return mock.Object;
        }
    }
}
