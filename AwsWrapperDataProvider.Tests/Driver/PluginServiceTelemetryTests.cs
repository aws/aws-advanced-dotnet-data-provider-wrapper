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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests.Driver;

/// <summary>
/// Tests that verify <see cref="PluginService"/> initializes its
/// <see cref="IPluginService.TelemetryFactory"/> property based on the
/// telemetry connection properties.
/// </summary>
public class PluginServiceTelemetryTests
{
    private const string ConnectionString =
        "Server=<insert_rds_instance_here>;User ID=admin;Password=<password>;Initial Catalog=test;" +
        "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;";

    static PluginServiceTelemetryTests()
    {
        MySqlConnectorDialectLoader.Load();
        MySqlClientDialectLoader.Load();
    }

    private static ConfigurationProfile CreateProfile(Dictionary<string, string> props)
        => new(
            "pluginServiceTelemetry",
            [new MockFailoverPluginFactory()],
            props,
            new MySqlDialect(),
            new MySqlConnectorDialect(),
            new DbConnectionProvider());

    private static Dictionary<string, string> MinimumProps() => new()
    {
        { "TargetConnectionType", "MySqlConnector.MySqlConnection,MySqlConnector" },
        { "Host", "<insert_rds_instance_here>" },
    };

    [Fact]
    [Trait("Category", "Unit")]
    public void TelemetryFactory_WhenEnableTelemetryIsFalse_IsNullTelemetryFactorySingleton()
    {
        Dictionary<string, string> props = MinimumProps();
        props["EnableTelemetry"] = "false";

        using TestableConnection connection = new(ConnectionString, CreateProfile(props));

        IPluginService pluginService = connection.GetPluginServiceForTest();
        Assert.Same(NullTelemetryFactory.Instance, pluginService.TelemetryFactory);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TelemetryFactory_WhenEnableTelemetryIsAbsent_DefaultsToNullTelemetryFactory()
    {
        // Default property value is "false", so an absent key must behave
        // identically to explicitly disabling telemetry.
        Dictionary<string, string> props = MinimumProps();

        using TestableConnection connection = new(ConnectionString, CreateProfile(props));

        IPluginService pluginService = connection.GetPluginServiceForTest();
        Assert.Same(NullTelemetryFactory.Instance, pluginService.TelemetryFactory);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TelemetryFactory_WhenEnableTelemetryIsTrue_IsDefaultTelemetryFactory()
    {
        Dictionary<string, string> props = MinimumProps();
        props["EnableTelemetry"] = "true";
        props["TelemetryTracesBackend"] = "OTLP";
        props["TelemetryMetricsBackend"] = "OTLP";

        using TestableConnection connection = new(ConnectionString, CreateProfile(props));

        IPluginService pluginService = connection.GetPluginServiceForTest();
        Assert.IsType<DefaultTelemetryFactory>(pluginService.TelemetryFactory);

        // DefaultTelemetryFactory is not the same reference as the Null singleton.
        Assert.NotSame(NullTelemetryFactory.Instance, pluginService.TelemetryFactory);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TelemetryFactory_WhenEnabledButBackendsAreNone_StillRoutesThroughDefaultTelemetryFactory()
    {
        // Even with both backends NONE, enabling telemetry creates a
        // DefaultTelemetryFactory (which internally routes to Null). The spec
        // literal is "enableTelemetry=false → NullTelemetryFactory singleton";
        // any other configuration yields the router.
        Dictionary<string, string> props = MinimumProps();
        props["EnableTelemetry"] = "true";

        using TestableConnection connection = new(ConnectionString, CreateProfile(props));

        IPluginService pluginService = connection.GetPluginServiceForTest();
        Assert.IsType<DefaultTelemetryFactory>(pluginService.TelemetryFactory);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TelemetryFactory_IsNeverNull_OnFreshlyConstructedPluginService()
    {
        Dictionary<string, string> props = MinimumProps();

        using TestableConnection connection = new(ConnectionString, CreateProfile(props));

        IPluginService pluginService = connection.GetPluginServiceForTest();
        Assert.NotNull(pluginService.TelemetryFactory);
    }

    /// <summary>
    /// Thin test-only subclass that exposes <c>AwsWrapperConnection.pluginService</c>
    /// (a <c>protected</c> field) for verification. Keeping production surface
    /// free of test-only accessors.
    /// </summary>
    private sealed class TestableConnection : AwsWrapperConnection<MySqlConnection>
    {
        public TestableConnection(string connectionString, ConfigurationProfile profile)
            : base(connectionString, profile)
        {
        }

        public IPluginService GetPluginServiceForTest()
            => this.pluginService
               ?? throw new InvalidOperationException("pluginService was not initialized.");
    }
}
