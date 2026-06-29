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
using AwsWrapperDataProvider.Driver.Auth;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests.Driver.TargetConnectionDialects;

public class TargetConnectionDialectTests
{
    private static readonly HostSpec HostWithPort = new(
        "test-host",
        5432,
        "test-id",
        HostRole.Writer,
        HostAvailability.Available);

    private static readonly HostSpec HostWithoutPort = new(
        "test-host",
        HostSpec.NoPort,
        "test-id",
        HostRole.Writer,
        HostAvailability.Available);

    private static readonly Dictionary<string, string> ConnectionProps = new()
    {
        { "Database", "testdb" },
        { "uid", "testuser" },
        { "pwd", "testpass" },
    };

    private static readonly Dictionary<string, string> BasicDatabaseProps = new()
    {
        { "Database", "testdb" },
    };

    private static readonly Dictionary<string, string> PropertiesWithServer = new()
    {
        { "Server", "original-host" },
        { "Port", "5432" },
        { "Database", "testdb" },
    };

    private static readonly Dictionary<string, string> PropsWithInternalProperties = new()
    {
        { "Database", "testdb" },
        { PropertyDefinition.TargetConnectionType.Name, "SomeType" },
        { PropertyDefinition.CustomTargetConnectionDialect.Name, "SomeDialect" },
    };

    private static readonly Dictionary<string, string> PropsWithLowercasePluginsKey = new()
    {
        { "Database", "testdb" },
        { "plugins", "failover" },
    };

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_WithHostSpec_IncludesHostAndPort()
    {
        var connectionDialect = new NpgsqlDialect();
        var dialect = new PgDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, ConnectionProps);

        Assert.Contains("Host=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.Contains("Username=testuser", connectionString);
        Assert.Contains("Password=testpass", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_WithoutPort_OmitsPortParameter()
    {
        var connectionDialect = new NpgsqlDialect();
        var dialect = new PgDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithoutPort, BasicDatabaseProps);

        Assert.Contains("Host=test-host", connectionString);
        Assert.DoesNotContain("Port=", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_WithoutHostSpec_UsesPropertiesOnly()
    {
        var connectionDialect = new NpgsqlDialect();
        var dialect = new PgDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, null, PropertiesWithServer);

        Assert.Contains("Host=original-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_FiltersInternalProperties()
    {
        var connectionDialect = new NpgsqlDialect();
        var dialect = new PgDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, PropsWithInternalProperties);

        Assert.Contains("Host=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.DoesNotContain(PropertyDefinition.TargetConnectionType.Name, connectionString);
        Assert.DoesNotContain(PropertyDefinition.CustomTargetConnectionDialect.Name, connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_WithHostSpec_IncludesServerAndPort()
    {
        var connectionDialect = new MySqlConnectorDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, ConnectionProps);

        Assert.Contains("Server=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.Contains("User ID=testuser", connectionString);
        Assert.Contains("Password=testpass", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_WithoutHostSpec_UsesPropertiesOnly()
    {
        var connectionDialect = new MySqlConnectorDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, null, PropertiesWithServer);

        Assert.Contains("Server=original-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_FiltersInternalProperties()
    {
        var connectionDialect = new MySqlConnectorDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, PropsWithInternalProperties);

        Assert.Contains("Server=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.DoesNotContain(PropertyDefinition.TargetConnectionType.Name, connectionString);
        Assert.DoesNotContain(PropertyDefinition.CustomTargetConnectionDialect.Name, connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_FiltersPluginsCaseInsensitive()
    {
        var connectionDialect = new MySqlConnectorDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, PropsWithLowercasePluginsKey);

        Assert.Contains("Server=test-host", connectionString);
        Assert.DoesNotContain("plugins", connectionString, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Plugins", true)]
    [InlineData("plugins", true)]
    [InlineData("PLUGINS", true)]
    [InlineData("Database", false)]
    public void PropertyDefinition_IsInternalWrapperPropertyKey_IsCaseInsensitive(string key, bool expected) =>
        Assert.Equal(expected, PropertyDefinition.IsInternalWrapperPropertyKey(key));

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClientDialect_PrepareConnectionString_WithHostSpec_IncludesServerAndPort()
    {
        var connectionDialect = new MySqlClientDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, ConnectionProps);

        Assert.Contains("server=test-host", connectionString);
        Assert.Contains("port=5432", connectionString);
        Assert.Contains("database=testdb", connectionString);
        Assert.Contains("user id=testuser", connectionString);
        Assert.Contains("password=testpass", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClientDialect_PrepareConnectionString_WithoutHostSpec_UsesPropertiesOnly()
    {
        var connectionDialect = new MySqlClientDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, null, PropertiesWithServer);

        Assert.Contains("server=original-host", connectionString);
        Assert.Contains("port=5432", connectionString);
        Assert.Contains("database=testdb", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClientDialect_PrepareConnectionString_FiltersInternalProperties()
    {
        var connectionDialect = new MySqlClientDialect();
        var dialect = new MySqlDialect();
        var connectionString = connectionDialect.PrepareConnectionString(dialect, HostWithPort, PropsWithInternalProperties);

        Assert.Contains("server=test-host", connectionString);
        Assert.Contains("port=5432", connectionString);
        Assert.Contains("database=testdb", connectionString);
        Assert.DoesNotContain(PropertyDefinition.TargetConnectionType.Name, connectionString);
        Assert.DoesNotContain(PropertyDefinition.CustomTargetConnectionDialect.Name, connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SupportsPasswordProvider_MatchesDriverCapability()
    {
        Assert.True(new NpgsqlDialect().SupportsPasswordProvider);
        Assert.True(new MySqlConnectorDialect().SupportsPasswordProvider);
        Assert.False(new MySqlClientDialect().SupportsPasswordProvider);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateConnection_WithoutProviderKey_FallsBackToReflection()
    {
        PasswordProviderRegistry.Clear();
        var connectionDialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string>();

        using var connection = connectionDialect.CreateConnection(typeof(MySqlConnection), "Server=test-host;", props);

        var mySql = Assert.IsType<MySqlConnection>(connection);
        Assert.Null(mySql.ProvidePasswordCallback);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateConnection_WithUnregisteredProviderKey_DoesNotSetCallback()
    {
        PasswordProviderRegistry.Clear();
        var connectionDialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string>
        {
            { PasswordProviderRegistry.ProviderKeyPropertyName, "missing-key" },
        };

        using var connection = connectionDialect.CreateConnection(typeof(MySqlConnection), "Server=test-host;", props);

        var mySql = Assert.IsType<MySqlConnection>(connection);
        Assert.Null(mySql.ProvidePasswordCallback);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_CreateConnection_WithRegisteredProvider_SetsCallbackReturningToken()
    {
        PasswordProviderRegistry.Clear();
        const string key = "mysql-endpoint-key";
        PasswordProviderRegistry.Register(
            key,
            new PasswordProviderRegistration(
                _ => new ValueTask<string>("rotating-token")));

        var connectionDialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string>
        {
            { PasswordProviderRegistry.ProviderKeyPropertyName, key },
        };

        using var connection = connectionDialect.CreateConnection(typeof(MySqlConnection), "Server=test-host;", props);

        var mySql = Assert.IsType<MySqlConnection>(connection);
        Assert.NotNull(mySql.ProvidePasswordCallback);

        // The callback ignores its context argument and serves the registered provider's token.
        Assert.Equal("rotating-token", mySql.ProvidePasswordCallback!(null!));

        PasswordProviderRegistry.Clear();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PgConnectionString_IsByteIdenticalAcrossTokenRotations()
    {
        // Regression for issue #301: with the password supplied via a provider (and therefore absent
        // from the connection string), the prepared string — and thus the driver's pool key — must
        // be identical regardless of the current token value.
        var connectionDialect = new NpgsqlDialect();
        var dialect = new PgDialect();

        var propsRotation1 = new Dictionary<string, string>
        {
            { "Database", "testdb" },
            { "uid", "testuser" },
            { PasswordProviderRegistry.ProviderKeyPropertyName, "endpoint-key" },
        };
        var propsRotation2 = new Dictionary<string, string>(propsRotation1);

        var first = connectionDialect.PrepareConnectionString(dialect, HostWithPort, propsRotation1);
        var second = connectionDialect.PrepareConnectionString(dialect, HostWithPort, propsRotation2);

        Assert.Equal(first, second);
        Assert.DoesNotContain("Password=", first);
        Assert.DoesNotContain(PasswordProviderRegistry.ProviderKeyPropertyName, first);
    }
}
