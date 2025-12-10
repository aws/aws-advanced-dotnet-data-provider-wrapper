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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

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
}
