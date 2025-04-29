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

using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
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
        { "Username", "testuser" },
        { "Password", "testpass" },
    };

    private static readonly Dictionary<string, string> BasicDatabaseProps = new()
    {
        { "Database", "testdb" },
    };

    private static readonly Dictionary<string, string> PropertiesWithHost = new()
    {
        { "Host", "original-host" },
        { "Port", "5432" },
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
        { PropertyDefinition.CustomTargetDriverDialect.Name, "SomeDialect" },
    };

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_WithHostSpec_IncludesHostAndPort()
    {
        var dialect = new PgTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(HostWithPort, ConnectionProps);

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
        var dialect = new PgTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(HostWithoutPort, BasicDatabaseProps);

        Assert.Contains("Host=test-host", connectionString);
        Assert.DoesNotContain("Port=", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_WithoutHostSpec_UsesPropertiesOnly()
    {
        var dialect = new PgTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(null, PropertiesWithHost);

        Assert.Contains("Host=original-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PgTargetDriverDialect_PrepareConnectionString_FiltersInternalProperties()
    {
        var dialect = new PgTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(HostWithPort, PropsWithInternalProperties);

        Assert.Contains("Host=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.DoesNotContain(PropertyDefinition.TargetConnectionType.Name, connectionString);
        Assert.DoesNotContain(PropertyDefinition.CustomTargetDriverDialect.Name, connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_WithHostSpec_IncludesServerAndPort()
    {
        var dialect = new MySqlTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(HostWithPort, ConnectionProps);

        Assert.Contains("Server=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.Contains("Username=testuser", connectionString);
        Assert.Contains("Password=testpass", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_WithoutPort_OmitsPortParameter()
    {
        var dialect = new MySqlTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(HostWithoutPort, BasicDatabaseProps);

        Assert.Contains("Server=test-host", connectionString);
        Assert.DoesNotContain("Port=", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_WithoutHostSpec_UsesPropertiesOnly()
    {
        var dialect = new MySqlTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(null, PropertiesWithServer);

        Assert.Contains("Server=original-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlTargetDriverDialect_PrepareConnectionString_FiltersInternalProperties()
    {
        var dialect = new MySqlTargetConnectionDialect();
        var connectionString = dialect.PrepareConnectionString(HostWithPort, PropsWithInternalProperties);

        Assert.Contains("Server=test-host", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.DoesNotContain(PropertyDefinition.TargetConnectionType.Name, connectionString);
        Assert.DoesNotContain(PropertyDefinition.CustomTargetDriverDialect.Name, connectionString);
    }
}
