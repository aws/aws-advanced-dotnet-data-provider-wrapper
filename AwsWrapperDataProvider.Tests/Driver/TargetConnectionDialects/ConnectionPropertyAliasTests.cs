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
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.TargetConnectionDialects;

public class ConnectionPropertyAliasTests
{
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Server", "Host", "localhost")]
    [InlineData("host", "Host", "localhost")]
    [InlineData("data source", "Host", "localhost")]
    [InlineData("datasource", "Host", "localhost")]
    [InlineData("Port", "Port", "3306")]
    [InlineData("User ID", "Username", "testuser")]
    [InlineData("user id", "Username", "testuser")]
    [InlineData("Uid", "Username", "testuser")]
    [InlineData("uid", "Username", "testuser")]
    [InlineData("password", "Password", "my_password")]
    [InlineData("pwd", "Password", "my_password")]
    [InlineData("password1", "Password", "my_password")]
    [InlineData("pwd1", "Password", "my_password")]
    public void MySqlClientDialect_AliasesMapToPropertyName(string alias, string expected, string value)
    {
        var props = new Dictionary<string, string>
        {
            { alias, value },
        };

        var dialect = new MySqlClientDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);
        Assert.True(props.ContainsKey(expected));
        Assert.Equal(value, props[expected]);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Server", "Host", "localhost")]
    [InlineData("Host", "Host", "localhost")]
    [InlineData("Data Source", "Host", "localhost")]
    [InlineData("DataSource", "Host", "localhost")]
    [InlineData("Address", "Host", "localhost")]
    [InlineData("Addr", "Host", "localhost")]
    [InlineData("Network Address", "Host", "localhost")]
    [InlineData("Port", "Port", "3306")]
    [InlineData("User ID", "Username", "testuser")]
    [InlineData("UserID", "Username", "testuser")]
    [InlineData("Username", "Username", "testuser")]
    [InlineData("Uid", "Username", "testuser")]
    [InlineData("User name", "Username", "testuser")]
    [InlineData("User", "Username", "testuser")]
    [InlineData("Password", "Password", "my_password")]
    [InlineData("pwd", "Password", "my_password")]
    public void MySqlConnectorDialect_AliasesMapToPropertyName(string alias, string expected, string value)
    {
        var props = new Dictionary<string, string>
        {
            { alias, value },
        };

        var dialect = new MySqlConnectorDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);
        Assert.True(props.ContainsKey(expected));
        Assert.Equal(value, props[expected]);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Host", "Host", "localhost")]
    [InlineData("Server", "Host", "localhost")]
    [InlineData("Port", "Port", "5432")]
    [InlineData("Username", "Username", "testuser")]
    [InlineData("User name", "Username", "testuser")]
    [InlineData("UserID", "Username", "testuser")]
    [InlineData("User ID", "Username", "testuser")]
    [InlineData("Uid", "Username", "testuser")]
    [InlineData("Password", "Password", "my_password")]
    [InlineData("PSW", "Password", "my_password")]
    [InlineData("PWD", "Password", "my_password")]
    public void NpgsqlDialect_AliasesMapToPropertyName(string alias, string expected, string value)
    {
        var props = new Dictionary<string, string>
        {
            { alias, value },
        };

        var dialect = new NpgsqlDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);
        Assert.True(props.ContainsKey(expected));
        Assert.Equal(value, props[expected]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NormalizeConnectionPropertyKeys_Aliases_Normalizes()
    {
        var props = new Dictionary<string, string>
        {
            { "Server", "localhost" },
            { "Uid", "testuser" },
            { "Port", "5432" },
            { "Database", "testdb" },
            { PropertyDefinition.Plugins.Name, "failover" },
        };

        var dialect = new MySqlClientDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);

        Assert.True(props.ContainsKey(PropertyDefinition.Host.Name));
        Assert.Equal("localhost", props[PropertyDefinition.Host.Name]);
        Assert.True(props.ContainsKey(PropertyDefinition.User.Name));
        Assert.Equal("testuser", props[PropertyDefinition.User.Name]);
        Assert.True(props.ContainsKey(PropertyDefinition.Plugins.Name));
        Assert.Equal("failover", props[PropertyDefinition.Plugins.Name]);
        Assert.False(props.ContainsKey("Server"));
        Assert.False(props.ContainsKey("Uid"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NormalizeConnectionPropertyKeys_MultipleAliasesPresent_LastOneWins()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "canonical-host" },
            { "Server", "alias-host" },
        };

        var dialect = new MySqlClientDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);

        Assert.True(props.ContainsKey(PropertyDefinition.Host.Name));
        Assert.Equal("alias-host", props[PropertyDefinition.Host.Name]);
    }
}
