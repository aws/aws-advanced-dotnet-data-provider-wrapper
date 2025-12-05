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
    [InlineData("Server", "Host")]
    [InlineData("User ID", "Username")]
    [InlineData("user id", "Username")]
    [InlineData("Uid", "Username")]
    [InlineData("uid", "Username")]
    public void MySqlClientDialect_AliasesMapToPropertyName(string alias, string expected)
    {
        var dialect = new MySqlClientDialect();
        var result = dialect.GetAliasAwsWrapperPropertyName(alias);
        Assert.Equal(expected, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Server", "Host")]
    [InlineData("User ID", "Username")]
    [InlineData("Uid", "Username")]
    public void MySqlConnectorDialect_AliasesMapToPropertyName(string alias, string expected)
    {
        var dialect = new MySqlConnectorDialect();
        var result = dialect.GetAliasAwsWrapperPropertyName(alias);
        Assert.Equal(expected, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Host", "Host")]
    [InlineData("Server", "Host")]
    [InlineData("Username", "Username")]
    [InlineData("User ID", "Username")]
    [InlineData("Uid", "Username")]
    public void NpgsqlDialect_AliasesMapToPropertyName(string alias, string expected)
    {
        var dialect = new NpgsqlDialect();
        var result = dialect.GetAliasAwsWrapperPropertyName(alias);
        Assert.Equal(expected, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NormalizeConnectionPropertyKeys_Aliases_Normalizes()
    {
        var props = new Dictionary<string, string>
        {
            { "Server", "localhost" },
            { "Uid", "testuser" },
            { "Database", "testdb" },
        };

        var dialect = new MySqlClientDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);

        Assert.True(props.ContainsKey("Host"));
        Assert.Equal("localhost", props["Host"]);
        Assert.True(props.ContainsKey("Username"));
        Assert.Equal("testuser", props["Username"]);
        Assert.False(props.ContainsKey("Server"));
        Assert.False(props.ContainsKey("Uid"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NormalizeConnectionPropertyKeys_CanonicalKeyExists_PrefersCanonical()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "canonical-host" },
            { "Server", "alias-host" },
        };

        var dialect = new MySqlClientDialect();
        ConnectionPropertiesUtils.NormalizeConnectionPropertyKeys(dialect, props);

        Assert.True(props.ContainsKey("Host"));
        Assert.Equal("canonical-host", props["Host"]);
        Assert.False(props.ContainsKey("Server"));
    }
}
