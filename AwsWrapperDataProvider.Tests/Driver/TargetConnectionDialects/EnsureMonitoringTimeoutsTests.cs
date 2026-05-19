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

namespace AwsWrapperDataProvider.Tests.Driver.TargetConnectionDialects;

public class EnsureMonitoringTimeoutsTests
{
    // --- Npgsql ---

    [Fact]
    [Trait("Category", "Unit")]
    public void Npgsql_NoTimeoutSet_AddsDefaults()
    {
        var dialect = new NpgsqlDialect();
        var props = new Dictionary<string, string> { { "Host", "localhost" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("5", props["Timeout"]);
        Assert.Equal("5", props["Command Timeout"]);
        Assert.Equal("localhost", props["Host"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Npgsql_ConnectTimeoutSet_DoesNotOverride()
    {
        var dialect = new NpgsqlDialect();
        var props = new Dictionary<string, string> { { "Timeout", "10" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["Timeout"]);
        Assert.Equal("5", props["Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Npgsql_CommandTimeoutSet_DoesNotOverride()
    {
        var dialect = new NpgsqlDialect();
        var props = new Dictionary<string, string> { { "Command Timeout", "20" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("5", props["Timeout"]);
        Assert.Equal("20", props["Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Npgsql_BothTimeoutsSet_DoesNotOverride()
    {
        var dialect = new NpgsqlDialect();
        var props = new Dictionary<string, string>
        {
            { "Timeout", "10" },
            { "Command Timeout", "20" },
        };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["Timeout"]);
        Assert.Equal("20", props["Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Npgsql_WrapperPropsPreserved()
    {
        var dialect = new NpgsqlDialect();
        var props = new Dictionary<string, string>
        {
            { "Host", "myhost" },
            { "Plugins", "failover" },
        };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("myhost", props["Host"]);
        Assert.Equal("failover", props["Plugins"]);
        Assert.Equal("5", props["Timeout"]);
        Assert.Equal("5", props["Command Timeout"]);
    }

    // --- MySqlConnector ---

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_NoTimeoutSet_AddsDefaults()
    {
        var dialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string> { { "Server", "localhost" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("5", props["Connection Timeout"]);
        Assert.Equal("5", props["Default Command Timeout"]);
        Assert.Equal("localhost", props["Server"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_ConnectTimeoutSet_DoesNotOverride()
    {
        var dialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string> { { "ConnectionTimeout", "10" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["ConnectionTimeout"]);
        Assert.Equal("5", props["Default Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_CommandTimeoutSet_DoesNotOverride()
    {
        var dialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string> { { "DefaultCommandTimeout", "20" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("5", props["Connection Timeout"]);
        Assert.Equal("20", props["DefaultCommandTimeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_BothTimeoutsSet_DoesNotOverride()
    {
        var dialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string>
        {
            { "ConnectionTimeout", "10" },
            { "DefaultCommandTimeout", "20" },
        };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["ConnectionTimeout"]);
        Assert.Equal("20", props["DefaultCommandTimeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_AliasUsed_DoesNotOverride()
    {
        var dialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string> { { "Connection Timeout", "10" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["Connection Timeout"]);
        Assert.Equal("5", props["Default Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlConnector_WrapperPropsPreserved()
    {
        var dialect = new MySqlConnectorDialect();
        var props = new Dictionary<string, string>
        {
            { "Server", "myhost" },
            { "Plugins", "failover" },
        };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("myhost", props["Server"]);
        Assert.Equal("failover", props["Plugins"]);
        Assert.Equal("5", props["Connection Timeout"]);
        Assert.Equal("5", props["Default Command Timeout"]);
    }

    // --- MySqlClient ---
    // MySql.Data Connector/NET canonical key names in builder.Keys: "connectiontimeout", "defaultcommandtimeout"

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClient_NoTimeoutSet_AddsDefaults()
    {
        var dialect = new MySqlClientDialect();
        var props = new Dictionary<string, string> { { "Server", "localhost" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("5", props["connectiontimeout"]);
        Assert.Equal("5", props["defaultcommandtimeout"]);
        Assert.Equal("localhost", props["Server"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClient_ConnectTimeoutSet_DoesNotOverride()
    {
        var dialect = new MySqlClientDialect();
        var props = new Dictionary<string, string> { { "Connection Timeout", "10" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["Connection Timeout"]);
        Assert.Equal("5", props["defaultcommandtimeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClient_CommandTimeoutSet_DoesNotOverride()
    {
        var dialect = new MySqlClientDialect();
        var props = new Dictionary<string, string> { { "Default Command Timeout", "20" } };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("5", props["connectiontimeout"]);
        Assert.Equal("20", props["Default Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClient_BothTimeoutsSet_DoesNotOverride()
    {
        var dialect = new MySqlClientDialect();
        var props = new Dictionary<string, string>
        {
            { "Connection Timeout", "10" },
            { "Default Command Timeout", "20" },
        };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("10", props["Connection Timeout"]);
        Assert.Equal("20", props["Default Command Timeout"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void MySqlClient_WrapperPropsPreserved()
    {
        var dialect = new MySqlClientDialect();
        var props = new Dictionary<string, string>
        {
            { "Server", "myhost" },
            { "Plugins", "failover" },
        };

        dialect.EnsureMonitoringTimeouts(props, 5, 5);

        Assert.Equal("myhost", props["Server"]);
        Assert.Equal("failover", props["Plugins"]);
        Assert.Equal("5", props["connectiontimeout"]);
        Assert.Equal("5", props["defaultcommandtimeout"]);
    }
}
