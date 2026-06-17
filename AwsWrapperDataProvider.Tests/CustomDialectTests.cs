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
using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Integration tests that verify a user-supplied custom database dialect and a custom target
/// connection dialect are honored end-to-end against a real cluster, for both MySQL and PostgreSQL.
///
/// The dummy dialects extend the production Aurora dialect / target connection dialect for the
/// engine under test, so the connection behaves like a normal wrapper connection while recording,
/// via a static flag, that the wrapper actually routed through it. The custom dialects are supplied
/// two ways:
/// <list type="bullet">
/// <item>through the public <see cref="AwsWrapperConnectionStringBuilder.CustomDialect"/> and
/// <see cref="AwsWrapperConnectionStringBuilder.CustomTargetConnectionDialect"/> properties; and</item>
/// <item>by appending the <c>CustomDialect</c> / <c>CustomTargetConnectionDialect</c> keys directly
/// to a raw connection string.</item>
/// </list>
/// Each test asserts both flags were set after a successful query.
/// </summary>
public class CustomDialectTests : IntegrationTestBase
{
    private readonly ITestOutputHelper logger;

    public CustomDialectTests(ITestOutputHelper output)
    {
        this.logger = output;
        CustomMySqlDialect.Reset();
        CustomPgDialect.Reset();
        CustomMySqlConnectorDialect.Reset();
        CustomNpgsqlDialect.Reset();
    }

    [Fact(Timeout = 60 * 60 * 1000)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task CustomDialects_AreHonored_UsingConnectionStringBuilder()
    {
        var config = GetEngineConfig();

        var builder = new AwsWrapperConnectionStringBuilder
        {
            ConnectionString = BaseConnectionString(),
            CustomDialect = config.DatabaseDialect.AssemblyQualifiedName,
            CustomTargetConnectionDialect = config.TargetConnectionDialect.AssemblyQualifiedName,
        };

        await this.AssertCustomDialectsUsedAsync(builder.ConnectionString, config);
    }

    [Fact(Timeout = 60 * 60 * 1000)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task CustomDialects_AreHonored_UsingRawConnectionString()
    {
        var config = GetEngineConfig();

        // Append the custom dialect keys directly to a raw connection string instead of using the
        // AwsWrapperConnectionStringBuilder. The AssemblyQualifiedName values contain '=' and ','
        // characters; the wrapper's parser splits each property on its first '=' and on ';' between
        // properties, so the values survive intact.
        string connectionString = BaseConnectionString() +
            $";CustomDialect={config.DatabaseDialect.AssemblyQualifiedName}" +
            $";CustomTargetConnectionDialect={config.TargetConnectionDialect.AssemblyQualifiedName}";

        await this.AssertCustomDialectsUsedAsync(connectionString, config);
    }

    private static string BaseConnectionString()
    {
        return ConnectionStringHelper.GetUrl(
            Engine,
            Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            plugins: "failover");
    }

    private static EngineConfig GetEngineConfig()
    {
        return Engine switch
        {
            DatabaseEngine.MYSQL => new EngineConfig(
                typeof(CustomMySqlDialect),
                typeof(CustomMySqlConnectorDialect),
                () => CustomMySqlDialect.WasUsed,
                () => CustomMySqlConnectorDialect.WasUsed),
            DatabaseEngine.PG => new EngineConfig(
                typeof(CustomPgDialect),
                typeof(CustomNpgsqlDialect),
                () => CustomPgDialect.WasUsed,
                () => CustomNpgsqlDialect.WasUsed),
            _ => throw new InvalidOperationException($"Unsupported engine {Engine}"),
        };
    }

    private async Task AssertCustomDialectsUsedAsync(string connectionString, EngineConfig config)
    {
        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async: true);
        Assert.Equal(ConnectionState.Open, connection.State);

        string? instanceId = await AuroraUtils.QueryInstanceId(connection, async: true);
        this.logger.WriteLine($"Connected to instance: {instanceId}");
        Assert.False(string.IsNullOrEmpty(instanceId));

        Assert.True(
            config.DatabaseDialectUsed(),
            "Expected the custom database dialect to be used when establishing the connection.");
        Assert.True(
            config.TargetConnectionDialectUsed(),
            "Expected the custom target connection dialect to be used when establishing the connection.");
    }

    private sealed record EngineConfig(
        Type DatabaseDialect,
        Type TargetConnectionDialect,
        Func<bool> DatabaseDialectUsed,
        Func<bool> TargetConnectionDialectUsed);

    // ----- Dummy database dialects (extend the production Aurora dialects) -----

    public class CustomMySqlDialect : AuroraMySqlDialect
    {
        public static bool WasUsed { get; private set; }

        public static void Reset() => WasUsed = false;

        public override void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
        {
            WasUsed = true;
            base.PrepareConnectionProperties(props, hostSpec);
        }
    }

    public class CustomPgDialect : AuroraPgDialect
    {
        public static bool WasUsed { get; private set; }

        public static void Reset() => WasUsed = false;

        public override void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
        {
            WasUsed = true;
            base.PrepareConnectionProperties(props, hostSpec);
        }
    }

    // ----- Dummy target connection dialects (extend the production target dialects) -----

    public class CustomMySqlConnectorDialect : MySqlConnectorDialect
    {
        public static bool WasUsed { get; private set; }

        public static void Reset() => WasUsed = false;

        public override string PrepareConnectionString(
            IDialect dialect,
            HostSpec? hostSpec,
            Dictionary<string, string> props,
            bool isForceOpen = false)
        {
            WasUsed = true;
            return base.PrepareConnectionString(dialect, hostSpec, props, isForceOpen);
        }
    }

    public class CustomNpgsqlDialect : NpgsqlDialect
    {
        public static bool WasUsed { get; private set; }

        public static void Reset() => WasUsed = false;

        public override string PrepareConnectionString(
            IDialect dialect,
            HostSpec? hostSpec,
            Dictionary<string, string> props,
            bool isForceOpen = false)
        {
            WasUsed = true;
            return base.PrepareConnectionString(dialect, hostSpec, props, isForceOpen);
        }
    }
}
