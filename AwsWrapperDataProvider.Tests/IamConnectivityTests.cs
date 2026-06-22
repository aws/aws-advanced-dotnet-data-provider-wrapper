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
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Plugin.Iam.Iam;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class IamConnectivityTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public void PgWrapper_WithIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via IAM...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public void MySqlClientWrapper_WithIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via IAM...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public void MySqlConnectorWrapper_WithIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via IAM...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");
    }

    /// <summary>
    /// Regression test for https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/issues/301.
    /// With the IAM token supplied via Npgsql's periodic password provider (rather than injected into
    /// the connection string), the dialect must reuse a single <see cref="NpgsqlDataSource"/> — and
    /// therefore a single connection pool — across token rotations. Before the fix, each rotation
    /// changed the connection string and spawned a new pool, fragmenting it.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task PgWrapper_IamTokenRotation_DoesNotFragmentPool()
    {
        NpgsqlDialect.ClearDataSources();
        IamAuthPlugin.ClearCache();

        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        const int rotations = 3;
        const int opensPerRotation = 5;

        for (int rotation = 0; rotation < rotations; rotation++)
        {
            // Clearing the token cache forces the next open to generate a fresh token, exactly as a
            // real ~15-minute token expiry would. Before the fix this is what fragmented the pool.
            IamAuthPlugin.ClearCache();

            for (int i = 0; i < opensPerRotation; i++)
            {
                await using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
                await connection.OpenAsync(TestContext.Current.CancellationToken);
                Assert.Equal(ConnectionState.Open, connection.State);

                await using var command = connection.CreateCommand<NpgsqlCommand>();
                command.CommandText = "SELECT 1";
                Assert.Equal(1, Convert.ToInt32(await command.ExecuteScalarAsync()));
            }
        }

        Console.WriteLine($"Opened {rotations * opensPerRotation} connections across {rotations} token generations.");
        Console.WriteLine($"NpgsqlDataSource (pool) count: {NpgsqlDialect.DataSourceCount}");

        // All opens — across every token rotation — must map to a single cached data source / pool.
        Assert.Equal(1, NpgsqlDialect.DataSourceCount);
    }

    /// <summary>
    /// The IAM token must reach Npgsql via the periodic password provider, never through the
    /// connection string. This asserts the opened target connection's connection string carries no
    /// password — the property that keeps the pool key stable (issue #301). Before the fix, the
    /// token was injected as <c>Password=&lt;token&gt;</c> into this string.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task PgWrapper_WithIamPlugin_DoesNotSetPasswordInConnectionString()
    {
        NpgsqlDialect.ClearDataSources();
        IamAuthPlugin.ClearCache();

        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        await using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        // The connection authenticated successfully...
        Assert.Equal(ConnectionState.Open, connection.State);

        // ...yet the underlying driver connection string carries no password (the token is supplied
        // out-of-band via UsePeriodicPasswordProvider). Parse with the driver's own builder so the
        // assertion resolves every password alias (Password/pwd/...) to the canonical property.
        Console.WriteLine($"Target connection string: {connection.ConnectionString}");
        var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
        Assert.True(string.IsNullOrEmpty(builder.Password), $"Expected no password but found one. Connection string: {connection.ConnectionString}");
    }

    /// <summary>
    /// The IAM token must reach MySqlConnector via <c>ProvidePasswordCallback</c>, never through the
    /// connection string. This asserts the opened target connection's connection string carries no
    /// password — the property that keeps the pool key stable (issue #301). Before the fix, the
    /// token was injected as <c>Password=&lt;token&gt;</c> into this string.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public async Task MySqlConnectorWrapper_WithIamPlugin_DoesNotSetPasswordInConnectionString()
    {
        IamAuthPlugin.ClearCache();

        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        await using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        // The connection authenticated successfully...
        Assert.Equal(ConnectionState.Open, connection.State);

        // ...yet the underlying driver connection string carries no password (the token is supplied
        // out-of-band via ProvidePasswordCallback). Parse with the driver's own builder so the
        // assertion resolves every password alias (Password/pwd/...) to the canonical property.
        Console.WriteLine($"Target connection string: {connection.ConnectionString}");
        var builder = new MySqlConnector.MySqlConnectionStringBuilder(connection.ConnectionString);
        Assert.True(string.IsNullOrEmpty(builder.Password), $"Expected no password but found one. Connection string: {connection.ConnectionString}");
    }
}
