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

using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver.Auth;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Dialect.Npgsql;

public class NpgsqlDialect : AbstractTargetConnectionDialect
{
    /// <summary>
    /// Cache of <see cref="NpgsqlDataSource"/> instances keyed by the (password-less) connection
    /// string. Each data source owns its own connection pool and a password provider (configured via
    /// <c>UsePasswordProvider</c>) that supplies the current token when a new physical connection is
    /// opened, so a rotating token never changes the connection string and the pool key stays stable.
    /// Process-lifetime and bounded by the number of distinct endpoints.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Lazy<NpgsqlDataSource>> DataSources = new();

    public override Type DriverConnectionType { get; } = typeof(NpgsqlConnection);

    public override bool SupportsPasswordProvider => true;

    public override DbConnection CreateConnection(Type connectionType, string connectionString, Dictionary<string, string> props)
    {
        string? key = props.GetValueOrDefault(PasswordProviderRegistry.ProviderKeyPropertyName);
        if (key == null
            || !PasswordProviderRegistry.TryGet(key, out PasswordProviderRegistration? registration))
        {
            return base.CreateConnection(connectionType, connectionString, props);
        }

        // The connection string carries no password (the auth plugin removed it), so the password
        // provider is authoritative. The provider is invoked on each new physical connection open and
        // serves the current token from the auth plugin's in-memory cache, so it returns quickly on
        // the common path. The data source is cached by connection string so the same pool is reused
        // across token rotations. The Lazy wrapper ensures Build() runs exactly once per connection
        // string even if multiple threads race here (see DataSources remarks).
        Lazy<NpgsqlDataSource> dataSource = DataSources.GetOrAdd(connectionString, cs =>
            new Lazy<NpgsqlDataSource>(() =>
            {
                var builder = new NpgsqlDataSourceBuilder(cs);
                builder.UsePasswordProvider(
                    _ => registration.Provider(CancellationToken.None).AsTask().GetAwaiter().GetResult(),
                    (_, ct) => registration.Provider(ct));
                return builder.Build();
            }));

        return dataSource.Value.CreateConnection();
    }

    /// <summary>
    /// Gets the number of cached <see cref="NpgsqlDataSource"/> instances. Because each data source
    /// owns one connection pool keyed by the (password-less) connection string, this is the number
    /// of distinct pools the dialect has created — used by tests to assert that token rotation does
    /// not fragment the pool.
    /// </summary>
    internal static int DataSourceCount => DataSources.Count;

    /// <summary>
    /// Disposes and clears all cached data sources. Intended for test isolation.
    /// </summary>
    internal static void ClearDataSources()
    {
        foreach (Lazy<NpgsqlDataSource> dataSource in DataSources.Values)
        {
            // Only dispose data sources that were actually built; forcing .Value here would
            // needlessly construct a data source (and its pool/timer) just to dispose it.
            if (dataSource.IsValueCreated)
            {
                dataSource.Value.Dispose();
            }
        }

        DataSources.Clear();
    }

    public override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
        return new NpgsqlConnectionStringBuilder();
    }

    public override string? MapCanonicalKeyToWrapperProperty(string canonicalKey)
    {
        return canonicalKey.ToLowerInvariant() switch
        {
            "host" => PropertyDefinition.Host.Name,
            "port" => PropertyDefinition.Port.Name,
            "username" => PropertyDefinition.User.Name,
            "password" => PropertyDefinition.Password.Name,
            _ => null,
        };
    }

    public override string PrepareConnectionString(
        IDialect dialect,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isForceOpen = false)
    {
        Dictionary<string, string> copyOfProps = new(props);

        if (isForceOpen)
        {
            copyOfProps[DefaultPoolingParameterName] = "false";
        }

        return this.PrepareConnectionString(dialect, hostSpec, copyOfProps, PropertyDefinition.Host);
    }

    public override void EnsureMonitoringTimeouts(
        Dictionary<string, string> props,
        int defaultConnectTimeoutSec,
        int defaultCommandTimeoutSec)
    {
        var builder = new NpgsqlConnectionStringBuilder();
        foreach (var kvp in props)
        {
            try
            {
                builder[kvp.Key] = kvp.Value;
            }
            catch (ArgumentException)
            {
            }
        }

        var setKeys = new HashSet<string>(builder.Keys.Cast<string>(), StringComparer.OrdinalIgnoreCase);

        builder.Timeout = defaultConnectTimeoutSec;
        builder.CommandTimeout = defaultCommandTimeoutSec;

        foreach (string key in builder.Keys.Cast<string>())
        {
            if (!setKeys.Contains(key))
            {
                props[key] = builder[key]?.ToString() ?? string.Empty;
            }
        }
    }

    public override (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection)
    {
        try
        {
            if (connection is NpgsqlConnection npgsqlConnection)
            {
                using var cmd = new NpgsqlCommand("SELECT 1", npgsqlConnection);
                cmd.ExecuteScalar();
                return (true, null);
            }
        }
        catch (Exception ex)
        {
            return (false, ex);
        }

        return (false, null);
    }
}
