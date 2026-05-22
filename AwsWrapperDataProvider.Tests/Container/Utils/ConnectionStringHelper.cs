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

using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class ConnectionStringHelper
{
    public static string GetUrl(DatabaseEngine engine, string host, int? port, string? username, string? password, string? dbName, int commandTimeout = 30, int connectionTimeout = 30, string? plugins = null, bool enablePooling = true)
    {
        string url;
        switch (engine)
        {
            case DatabaseEngine.MYSQL:
                MySqlConnectionStringBuilder mySqlConnectionStringBuilder = new();
                mySqlConnectionStringBuilder.Server = host;
                if (port != null & port > 0)
                {
                    mySqlConnectionStringBuilder.Port = (uint)port!;
                }

                if (username != null)
                {
                    mySqlConnectionStringBuilder.UserID = username;
                }

                if (password != null)
                {
                    mySqlConnectionStringBuilder.Password = password;
                }

                if (dbName != null)
                {
                    mySqlConnectionStringBuilder.Database = dbName;
                }

                mySqlConnectionStringBuilder.DefaultCommandTimeout = (uint)commandTimeout;
                mySqlConnectionStringBuilder.ConnectionTimeout = (uint)connectionTimeout;
                mySqlConnectionStringBuilder.Pooling = enablePooling;

                url = mySqlConnectionStringBuilder.ConnectionString;
                break;
            case DatabaseEngine.PG:
                NpgsqlConnectionStringBuilder npgsqlConnectionStringBuilder = new();
                npgsqlConnectionStringBuilder.Host = host;
                if (port != null & port > 0)
                {
                    npgsqlConnectionStringBuilder.Port = (int)port!;
                }

                if (username != null)
                {
                    npgsqlConnectionStringBuilder.Username = username;
                }

                if (password != null)
                {
                    npgsqlConnectionStringBuilder.Password = password;
                }

                if (dbName != null)
                {
                    npgsqlConnectionStringBuilder.Database = dbName;
                }

                npgsqlConnectionStringBuilder.Timeout = connectionTimeout;
                npgsqlConnectionStringBuilder.CommandTimeout = commandTimeout;
                npgsqlConnectionStringBuilder.Pooling = enablePooling;
                npgsqlConnectionStringBuilder.SslMode = SslMode.Require;

                url = npgsqlConnectionStringBuilder.ConnectionString;
                break;
            default:
                throw new NotSupportedException($"Unsupported database engine: {engine}");
        }

        if (plugins != null)
        {
            url += $"; Plugins={plugins}";
            url += BuildTelemetrySuffix();
        }

        return url;
    }

    /// <summary>
    /// Builds the wrapper-specific telemetry connection-string fragment
    /// (with a leading separator) based on the test environment's feature
    /// flags. Returns an empty string when telemetry features are off so
    /// the helper is a no-op for environments that didn't spin up the
    /// telemetry containers.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Mirrors JDBC's <c>ConnectionStringHelper.getDefaultProperties()</c>:
    /// when <see cref="TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED"/> or
    /// <see cref="TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED"/> is
    /// set on the test request, this enables the wrapper's telemetry
    /// pipeline and pins the backends so wrapper spans / counters actually
    /// flow to the daemons started by the host-side
    /// <c>TestEnvironmentConfig</c>:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><c>TelemetryTracesBackend=XRAY</c> when the
    ///   traces feature is set; <c>NONE</c> otherwise.</description></item>
    ///   <item><description><c>TelemetryMetricsBackend=OTLP</c> when the
    ///   metrics feature is set; <c>NONE</c> otherwise.</description></item>
    ///   <item><description><c>TelemetrySubmitTopLevel=true</c> matches
    ///   JDBC's default and ensures wrapper entry-point spans surface as
    ///   root spans even when the test runner has no surrounding
    ///   Activity context (the typical case for an xUnit test
    ///   method).</description></item>
    /// </list>
    /// <para>
    /// Only emitted when the caller has passed a non-null <c>plugins</c>
    /// argument to <see cref="GetUrl"/>: a null <c>plugins</c> is the
    /// signal that the caller is opening a bare ADO.NET connection (e.g.,
    /// <see cref="AuroraTestUtils"/> for cluster-health probes) where the
    /// wrapper-only telemetry properties would be rejected by the
    /// underlying provider's connection-string parser.
    /// </para>
    /// </remarks>
    private static string BuildTelemetrySuffix()
    {
        // TestEnvironment.Env is a lazy singleton: this property triggers
        // env initialization on first call. By the time any helper-using
        // test runs, the env (and therefore the X-Ray daemon address /
        // OTel meter provider) is already wired up.
        var features = TestEnvironment.Env.Info.Request.Features;
        bool tracesEnabled = features.Contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED);
        bool metricsEnabled = features.Contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED);

        if (!tracesEnabled && !metricsEnabled)
        {
            return string.Empty;
        }

        // Emit a single combined fragment so the resulting connection
        // string stays compact and human-readable in test logs.
        string tracesBackend = tracesEnabled ? "XRAY" : "NONE";
        string metricsBackend = metricsEnabled ? "OTLP" : "NONE";
        return ";EnableTelemetry=true"
            + ";TelemetrySubmitTopLevel=true"
            + $";TelemetryTracesBackend={tracesBackend}"
            + $";TelemetryMetricsBackend={metricsBackend}";
    }
}
