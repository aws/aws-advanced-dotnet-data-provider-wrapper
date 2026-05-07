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

using AwsWrapperDataProvider.Driver.Dialects;

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// Helper for populating OpenTelemetry semantic-convention attributes on
/// telemetry trace contexts produced by the AWS wrapper. Attribute names
/// follow <see href="https://opentelemetry.io/docs/specs/semconv/database/database-spans/">
/// the OpenTelemetry database semantic conventions</see>.
/// </summary>
internal static class TelemetryAttributes
{
    /// <summary>Attribute key for the database management system.</summary>
    internal const string DbSystemKey = "db.system";

    /// <summary>Attribute key for the authenticated database user.</summary>
    internal const string DbUserKey = "db.user";

    /// <summary>Attribute key for the target host name (per OTel conventions).</summary>
    internal const string NetPeerNameKey = "net.peer.name";

    /// <summary>Attribute key for the target host port.</summary>
    internal const string NetPeerPortKey = "net.peer.port";

    /// <summary>Fallback value for unknown database systems.</summary>
    internal const string DbSystemOther = "other_sql";

    /// <summary>
    /// Maps an <see cref="IDialect"/> implementation to the corresponding
    /// OpenTelemetry <c>db.system</c> attribute value. Subclasses of the
    /// known base dialects (for example, <c>RdsMySqlDialect</c>,
    /// <c>AuroraPgDialect</c>) resolve to the same value as their base.
    /// </summary>
    /// <param name="dialect">The dialect to classify.</param>
    /// <returns>The OpenTelemetry <c>db.system</c> value.</returns>
    public static string GetDbSystem(IDialect? dialect) => dialect switch
    {
        MySqlDialect => "mysql",
        PgDialect => "postgresql",
        _ => DbSystemOther,
    };

    /// <summary>
    /// Sets the connection-level OpenTelemetry attributes (<c>db.system</c>,
    /// <c>db.user</c>, <c>net.peer.name</c>, <c>net.peer.port</c>) on the
    /// supplied telemetry context, reading user, host, and port from the
    /// connection properties. Attributes whose source value is missing or
    /// empty are omitted.
    /// </summary>
    /// <param name="context">The telemetry context to populate.</param>
    /// <param name="dialect">The dialect used to populate <c>db.system</c>.</param>
    /// <param name="properties">The connection properties used to populate
    /// <c>db.user</c>, <c>net.peer.name</c>, and <c>net.peer.port</c>.</param>
    public static void SetConnectionAttributes(
        ITelemetryContext context,
        IDialect? dialect,
        Dictionary<string, string> properties)
    {
        context.SetAttribute(DbSystemKey, GetDbSystem(dialect));

        string? user = PropertyDefinition.User.GetString(properties);
        if (!string.IsNullOrEmpty(user))
        {
            context.SetAttribute(DbUserKey, user);
        }

        string? host = PropertyDefinition.Host.GetString(properties);
        if (!string.IsNullOrEmpty(host))
        {
            context.SetAttribute(NetPeerNameKey, host);
        }

        string? port = PropertyDefinition.Port.GetString(properties);
        if (!string.IsNullOrEmpty(port))
        {
            context.SetAttribute(NetPeerPortKey, port);
        }
    }
}
