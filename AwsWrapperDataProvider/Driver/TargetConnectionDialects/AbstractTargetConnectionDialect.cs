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
using System.Data.Common;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

public abstract class AbstractTargetConnectionDialect : ITargetConnectionDialect
{
    private const string DefaultPluginCode = "initialConnection,auroraConnectionTracker,failover,efm";

    protected const string DefaultPoolingParameterName = "Pooling";

    /// <summary>
    /// Prefix marking a connection property as an internal runtime handle (e.g. the dynamic password
    /// provider key). Keys with this prefix are never emitted into a target connection string, and
    /// are rejected from user-supplied connection strings when they are parsed.
    /// </summary>
    internal const string ReservedPropertyPrefix = "__";

    public abstract Type DriverConnectionType { get; }

    public virtual bool SupportsPasswordProvider => false;

    public bool IsDialect(Type connectionType)
    {
        return connectionType == this.DriverConnectionType;
    }

    public virtual DbConnection CreateConnection(Type connectionType, string connectionString, Dictionary<string, string> props)
    {
        DbConnection? connection = string.IsNullOrWhiteSpace(connectionString)
            ? (DbConnection?)Activator.CreateInstance(connectionType)
            : (DbConnection?)Activator.CreateInstance(connectionType, connectionString);

        return connection ?? throw new InvalidCastException(Resources.Error_InvalidConnection);
    }

    public abstract string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, bool isForcedOpen = false);

    public ISet<string> GetAllowedOnConnectionMethodNames()
    {
        return new HashSet<string>
        {
            "DbConnection.get_ConnectionString",
            "DbConnection.get_ConnectionTimeout",
            "DbConnection.get_Database",
            "DbConnection.get_DataSource",
            "DbConnection.get_State",
            "DbConnection.Close",
            "DbConnection.CloseAsync",
            "DbConnection.DisposeAsync",
            "DbConnection.get_CanCreateBatch",
            "DbConnection.CreateBatch",
            "DbConnection.CreateCommand",
            "DbConnection.OpenAsync",
            "DbConnection.get_Site",
            "DbConnection.Dispose",
            "DbConnection.get_Container",
            "DbConnection.ToString",
            "DbConnection.GetType",
            "DbConnection.GetHashCode",
            "DbCommand.get_Site",
            "DbCommand.Dispose",
            "DbCommand.get_Container",
            "DbCommand.ToString",
            "DbCommand.GetType",
            "DbCommand.GetHashCode",
        };
    }

    public virtual string GetPluginCodesOrDefault(Dictionary<string, string> props)
    {
        return PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCode;
    }

    public abstract string? MapCanonicalKeyToWrapperProperty(string canonicalKey);

    public virtual void EnsureMonitoringTimeouts(
        Dictionary<string, string> props,
        int defaultConnectTimeoutSec,
        int defaultCommandTimeoutSec)
    {
        // Default no-op. Driver-specific dialects override this.
    }

    public bool IsSyntaxError(DbException ex)
    {
        return ex.SqlState != null && ex.SqlState.StartsWith("42");
    }

    public abstract (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection);

    protected string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, AwsWrapperProperty hostProperty)
    {
        Dictionary<string, string> targetConnectionParameters = props
            .Where(x => !PropertyDefinition.IsInternalWrapperPropertyKey(x.Key)
                        && !x.Key.StartsWith(ReservedPropertyPrefix, StringComparison.Ordinal)
                        && !PropertyDefinition.MonitoringPropertyPrefixes.Any(prefix => x.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
            .ToDictionary(x => x.Key, x => x.Value);

        if (hostSpec == null)
        {
            return this.NormalizeConnectionString(targetConnectionParameters);
        }

        dialect.PrepareConnectionProperties(targetConnectionParameters, hostSpec);
        hostProperty.Set(targetConnectionParameters, hostSpec.Host);
        if (hostSpec.IsPortSpecified)
        {
            PropertyDefinition.Port.Set(targetConnectionParameters, hostSpec.Port.ToString());
        }

        return this.NormalizeConnectionString(targetConnectionParameters);
    }

    protected string NormalizeConnectionString(Dictionary<string, string> props)
    {
        var builder = this.CreateConnectionStringBuilder();

        foreach (var kvp in props)
        {
            try
            {
                builder[kvp.Key] = kvp.Value;
            }
            catch (ArgumentException)
            {
                // Key not recognized by the driver's connection string builder — skip it.
                // This can happen for wrapper-internal properties like ConnectTimeout/CommandTimeout.
            }
        }

        return builder.ConnectionString;
    }

    public virtual DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
        return new DbConnectionStringBuilder();
    }
}
