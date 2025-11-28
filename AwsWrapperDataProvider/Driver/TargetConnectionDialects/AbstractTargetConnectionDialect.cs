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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

public abstract class AbstractTargetConnectionDialect : ITargetConnectionDialect
{
    private const string DefaultPluginCode = "initialConnection,efm,failover";

    protected const string DefaultPoolingParameterName = "Pooling";

    public abstract Type DriverConnectionType { get; }

    public virtual Dictionary<string, string[]> AwsWrapperPropertyNameAliasesMap { get; } = new();

    public bool IsDialect(Type connectionType)
    {
        return connectionType == this.DriverConnectionType;
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

    public string? GetAliasAwsWrapperPropertyName(string propAlias)
    {
        foreach (KeyValuePair<string, string[]> kvp in this.AwsWrapperPropertyNameAliasesMap)
        {
            if (kvp.Value.Any(s => string.Equals(s, propAlias, StringComparison.OrdinalIgnoreCase)))
            {
                return kvp.Key;
            }
        }

        return null;
    }

    public abstract (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection);

    protected string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, AwsWrapperProperty hostProperty)
    {
        Dictionary<string, string> targetConnectionParameters = props.Where(x =>
            !PropertyDefinition.InternalWrapperProperties
                .Select(prop => prop.Name)
                .Contains(x.Key)).ToDictionary();

        if (hostSpec != null)
        {
            dialect.PrepareConnectionProperties(targetConnectionParameters, hostSpec);
            hostProperty.Set(targetConnectionParameters, hostSpec.Host);
            if (hostSpec.IsPortSpecified)
            {
                PropertyDefinition.Port.Set(targetConnectionParameters, hostSpec.Port.ToString());
            }
        }

        this.NormalizePropertyKeys(targetConnectionParameters);

        return string.Join("; ", targetConnectionParameters.Select(x => $"{x.Key}={x.Value}"));
    }

    /// <summary>
    /// Reverts AwsWrapperProperty names to default connection parameter names.
    /// </summary>
    /// <param name="props">Dictionary of wrapper properties.</param>
    private void NormalizePropertyKeys(Dictionary<string, string> props)
    {
        foreach (var kvp in this.AwsWrapperPropertyNameAliasesMap)
        {
            var canonicalKey = kvp.Key;
            var aliases = kvp.Value;

            if (aliases.Length == 0 || !props.TryGetValue(canonicalKey, out var value))
            {
                continue;
            }

            props.Remove(canonicalKey);
            props[aliases[0]] = value;
        }
    }
}
