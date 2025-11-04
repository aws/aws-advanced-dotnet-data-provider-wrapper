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

using System.Data.Common;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

public abstract class AbstractTargetConnectionDialect : ITargetConnectionDialect
{
    private const string DefaultPluginCode = "initialConnection,efm,failover";

    private static readonly List<string> SslDisabledKeys = new() { "Ssl Mode", "SllMode" };


    public abstract Type DriverConnectionType { get; }

    public bool IsDialect(Type connectionType)
    {
        return connectionType == this.DriverConnectionType;
    }

    public abstract string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props);

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

        return string.Join("; ", targetConnectionParameters.Select(x => $"{x.Key}={x.Value}"));
    }

    public virtual bool IsSslValidationDisabled(Dictionary<string, string> props)
    {
        foreach (var key in SslDisabledKeys)
        {
            if (props.TryGetValue(key, out var v) &&
                !string.IsNullOrWhiteSpace(v) &&
                (v.Equals("none", StringComparison.OrdinalIgnoreCase) || v.Equals("disable", StringComparison.OrdinalIgnoreCase)))
            {
                return true;
            }
        }

        return false;
    }
}
