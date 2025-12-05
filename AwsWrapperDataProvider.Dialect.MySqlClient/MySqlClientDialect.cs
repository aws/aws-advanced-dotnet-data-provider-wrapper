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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using MySql.Data.MySqlClient;

namespace AwsWrapperDataProvider.Dialect.MySqlClient;

public class MySqlClientDialect : AbstractTargetConnectionDialect
{
    private const string DefaultPluginCode = "initialConnection, failover";

    public override Type DriverConnectionType { get; } = typeof(MySqlConnection);

    protected override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
        return new MySqlConnectionStringBuilder();
    }

    protected override string? MapCanonicalKeyToWrapperProperty(string canonicalKey)
    {
        return canonicalKey.ToLowerInvariant() switch
        {
            "server" => PropertyDefinition.Host.Name,
            "port" => PropertyDefinition.Port.Name,
            "user id" => PropertyDefinition.User.Name,
            "password" => PropertyDefinition.Password.Name,
            _ => null,
        };
    }

    public override string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, bool isForceOpen = false)
    {
        Dictionary<string, string> copyOfProps = new(props);

        if (isForceOpen)
        {
            copyOfProps[DefaultPoolingParameterName] = "false";
        }

        return this.PrepareConnectionString(dialect, hostSpec, copyOfProps, PropertyDefinition.Host);
    }

    public override (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection)
    {
        if (connection is MySqlConnection mySqlConnection)
        {
            return (mySqlConnection.Ping(), null);
        }

        return (false, null);
    }

    public override string GetPluginCodesOrDefault(Dictionary<string, string> props)
    {
        string pluginsCodes = PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCode;
        return pluginsCodes.Contains(PluginCodes.HostMonitoring) ? throw new InvalidOperationException("Invalid usage of Host Monitoring plugin with Mysql dialect.") : pluginsCodes;
    }
}
