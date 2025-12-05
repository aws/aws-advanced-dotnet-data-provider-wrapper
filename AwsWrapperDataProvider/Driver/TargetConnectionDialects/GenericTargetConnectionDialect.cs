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

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

public class GenericTargetConnectionDialect : AbstractTargetConnectionDialect
{
    public override Type DriverConnectionType { get; }

    public GenericTargetConnectionDialect(Type connectionType)
    {
        this.DriverConnectionType = connectionType;
    }

    protected override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
        return new DbConnectionStringBuilder();
    }

    protected override string? MapCanonicalKeyToWrapperProperty(string canonicalKey)
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

    public override string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, bool isForceOpen = false)
    {
        return this.PrepareConnectionString(dialect, hostSpec, props, PropertyDefinition.Host);
    }

    public override (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection)
    {
        try
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT 1";
                command.CommandType = CommandType.Text;
                command.ExecuteScalar();
            }

            return (true, null);
        }
        catch (Exception ex)
        {
            return (false, ex);
        }
    }
}
