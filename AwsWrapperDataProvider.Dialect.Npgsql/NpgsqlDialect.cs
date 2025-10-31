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
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Dialect.Npgsql;

public class NpgsqlDialect : AbstractTargetConnectionDialect
{
    public override Type DriverConnectionType { get; } = typeof(NpgsqlConnection);

    public override string PrepareConnectionString(
        IDialect dialect,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isForceOpen = false)
    {
        Dictionary<string, string> copyOfProps = new(props);

        if (isForceOpen)
        {
            copyOfProps["Pooling"] = "false";
        }

        return this.PrepareConnectionString(dialect, hostSpec, copyOfProps, PropertyDefinition.Host);
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
