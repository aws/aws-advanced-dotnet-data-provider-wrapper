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
using System.Runtime.InteropServices.Marshalling;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;

namespace AwsWrapperDataProvider.Driver.ConnectionProviders;

public class DbConnectionProvider() : IConnectionProvider
{
    public bool AcceptsUrl(string protocol, HostSpec hostSpec, Dictionary<string, string> props)
    {
        throw new NotImplementedException();
    }

    public bool AcceptsUrl(HostSpec hostSpec, Dictionary<string, string> props)
    {
        throw new NotImplementedException();
    }

    public DbConnection CreateDbConnection(
        IDialect dialect,
        ITargetConnectionDialect targetConnectionDialect,
        HostSpec? hostSpec,
        Dictionary<string, string> props)
    {
        Type targetConnectionType = targetConnectionDialect.DriverConnectionType;
        string connectionString = targetConnectionDialect.PrepareConnectionString(dialect, hostSpec, props);

        DbConnection? targetConnection = string.IsNullOrWhiteSpace(connectionString)
            ? (DbConnection?)Activator.CreateInstance(targetConnectionType)
            : (DbConnection?)Activator.CreateInstance(targetConnectionType, connectionString);

        if (targetConnection == null)
        {
            throw new InvalidCastException("Unable to create connection.");
        }

        return targetConnection;
    }

    public bool AcceptsStrategy(HostRole hostRole, string strategy)
    {
        // TODO: implement Functions to use strategy.
        return true;
    }

    public HostSpec? GetHostSpecByStrategy(
        IList<HostSpec> hosts,
        HostRole hostRole,
        string strategy,
        Dictionary<string, string> props)
    {
        // TODO: Implement Function to use strategy.
        foreach (HostSpec hostSpec in hosts)
        {
            if (hostSpec.Role == hostRole)
            {
                return hostSpec;
            }
        }

        return null;
    }

    public string GetTargetName()
    {
        throw new NotImplementedException();
    }
}
