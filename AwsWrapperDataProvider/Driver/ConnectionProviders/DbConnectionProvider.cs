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

using System.Collections.ObjectModel;
using System.Data.Common;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.ConnectionProviders;

public class DbConnectionProvider() : IConnectionProvider
{
    private static readonly ILogger<DbConnectionProvider> Logger = LoggerUtils.GetLogger<DbConnectionProvider>();

    private static readonly ReadOnlyDictionary<string, IHostSelector> AcceptedStrategies =
        new(new Dictionary<string, IHostSelector>
        {
            { HighestWeightHostSelector.StrategyName,  new HighestWeightHostSelector() },
            { RandomHostSelector.StrategyName, new RandomHostSelector() },
            { RoundRobinHostSelector.StrategyName, new RoundRobinHostSelector() },
        });

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

        Logger.LogTrace(
            "Connection created: {ConnectionType}@{Id} with Connection string {connectionString},",
            targetConnection.GetType().FullName,
            RuntimeHelpers.GetHashCode(targetConnection),
            connectionString);

        return targetConnection;
    }

    public bool AcceptsStrategy(string strategy)
    {
        return AcceptedStrategies.ContainsKey(strategy);
    }

    public HostSpec? GetHostSpecByStrategy(
        IList<HostSpec> hosts,
        HostRole hostRole,
        string strategy,
        Dictionary<string, string> props)
    {
        IHostSelector hostSelector = AcceptedStrategies.GetValueOrDefault(strategy, AcceptedStrategies[RandomHostSelector.StrategyName]);
        return hostSelector.GetHost(hosts, hostRole, props);
    }

    public string GetTargetName()
    {
        throw new NotImplementedException();
    }
}
