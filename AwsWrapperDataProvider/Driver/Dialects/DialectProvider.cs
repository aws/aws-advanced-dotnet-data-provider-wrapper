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
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.Dialects;

public static class DialectProvider
{
    private static readonly Dictionary<Type, IDialect> _dialectCache = new();

    private static readonly Dictionary<Type, string> ConnectionToDatasourceMap = new()
    {
        { typeof(Npgsql.NpgsqlConnection), "postgres" },
        { typeof(MySqlConnector.MySqlConnection), "mysql" },
        { typeof(MySql.Data.MySqlClient.MySqlConnection), "mysql" },
    };

    // TODO: Properly map RdsUrlTypes to IDialect.
    private static readonly Dictionary<(RdsUrlType UrlType, string DatasourceType), Type> DatasourceTypeToDialectMap = new()
    {
        { (RdsUrlType.IpAddress, "postgres"), typeof(PgDialect) },
        { (RdsUrlType.RdsInstance, "postgres"), typeof(PgDialect) },
        { (RdsUrlType.RdsWriterCluster, "postgres"), typeof(PgDialect) },
        { (RdsUrlType.RdsReaderCluster, "postgres"), typeof(PgDialect) },
        { (RdsUrlType.IpAddress, "mysql"), typeof(MysqlDialect) },
        { (RdsUrlType.RdsInstance, "mysql"), typeof(MysqlDialect) },
        { (RdsUrlType.RdsWriterCluster, "mysql"), typeof(MysqlDialect) },
        { (RdsUrlType.RdsReaderCluster, "mysql"), typeof(MysqlDialect) },
    };

    public static IDialect GuessDialect(
        Dictionary<string, string> props,
        ConfigurationProfile? configurationProfile)
    {
        return configurationProfile?.Dialect ?? GuessDialect(props);
    }

    public static IDialect GuessDialect(Dictionary<string, string> props)
    {
        // Check for custom dialect in properties
        if (PropertyDefinition.TargetDialect.GetString(props) is { } customDialectTypeName &&
            !string.IsNullOrEmpty(customDialectTypeName))
        {
            // Try to find and instantiate the custom dialect type
            Type? customDialectType = Type.GetType(customDialectTypeName);

            return GetDialectFromType(customDialectType) ??
                   throw new InvalidOperationException($"Failed to instantiate custom dialect type '{customDialectTypeName}'");
        }

        string url = PropertyDefinition.GetConnectionUrl(props);
        RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(url);
        Type targetConnectionType = Type.GetType(PropertyDefinition.TargetConnectionType.GetString(props)!) ??
                                    throw new InvalidCastException("Target connection type not found.");
        string targetDatasourceType = ConnectionToDatasourceMap.GetValueOrDefault(targetConnectionType) ?? "unknown";
        Type dialectType = DatasourceTypeToDialectMap.GetValueOrDefault((rdsUrlType, targetDatasourceType)) ?? typeof(UnknownDialect);
        return GetDialectFromType(dialectType) ??
               throw new InvalidOperationException($"Failed to instantiate dialect type '{dialectType.Name}'");
    }

    public static IDialect UpdateDialect(IDbConnection connection, IDialect currDialect)
    {
        IList<Type> dialectCandidates = currDialect.DialectUpdateCandidates;

        foreach (Type dialectCandidate in dialectCandidates)
        {
            IDialect dialect = GetDialectFromType(dialectCandidate) ??
                throw new ArgumentException("Invalid dialect type provided.");
            if (dialect.IsDialect(connection))
            {
                return dialect;
            }
        }

        if (currDialect.IsDialect(connection))
        {
            return currDialect;
        }

        throw new ArgumentException("Unable to find valid dialect type for connection.");
    }

    private static IDialect? GetDialectFromType(Type? dialectType)
    {
        if (dialectType != null && typeof(IDialect).IsAssignableFrom(dialectType))
        {
            _dialectCache.TryGetValue(dialectType, out IDialect? dialect);

            if (dialect == null)
            {
                dialect = (IDialect)Activator.CreateInstance(dialectType)!;
                _dialectCache[dialectType] = dialect;
            }

            return dialect;
        }

        return null;
    }
}
