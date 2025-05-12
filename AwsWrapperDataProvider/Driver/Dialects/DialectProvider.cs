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
using AwsWrapperDataProvider.Driver.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Driver.Dialects;

public static class DialectProvider
{
    private static readonly Dictionary<Type, IDialect> _dialectCache = new();

    // TODO: Decide on supported DbConnection types and fully map RdsUrlTypes to IDialect.
    private static readonly Dictionary<(RdsUrlType UrlType, Type ConnectionType), Type> ConnectionToDialectMap = new()
    {
        { (RdsUrlType.IpAddress, typeof(NpgsqlConnection)), typeof(PgDialect) },
        { (RdsUrlType.RdsInstance, typeof(NpgsqlConnection)), typeof(PgDialect) },
        { (RdsUrlType.RdsWriterCluster, typeof(NpgsqlConnection)), typeof(PgDialect) },
        { (RdsUrlType.RdsReaderCluster, typeof(NpgsqlConnection)), typeof(PgDialect) },
        { (RdsUrlType.IpAddress, typeof(MySqlConnection)), typeof(MysqlDialect) },
        { (RdsUrlType.RdsInstance, typeof(MySqlConnection)), typeof(MysqlDialect) },
        { (RdsUrlType.RdsWriterCluster, typeof(MySqlConnection)), typeof(MysqlDialect) },
        { (RdsUrlType.RdsReaderCluster, typeof(MySqlConnection)), typeof(MysqlDialect) },
    };

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
        Type dialectType = ConnectionToDialectMap.GetValueOrDefault((rdsUrlType, targetConnectionType)) ?? typeof(UnknownDialect);
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

        if (currDialect is UnknownDialect)
        {
            throw new ArgumentException("Unknown dialect type provided.");
        }

        return currDialect;
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
