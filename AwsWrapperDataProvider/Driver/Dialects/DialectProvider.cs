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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Driver.Dialects;

public static class DialectProvider
{
    private static readonly Dictionary<(RdsUrlType urlType, Type connectionType), Type> ConnectionToDialectMap = new()
    {
        { (RdsUrlType.IpAddress, typeof(NpgsqlConnection)), typeof(PgDialect) },
        { (RdsUrlType.IpAddress, typeof(MySqlConnection)), typeof(MysqlDialect) },
        { (RdsUrlType.RdsInstance, typeof(NpgsqlConnection)), typeof(PgDialect) },
        { (RdsUrlType.RdsInstance, typeof(MySqlConnection)), typeof(MysqlDialect) },
    };

    public static IDialect GuessDialect(Dictionary<string, string> props)
    {
        // Check for custom dialect in properties
        if (PropertyDefinition.CustomDialect.GetString(props) is { } customDialectTypeName &&
            !string.IsNullOrEmpty(customDialectTypeName))
        {
            // Try to find and instantiate the custom dialect type
            Type? customDialectType = Type.GetType(customDialectTypeName);
            if (customDialectType != null &&
                typeof(IDialect).IsAssignableFrom(customDialectType) &&
                Activator.CreateInstance(customDialectType) is IDialect customDialect)
            {
                return customDialect;
            }

            throw new InvalidOperationException($"Failed to instantiate custom dialect type '{customDialectTypeName}'");
        }

        string url = PropertyDefinition.GetConnectionUrl(props);
        RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(url);
        Type targetConnectionType = Type.GetType(PropertyDefinition.CustomDialect.GetString(props)!) ??
                                    throw new InvalidCastException("Target connection type not found.");

        Type dialectType = ConnectionToDialectMap.GetValueOrDefault((rdsUrlType, targetConnectionType)) ?? typeof(UnknownDialect);
        return (IDialect)Activator.CreateInstance(dialectType)!;
    }

    public static IDialect UpdateDialect(DbConnection connection, IDialect currDialect)
    {
        IList<Type> dialectCandidates = currDialect.DialectUpdateCandidates;

        foreach (Type dialectCandidate in dialectCandidates)
        {
            if (Activator.CreateInstance(dialectCandidate) is IDialect dialect)
            {
                if (dialect.IsDialect(connection))
                {
                    return dialect;
                }
            }
            else
            {
                throw new ArgumentException("Invalid dialect type provided.");
            }
        }

        if (currDialect is UnknownDialect)
        {
            throw new ArgumentException("Unknown dialect type provided.");
        }

        return currDialect;
    }
}
