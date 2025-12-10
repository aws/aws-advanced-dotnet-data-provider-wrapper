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

using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

public static class TargetConnectionDialectProvider
{
    private static readonly Dictionary<string, ITargetConnectionDialect?> ConnectionDialectsByConnectionType = new()
    {
        { TargetConnectionTypes.Npgsql, null },
        { TargetConnectionTypes.MySqlConnector, null },
        { TargetConnectionTypes.MySqlClient, null },
    };

    public static ITargetConnectionDialect GetDialect(
        Type connectionType,
        Dictionary<string, string>? props,
        ConfigurationProfile? configurationProfile)
    {
        return configurationProfile?.TargetConnectionDialect ?? GetDialect(connectionType, props);
    }

    public static ITargetConnectionDialect GetDialect(Type connectionType, Dictionary<string, string>? props)
    {
        // Check for custom dialect in properties
        if (props != null &&
            PropertyDefinition.CustomTargetConnectionDialect.GetString(props) is { } customDialectTypeName &&
            !string.IsNullOrEmpty(customDialectTypeName))
        {
            // Try to find and instantiate the custom dialect type
            Type? customDialectType = Type.GetType(customDialectTypeName);
            if (customDialectType != null &&
                    typeof(ITargetConnectionDialect).IsAssignableFrom(customDialectType) &&
                    Activator.CreateInstance(customDialectType) is ITargetConnectionDialect customDialect &&
                    customDialect.IsDialect(connectionType))
            {
                return customDialect;
            }

            throw new InvalidOperationException($"Failed to instantiate custom dialect type '{customDialectTypeName}'");
        }

        string connectionTypeName = connectionType.FullName!;
        if (!ConnectionDialectsByConnectionType.ContainsKey(connectionTypeName))
        {
            return new GenericTargetConnectionDialect(connectionType);
        }

        return ConnectionDialectsByConnectionType[connectionType.FullName!] ?? new GenericTargetConnectionDialect(connectionType);
    }

    public static void RegisterDialect(string connectionType, ITargetConnectionDialect dialect)
    {
        ConnectionDialectsByConnectionType[connectionType] = dialect;
    }
}
