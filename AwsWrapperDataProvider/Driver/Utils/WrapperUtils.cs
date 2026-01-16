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
using System.Text.RegularExpressions;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Driver.Utils;

public partial class WrapperUtils
{
    public static Task<T> ExecuteWithPlugins<T>(
        ConnectionPluginManager connectionPluginManager,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        return connectionPluginManager.Execute(
            methodInvokeOn,
            methodName,
            methodFunc,
            methodArgs);
    }

    public static Task RunWithPlugins(
        ConnectionPluginManager connectionPluginManager,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate methodFunc,
        params object[] methodArgs)
    {
        // Type object does not mean anything since it's void return type
        return ExecuteWithPlugins<object>(
            connectionPluginManager,
            methodInvokeOn,
            methodName,
            async () =>
            {
                await methodFunc();
                return default!;
            },
            methodArgs);
    }

    public static Task<DbConnection> OpenWithPlugins(
        ConnectionPluginManager connectionPluginManager,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool async)
    {
        return connectionPluginManager.Open(hostSpec, props, isInitialConnection, null, async);
    }

    public static Task<DbConnection> ForceOpenWithPlugins(
        ConnectionPluginManager connectionPluginManager,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate openFunc,
        bool async)
    {
        return connectionPluginManager.ForceOpen(hostSpec, props, isInitialConnection, null, async);
    }

    public static string GetQueryFromSqlObject(object sqlObject)
    {
        string query = string.Empty;
        if (sqlObject is DbCommand command)
        {
            query = command.CommandText;
        }

        return query;
    }

    public static (bool ReadOnly, bool Found) DoesSetReadOnly(string query, IDialect dialect)
    {
        if (query == null)
        {
            return (false, false);
        }

        foreach (var statement in ParseMultiStatementQueries(query))
        {
            // Remove block comments and trim
            var cleanStmt = BlockCommentsRegex().Replace(statement, " ").Trim();
            if (cleanStmt.Length == 0)
            {
                continue;
            }

            var (readOnly, found) = dialect.DoesStatementSetReadOnly(cleanStmt);
            if (found)
            {
                return (readOnly, true);
            }
        }

        return (false, false);
    }

    public static List<string> ParseMultiStatementQueries(string query)
    {
        var result = new List<string>();

        if (string.IsNullOrWhiteSpace(query))
        {
            return result;
        }

        // Normalize whitespace: collapse multiple spaces/newlines into a single space
        var normalized = WhitespaceRegex().Replace(query, " ").Trim();
        if (normalized.Length == 0)
        {
            return result;
        }

        foreach (var stmt in normalized.Split(';'))
        {
            if (!string.IsNullOrWhiteSpace(stmt))
            {
                result.Add(stmt);
            }
        }

        return result;
    }

    public static HostSpec? GetWriter(IList<HostSpec> hosts)
    {
        return hosts.FirstOrDefault(host => host.Role == HostRole.Writer);
    }

    public static bool ContainsHostUrl(IList<HostSpec> hosts, string hostAndPort)
    {
        return hosts.Any(host => string.Equals(host.GetHostAndPort(), hostAndPort, StringComparison.OrdinalIgnoreCase));
    }

    [GeneratedRegex(@"\s+")]
    private static partial Regex WhitespaceRegex();

    [GeneratedRegex(@"\s*/\*(.*?)\*/\s*", RegexOptions.Singleline)]
    private static partial Regex BlockCommentsRegex();
}
