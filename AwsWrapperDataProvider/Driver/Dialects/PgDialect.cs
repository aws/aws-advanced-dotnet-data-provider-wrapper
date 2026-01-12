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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class PgDialect : IDialect
{
    private static readonly ILogger<PgDialect> Logger = LoggerUtils.GetLogger<PgDialect>();

    public static readonly string DefaultPluginCodes = "initialConnection,efm,failover";

    public int DefaultPort { get; } = 5432;

    public string HostAliasQuery { get; } = "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())";

    public string ServerVersionQuery { get; } = "SELECT 'version', pg_catalog.VERSION()";

    internal static readonly string PGSelect1Query = "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1";

    public IExceptionHandler ExceptionHandler { get; } = new PgExceptionHandler();

    public virtual IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(AuroraPgDialect),
        typeof(RdsMultiAzDbClusterPgDialect),
        typeof(RdsPgDialect),
    ];

    public virtual HostListProviderSupplier HostListProviderSupplier { get; } = (
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        IPluginService pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public virtual async Task<bool> IsDialect(DbConnection conn)
    {
        try
        {
            if (conn.State != ConnectionState.Open)
            {
                Logger.LogWarning(Resources.Error_ConnectionNotOpenWhenChecking, nameof(PgDialect));
                return false;
            }

            await using var command = conn.CreateCommand();
            command.CommandText = PGSelect1Query;
            await using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                return true;
            }
        }
        catch (Exception ex) when (this.ExceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, Resources.Error_CantCheckDialect, nameof(PgDialect));
        }

        return false;
    }

    public virtual void PrepareConnectionProperties(Dictionary<string, string> connectionpProps, HostSpec hostSpec)
    {
        // Do nothing.
    }

    public (bool ReadOnly, bool Found) DoesStatementSetReadOnly(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return (false, false);
        }

        var lowercaseQuery = query.Trim().ToLowerInvariant();

        if (lowercaseQuery.StartsWith("set session characteristics as transaction read only"))
        {
            return (true, true);
        }

        if (lowercaseQuery.StartsWith("set session characteristics as transaction read write"))
        {
            return (false, true);
        }

        return (false, false);
    }
}
