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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class RdsPgDialect : PgDialect
{
    internal const string ExtensionsSql = "SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
                                         + "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
                                         + "FROM pg_catalog.pg_settings "
                                         + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'";

    private static readonly ILogger<RdsPgDialect> Logger = LoggerUtils.GetLogger<RdsPgDialect>();

    public override IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(RdsMultiAzDbClusterPgDialect),
        typeof(AuroraPgDialect),
    ];

    public override async Task<bool> IsDialect(DbConnection conn)
    {
        if (!(await base.IsDialect(conn)))
        {
            return false;
        }

        try
        {
            await using var command = conn.CreateCommand();
            command.CommandText = ExtensionsSql;
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bool rdsTools = reader.GetBoolean(reader.GetOrdinal("rds_tools"));
                bool auroraUtils = reader.GetBoolean(reader.GetOrdinal("aurora_stat_utils"));
                if (rdsTools && !auroraUtils)
                {
                    return true;
                }
            }
        }
        catch (Exception ex) when (this.ExceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, Resources.Error_CantCheckDialect, nameof(RdsPgDialect));
        }

        return false;
    }
}
