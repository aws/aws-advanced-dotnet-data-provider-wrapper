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

public class RdsMySqlDialect : MySqlDialect
{
    private static readonly ILogger<RdsMySqlDialect> Logger = LoggerUtils.GetLogger<RdsMySqlDialect>();

    public override IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(AuroraMySqlDialect),
        typeof(RdsMultiAzDbClusterMySqlDialect),
    ];

    public override async Task<bool> IsDialect(DbConnection conn)
    {
        if (await base.IsDialect(conn))
        {
            return false;
        }

        try
        {
            await using var command = conn.CreateCommand();
            command.CommandText = this.ServerVersionQuery;
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                int columnCount = reader.FieldCount;
                for (int i = 0; i < columnCount; i++)
                {
                    string columnValue = reader.GetString(i);
                    if (string.Equals(columnValue, "Source distribution", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, Resources.Error_CantCheckDialect, nameof(RdsMySqlDialect));
        }

        return false;
    }
}
