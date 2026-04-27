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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public static class DialectUtils
{
    public static async Task<bool> CheckExistenceQueries(DbConnection connection, IExceptionHandler exceptionHandler, ILogger logger, string query)
    {
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = query;
            await using var reader = await command.ExecuteReaderAsync();
            return await reader.ReadAsync();
        }
        catch (Exception ex) when (exceptionHandler.IsSyntaxError(ex))
        {
            // Syntax error - expected when querying against incorrect dialect
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, Resources.Error_CantCheckDialect, nameof(AuroraMySqlDialect));
        }

        return false;
    }

    public static bool IsBlueGreenConnectionDialect(IDialect dialect)
    {
        return dialect is IBlueGreenDialect;
    }
}
