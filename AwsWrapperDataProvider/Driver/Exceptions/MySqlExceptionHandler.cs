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
using System.Net.Sockets;
using System.Text;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Exceptions;

public class MySqlExceptionHandler : GenericExceptionHandler
{
    private static readonly ILogger<MySqlExceptionHandler> Logger = LoggerUtils.GetLogger<MySqlExceptionHandler>();

    private static bool IsMySqlException(Exception exception)
    {
        return exception.GetType().Name == "MySqlException";
    }

    private static int GetMySqlExceptionNumber(Exception exception)
    {
        var numberProperty = exception.GetType().GetProperty("Number");
        return numberProperty?.GetValue(exception) as int? ?? 0;
    }

    private static bool IsMySqlEndOfStreamException(Exception exception)
    {
        return exception.GetType().Name == "MySqlEndOfStreamException";
    }

    // TODO: Check if we need to handle HikariMariaDb exception codes as well.
    protected override IReadOnlySet<string> NetworkErrorStates { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "08000", // Connection Exception
        "08001", // SQL client unable to establish SQL connection
        "08004", // SQL server rejected SQL connection
        "08S01", // Communication link failure
    };

    protected override IReadOnlySet<string> LoginErrorStates { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "28000", // Invalid authorization specification
    };

    protected override IReadOnlySet<string> SyntaxErrorStates { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "42000", // Syntax error or access violation
        "3F000", // Schema does not exist
    };

    private static IReadOnlySet<int> SyntaxErrorNumbers { get; } = new HashSet<int>()
    {
        1064, // ER_PARSE_ERROR
        1049, // ER_BAD_DB_ERROR
    };

    public override bool IsLoginException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is DbException dbException)
            {
                return this.IsLoginException(dbException);
            }

            currException = currException.InnerException;
        }

        return false;
    }

    private bool IsLoginException(DbException exception)
    {
        if (exception.SqlState == null && IsMySqlException(exception))
        {
            // ER_ACCESS_DENIED_ERROR
            return GetMySqlExceptionNumber(exception) == 1045;
        }
        else
        {
            return this.ExceptionHasSqlState(exception, this.LoginErrorStates);
        }
    }

    private bool IsMySqlCommandTimeoutException(DbException exception)
    {
        if (IsMySqlException(exception))
        {
            // MySQL command timeout error code
            return GetMySqlExceptionNumber(exception) == -1;
        }

        return false;
    }

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException != null)
        {
            Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_CurrentException, currException.GetType().FullName, currException.Message);
            if (currException is SocketException or TimeoutException or EndOfStreamException || IsMySqlEndOfStreamException(currException))
            {
                Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_CurrentNetworkException, currException.GetType().FullName);
                return true;
            }

            if (currException is DbException dbException)
            {
                string sqlState = dbException.SqlState ?? string.Empty;

                StringBuilder log = new();
                log.AppendLine("=== DbException Details ===");
                log.AppendLine($"Type: {dbException.GetType().FullName}");
                log.AppendLine($"Message: {dbException.Message}");
                log.AppendLine($"Error Code: {dbException.ErrorCode}");
                log.AppendLine($"Sql State: {dbException.SqlState}");
                log.AppendLine($"Source: {dbException.Source}");
                Logger.LogDebug(log.ToString());

                if (this.NetworkErrorStates.Contains(sqlState) || this.IsMySqlCommandTimeoutException(dbException))
                {
                    Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_CurrentNetworkException, currException.GetType().FullName);
                    return true;
                }
            }

            currException = currException.InnerException;
            Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_InnerException);
        }

        Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_InvalidNetworkException);
        return false;
    }

    public override bool IsSyntaxError(Exception exception)
    {
        if (base.IsSyntaxError(exception))
        {
            return true;
        }

        return IsMySqlException(exception) && SyntaxErrorNumbers.Contains(GetMySqlExceptionNumber(exception));
    }
}
