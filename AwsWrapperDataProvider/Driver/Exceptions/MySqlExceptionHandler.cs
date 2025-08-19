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
using System.Text;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;
using MySqlConnector;

namespace AwsWrapperDataProvider.Driver.Exceptions;

public class MySqlExceptionHandler : GenericExceptionHandler
{
    private static readonly ILogger<MySqlExceptionHandler> Logger = LoggerUtils.GetLogger<MySqlExceptionHandler>();

    // TODO: Check if we need to handle HikariMariaDb exception codes as well.
    private readonly HashSet<string> networkErrorStates =
    [
        "08000", // Connection Exception
        "08001", // SQL client unable to establish SQL connection
        "08004", // SQL server rejected SQL connection
        "08S01", // Communication link failure
    ];

    private readonly HashSet<string> loginErrorStates =
    [
        "28000", // Invalid authorization specification
    ];

    protected override HashSet<string> NetworkErrorStates => this.networkErrorStates;

    protected override HashSet<string> LoginErrorStates => this.loginErrorStates;

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
        if (exception.SqlState == null && exception is MySqlException mySqlException)
        {
            // invalid username/password
            return mySqlException.Number == 1045;
        }
        else
        {
            return this.ExceptionHasSqlState(exception, this.LoginErrorStates);
        }
    }

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is ArgumentException or TimeoutException or MySqlEndOfStreamException)
            {
                Logger.LogDebug("Current exception is a network exception: {type}", currException.GetType().FullName);
                return true;
            }

            if (currException is DbException dbException)
            {
                string sqlState = dbException.SqlState ?? string.Empty;

                var log = new StringBuilder();
                log.AppendLine("=== DbException Details ===");
                log.AppendLine($"Type: {dbException.GetType().FullName}");
                log.AppendLine($"Message: {dbException.Message}");
                log.AppendLine($"Sql State: {dbException.SqlState}");
                log.AppendLine($"Error Code: {dbException.ErrorCode}");
                log.AppendLine($"Source: {dbException.Source}");
                Logger.LogDebug(log.ToString());

                if (this.NetworkErrorStates.Contains(sqlState))
                {
                    Logger.LogDebug("Current exception is a network exception");
                    return true;
                }
            }

            currException = currException.InnerException;
            Logger.LogDebug("Checking innner exception");
        }

        Logger.LogDebug("Current exception is not a network exception");
        return false;
    }
}
