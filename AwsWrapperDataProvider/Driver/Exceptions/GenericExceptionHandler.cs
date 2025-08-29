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
using Microsoft.Extensions.Logging;
using MySqlConnector;

namespace AwsWrapperDataProvider.Driver.Exceptions;

/// <summary>
/// Default implementation of IExceptionHandler that provides basic exception handling
/// when a specific database handler is not available.
/// </summary>
public class GenericExceptionHandler : IExceptionHandler
{
    private static readonly ILogger<GenericExceptionHandler> Logger = LoggerUtils.GetLogger<GenericExceptionHandler>();

    // Common SQL states for network-related issues across different databases
    protected virtual HashSet<string> NetworkErrorStates => new(StringComparer.OrdinalIgnoreCase);

    // Common SQL states for authentication-related issues across different databases
    protected virtual HashSet<string> LoginErrorStates => new(StringComparer.OrdinalIgnoreCase);

    public virtual bool IsNetworkException(string sqlState) => this.NetworkErrorStates.Contains(sqlState);

    public virtual bool IsNetworkException(Exception exception, params Type[] types)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            Logger.LogDebug("Current exception {type}: {message}", currException.GetType().FullName, currException.Message);

            if (currException is SocketException or TimeoutException or MySqlEndOfStreamException)
            {
                Logger.LogDebug("Current exception is a network exception: {type}", currException.GetType().FullName);
                return true;
            }

            if (currException is DbException dbException)
            {
                string sqlState = dbException.SqlState ?? string.Empty;

                StringBuilder log = new();
                log.AppendLine("=== DbException Details ===");
                log.AppendLine($"Type: {dbException.GetType().FullName}");
                log.AppendLine($"Message: {dbException.Message}");
                log.AppendLine($"Sql State: {dbException.SqlState}");
                log.AppendLine($"Error Code: {dbException.ErrorCode}");
                log.AppendLine($"Source: {dbException.Source}");
                Logger.LogDebug(log.ToString());

                if (this.NetworkErrorStates.Contains(sqlState))
                {
                    Logger.LogDebug("Current exception is a network exception: {type}", currException.GetType().FullName);
                    return true;
                }
            }

            currException = currException.InnerException;
            Logger.LogDebug("Checking innner exception");
        }

        Logger.LogDebug("Current exception is not a network exception");
        return false;
    }

    public virtual bool IsLoginException(string sqlState) => this.LoginErrorStates.Contains(sqlState);

    public virtual bool IsLoginException(Exception exception) => this.ExceptionHasSqlState(exception, this.LoginErrorStates);

    protected bool ExceptionHasSqlState(Exception exception, HashSet<string> sqlStates)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is DbException dbException)
            {
                string sqlState = dbException.SqlState ??
                                  string.Empty;
                return sqlStates.Contains(sqlState);
            }

            currException = currException.InnerException;
        }

        return false;
    }
}
