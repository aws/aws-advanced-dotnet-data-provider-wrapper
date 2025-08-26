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

namespace AwsWrapperDataProvider.Driver.Exceptions;

public class PgExceptionHandler : GenericExceptionHandler
{
    private static readonly ILogger<PgExceptionHandler> Logger = LoggerUtils.GetLogger<PgExceptionHandler>();

    private readonly HashSet<string> networkErrorStates =
    [
        "53", // insufficient resources
        "57P01", // admin shutdown
        "57P02", // crash shutdown
        "57P03", // cannot connect now
        "58", // system error (backend)
        "08", // connection error
        "99", // unexpected error
        "F0", // configuration file error (backend)
        "XX", // internal error (backend)
    ];

    private readonly HashSet<string> loginErrorStates =
    [
        "28000", // Invalid authorization specification
        "28P01", // Wrong password
    ];

    protected override HashSet<string> NetworkErrorStates => this.networkErrorStates;

    protected override HashSet<string> LoginErrorStates => this.loginErrorStates;

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            Logger.LogDebug("Current exception type: {type}", currException.GetType().FullName);
            Logger.LogDebug("Current exception message: {message}", currException.Message);

            if (currException is SocketException or TimeoutException or EndOfStreamException)
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

                if (this.NetworkErrorStates.Any(prefix => sqlState.StartsWith(prefix)))
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
}
