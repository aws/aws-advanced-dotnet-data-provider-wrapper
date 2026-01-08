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

public class PgExceptionHandler : GenericExceptionHandler
{
    private static readonly ILogger<PgExceptionHandler> Logger = LoggerUtils.GetLogger<PgExceptionHandler>();

    protected override IReadOnlySet<string> NetworkErrorStates { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "53", // insufficient resources
        "57P01", // admin shutdown
        "57P02", // crash shutdown
        "57P03", // cannot connect now
        "58", // system error (backend)
        "08", // connection error
        "99", // unexpected error
        "F0", // configuration file error (backend)
    };

    protected override IReadOnlySet<string> LoginErrorStates { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "28000", // Invalid authorization specification
        "28P01", // Wrong password
    };
    protected override IReadOnlySet<string> SyntaxErrorStates { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "42", // Syntax error or access violation
        "3F000", // Schema does not exist
    };

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_CurrentException, currException.GetType().FullName, currException.Message);

            if (currException is SocketException or TimeoutException or EndOfStreamException)
            {
                Logger.LogDebug(Resources.MySqlExceptionHandler_IsNetworkException_CurrentNetworkException, currException.GetType().FullName);
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
}
