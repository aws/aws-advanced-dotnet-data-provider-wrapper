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
using System.Runtime.InteropServices.Marshalling;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using Npgsql;

namespace AwsWrapperDataProvider.Driver.Exceptions;

public class PgExceptionHandler : GenericExceptionHandler
{
    private readonly HashSet<string> _networkErrorStates = new HashSet<string>
    {
        "53", // insufficient resources
        "57P01", // admin shutdown
        "57P02", // crash shutdown
        "57P03", // cannot connect now
        "58", // system error (backend)
        "08", // connection error
        "99", // unexpected error
        "F0", // configuration file error (backend)
        "XX", // internal error (backend)
    };

    private readonly HashSet<string> _loginErrorStates = new HashSet<string>
    {
        "28000", // Invalid authorization specification
        "28P01", // Wrong password
    };

    protected override HashSet<string> NetworkErrorStates => this._networkErrorStates;

    protected override HashSet<string> LoginErrorStates => this._loginErrorStates;

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is SocketException or TimeoutException)
            {
                return true;
            }

            if (currException is DbException dbException)
            {
                string sqlState = dbException.SqlState ??
                                  string.Empty;
                if (this.NetworkErrorStates.Contains(sqlState))
                {
                    return true;
                }
            }

            currException = currException.InnerException;
        }

        return false;
    }
}
