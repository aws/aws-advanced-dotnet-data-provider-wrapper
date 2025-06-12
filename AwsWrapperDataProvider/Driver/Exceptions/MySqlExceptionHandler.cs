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
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using MySql.Data.MySqlClient;

namespace AwsWrapperDataProvider.Driver.Exceptions;

public class MySqlExceptionHandler : GenericExceptionHandler
{
    // TODO: Check if we need to handle HikariMariaDb exception codes as well.
    private readonly string[] _networkErrorStates =
    {
        "08000", // Connection Exceptio n
        "08001", // SQL client unable to establish SQL connection
        "08004", // SQL server rejected SQL connection
        "08S01", // Communication link failure
    };

    private readonly string[] _loginErrorStates =
    {
        "28000", // Invalid authorization specification
    };

    protected override string[] NetworkErrorStates => this._networkErrorStates;

    protected override string[] LoginErrorStates => this._loginErrorStates;

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is DbException dbException)
            {
                string sqlState = dbException.SqlState ??
                                  string.Empty;
                return this.NetworkErrorStates.Contains(sqlState)
                       || dbException.InnerException is ArgumentException
                       || this.DbExceptionContainsTimeOutException(dbException);
            }

            currException = currException.InnerException;
        }

        return false;
    }
}
