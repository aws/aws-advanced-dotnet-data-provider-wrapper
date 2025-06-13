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
    private readonly HashSet<string> _networkErrorStates = new HashSet<string>
    {
        "08000", // Connection Exception
        "08001", // SQL client unable to establish SQL connection
        "08004", // SQL server rejected SQL connection
        "08S01", // Communication link failure
    };

    private readonly HashSet<string> _loginErrorStates = new HashSet<string>
    {
        "28000", // Invalid authorization specification
    };

    protected override HashSet<string> NetworkErrorStates => this._networkErrorStates;

    protected override HashSet<string> LoginErrorStates => this._loginErrorStates;

    public override bool IsNetworkException(Exception exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is DbException dbException)
            {
                return this.IsNetworkException(dbException);
            }

            currException = currException.InnerException;
        }

        return false;
    }

    private bool IsNetworkException(DbException exception)
    {
        Exception? currException = exception;

        while (currException is not null)
        {
            if (currException is ArgumentException or TimeoutException)
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
