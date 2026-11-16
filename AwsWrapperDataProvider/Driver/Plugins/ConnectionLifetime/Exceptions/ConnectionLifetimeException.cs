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

namespace AwsWrapperDataProvider.Driver.Plugins.ConnectionLifetime.Exceptions;

/// <summary>
/// Thrown when a connection has reached its configured maximum lifetime. Thrown directly, with SQLState
/// <c>08003</c>, when the plugin closed the connection; the subclasses report the outcome of a reconnect.
/// </summary>
public class ConnectionLifetimeException : DbException
{
    public const string ConnectionNotOpenSqlState = "08003";

    public ConnectionLifetimeException(string message, string sqlState)
        : base(message)
    {
        this.SqlState = sqlState;
    }

    public ConnectionLifetimeException(string message, string sqlState, Exception? innerException)
        : base(message, innerException)
    {
        this.SqlState = sqlState;
    }

    public override string? SqlState { get; }
}
