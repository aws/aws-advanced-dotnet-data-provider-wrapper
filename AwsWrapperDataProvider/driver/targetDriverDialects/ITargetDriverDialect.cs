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
using AwsWrapperDataProvider.driver.hostInfo;

namespace AwsWrapperDataProvider.driver.targetDriverDialects;

/// <summary>
/// Interface for target driver dialects that define driver-specific behavior.
/// </summary>
public interface ITargetDriverDialect
{
    /// <summary>
    /// Determines if the given connection type matches this dialect.
    /// </summary>
    /// <param name="connectionType">The connection type</param>
    /// <returns>True if the connection type matches this dialect, false otherwise</returns>
    bool IsDialect(Type connectionType);

    /// <summary>
    /// Prepares a connection string for the given host specification and properties.
    /// </summary>
    /// <param name="hostSpec">The host specification</param>
    /// <param name="props">Connection properties</param>
    /// <returns>The prepared connection string</returns>
    string PrepareConnectionString(HostSpec hostSpec, Dictionary<string, string> props);

    /// <summary>
    /// Prepares a data source with the given connection, host specification, and properties.
    /// </summary>
    /// <param name="connection">The database connection</param>
    /// <param name="hostSpec">The host specification</param>
    /// <param name="props">Connection properties</param>
    void PrepareDataSource(DbConnection connection, HostSpec hostSpec, Dictionary<string, string> props);

    /// <summary>
    /// Pings the database to check if the connection is still valid.
    /// </summary>
    /// <param name="connection">The database connection</param>
    /// <returns>True if the connection is valid, false otherwise</returns>
    bool Ping(DbConnection connection);

    /// <summary>
    /// Gets the set of method names that are allowed to be called on the connection.
    /// </summary>
    /// <returns>Set of allowed method names</returns>
    ISet<string> GetAllowedOnConnectionMethodNames();

    /// <summary>
    /// Gets the SQL state from an exception.
    /// </summary>
    /// <param name="exception">The exception</param>
    /// <returns>The SQL state code, or null if not available</returns>
    string? GetSqlState(Exception exception);
}