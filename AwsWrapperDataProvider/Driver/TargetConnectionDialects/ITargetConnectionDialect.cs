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

using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

/// <summary>
/// Interface for target driver dialects that define driver-specific behavior.
/// </summary>
public interface ITargetConnectionDialect
{
    /// <summary>
    /// Gets DbConnection type associated with this dialect.
    /// </summary>
    Type DriverConnectionType { get; }

    /// <summary>
    /// Determines if the given connection type matches this dialect.
    /// </summary>
    /// <param name="connectionType">The connection type.</param>
    /// <returns>True if the connection type matches this dialect, false otherwise.</returns>
    bool IsDialect(Type connectionType);

    /// <summary>
    /// Prepares a connection string for the given host specification and properties.
    /// </summary>
    /// <param name="dialect">The dialect of connection.</param>
    /// <param name="hostSpec">The host specification.</param>
    /// <param name="props">Connection properties.</param>
    /// <param name="isForcedOpen">Is connection string for a forced open connection.</param>
    /// <returns>The prepared connection string.</returns>
    string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, bool isForcedOpen = false);

    /// <summary>
    /// Gets the set of method names that are allowed to be called on the connection.
    /// </summary>
    /// <returns>Set of allowed method names.</returns>
    ISet<string> GetAllowedOnConnectionMethodNames();

    /// <summary>
    /// Checks if the connection is alive.
    /// </summary>
    /// <param name="connection">Connection to ping.</param>
    /// <returns>Tuple of bool that is True if connection alive, and Exception if ping throws an exception.</returns>
    (bool ConnectionAlive, Exception? ConnectionException) Ping(IDbConnection connection);

    /// <summary>
    /// Prepares the plugin codes from the given props if specified.
    /// Returns default plugin codes that are compatible with the dialect otherwise.
    /// </summary>
    /// <param name="props">Connection properties.</param>
    /// <returns>A string of plugin codes.</returns>
    string GetPluginCodesOrDefault(Dictionary<string, string> props);

    DbConnectionStringBuilder CreateConnectionStringBuilder();

    string? MapCanonicalKeyToWrapperProperty(string canonicalKey);

    bool IsSyntaxError(DbException ex);
}
