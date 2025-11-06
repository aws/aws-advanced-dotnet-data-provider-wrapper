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
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// Interface for the plugin service that manages connection plugins and host list providers.
/// </summary>
public interface IPluginService : IExceptionHandlerService
{
    IDialect Dialect { get; }

    ITargetConnectionDialect TargetConnectionDialect { get; }

    DbConnection? CurrentConnection { get; }

    DbTransaction? CurrentTransaction { get; set; }

    HostSpec? CurrentHostSpec { get; }

    HostSpec? InitialConnectionHostSpec { get; }

    IList<HostSpec> AllHosts { get; }

    IHostListProvider? HostListProvider { get; }

    HostSpecBuilder HostSpecBuilder { get; }

    /// <summary>
    /// Sets the current connection and associated host specification.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="hostSpec">The host specification.</param>
    void SetCurrentConnection(DbConnection? connection, HostSpec? hostSpec);

    /// <summary>
    /// Gets the currently active hosts.
    /// </summary>
    /// <returns>List of active host specifications.</returns>
    IList<HostSpec> GetHosts();

    /// <summary>
    /// Gets the role of the host for the given connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <returns>The host role.</returns>
    Task<HostRole> GetHostRole(DbConnection? connection);

    /// <summary>
    /// Sets the availability of hosts.
    /// </summary>
    /// <param name="hostAliases">Set of host aliases.</param>
    /// <param name="availability">The availability status.</param>
    void SetAvailability(ICollection<string> hostAliases, HostAvailability availability);

    /// <summary>
    /// Refreshes the host list.
    /// </summary>
    /// <returns>Refresh host list task.</returns>
    Task RefreshHostListAsync();

    /// <summary>
    /// Refreshes the host list using the given connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <returns>Refresh host list task.</returns>
    Task RefreshHostListAsync(DbConnection connection);

    /// <summary>
    /// Forces a refresh of the host list.
    /// </summary>
    /// <returns>Force refresh host list task.</returns>
    Task ForceRefreshHostListAsync();

    /// <summary>
    /// Forces a refresh of the host list using the given connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <returns>Force refresh host list task.</returns>
    Task ForceRefreshHostListAsync(DbConnection connection);

    /// <summary>
    /// Forces a refresh of the host list with verification options.
    /// </summary>
    /// <param name="shouldVerifyWriter">Whether to verify the writer.</param>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>Whether the operation was successful.</returns>
    Task<bool> ForceRefreshHostListAsync(bool shouldVerifyWriter, long timeoutMs);

    /// <summary>
    /// Connects to a host, skipping a specific plugin.
    /// </summary>
    /// <param name="hostSpec">The host specification.</param>
    /// <param name="props">Connection properties.</param>
    /// <param name="pluginToSkip">Plugin to skip.</param>
    /// <param name="async">True if async open, failse otherwise.</param>
    /// <returns>The created database connection.</returns>
    Task<DbConnection> OpenConnection(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin? pluginToSkip, bool async);

    /// <summary>
    /// Forces a connection to a host, bypassing certain plugins like failover to prevent cyclic dependencies.
    /// Used primarily for monitoring and internal connections.
    /// </summary>
    /// <param name="hostSpec">The host specification.</param>
    /// <param name="props">Connection properties.</param>
    /// <param name="pluginToSkip">Plugin to skip.</param>
    /// <param name="async">True if async open, failse otherwise.</param>
    /// <returns>The created database connection.</returns>
    Task<DbConnection> ForceOpenConnection(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin? pluginToSkip, bool async);

    /// <summary>
    /// Updates the dialect based on the given connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <returns>The task.</returns>
    Task UpdateDialectAsync(DbConnection connection);

    /// <summary>
    /// Identifies the host associated with the given connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="transaction">The database transaction.</param>
    /// <returns>The host specification task.</returns>
    Task<HostSpec?> IdentifyConnectionAsync(DbConnection connection, DbTransaction? transaction = null);

    /// <summary>
    /// Fills in aliases for the given host specification using the connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="hostSpec">The host specification.</param>
    /// <param name="transaction">The database transaction.</param>
    /// <returns>The task.</returns>
    Task FillAliasesAsync(DbConnection connection, HostSpec hostSpec, DbTransaction? transaction = null);

    /// <summary>
    /// Gets the connection provider.
    /// </summary>
    /// <returns>The connection provider.</returns>
    IConnectionProvider GetConnectionProvider();

    /// <summary>
    /// Checks if IConnectionPlugin and ConnectionProvider support host seclection strategy.
    /// </summary>
    /// <param name="strategy">The strategy that should be used to pick a host.</param>
    /// <returns>whether strategy is supported.</returns>
    bool AcceptsStrategy(string strategy);

    /// <summary>
    /// Retrieves host given role of host and host selection strategy.
    /// </summary>
    /// <param name="hostRole">The role of host to be selected.</param>
    /// <param name="strategy">Host selection strategy.</param>
    /// <returns>Host givent role and selection strategy.</returns>
    HostSpec GetHostSpecByStrategy(HostRole hostRole, string strategy);

    HostSpec GetHostSpecByStrategy(IList<HostSpec> hosts, HostRole hostRole, string strategy);
}
