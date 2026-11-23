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
using System.Globalization;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.ConnectionLifetime.Exceptions;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.ConnectionLifetime;

/// <summary>
/// Limits how long the application's connection keeps using one physical connection. The lifetime is
/// checked only before network-bound calls; there is no background timer. Once a connection has expired
/// and no transaction is active, the plugin either closes it or replaces it with a new connection to the
/// originally configured host, and fails the pending call without executing it.
/// </summary>
public class ConnectionLifetimePlugin : AbstractConnectionPlugin
{
    private const string CloseAction = "close";
    private const string ReconnectAction = "reconnect";

    private static readonly ILogger<ConnectionLifetimePlugin> Logger = LoggerUtils.GetLogger<ConnectionLifetimePlugin>();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly TimeProvider timeProvider;
    private readonly TimeSpan connectionLifetime;
    private readonly bool reconnect;

    // Keyed by reference identity. Weak keys let a connection that was dropped without passing through
    // this plugin be collected instead of being held here.
    private readonly ConditionalWeakTable<DbConnection, StrongBox<long>> openTimestamps = new();

    private DbConnection? timedConnection;
    private long timedConnectionOpenTimestamp;

    // The connection this plugin closed, or the replacement it failed to install. While it is still the
    // current connection every network-bound call fails. An application Open installs a new connection and
    // ends this state.
    private DbConnection? terminalConnection;

    public ConnectionLifetimePlugin(IPluginService pluginService, Dictionary<string, string> props)
        : this(pluginService, props, TimeProvider.System)
    {
    }

    internal ConnectionLifetimePlugin(IPluginService pluginService, Dictionary<string, string> props, TimeProvider timeProvider)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.timeProvider = timeProvider;
        this.connectionLifetime = GetConnectionLifetime(props);
        this.reconnect = IsReconnectAction(props);
    }

    public override IReadOnlySet<string> SubscribedMethods { get; } = PluginMethods.NetworkBoundMethods;

    internal int TrackedConnectionCount => this.openTimestamps.Count();

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        DbConnection connection = await methodFunc().ConfigureAwait(false);
        this.openTimestamps.AddOrUpdate(connection, new StrongBox<long>(this.timeProvider.GetTimestamp()));
        return connection;
    }

    public override async Task<T> Execute<T>(
        object methodInvokedOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        DbConnection? currentConnection = this.pluginService.CurrentConnection;
        if (currentConnection == null)
        {
            return await methodFunc().ConfigureAwait(false);
        }

        if (this.terminalConnection != null)
        {
            if (ReferenceEquals(this.terminalConnection, currentConnection))
            {
                throw NewConnectionClosedException(null);
            }

            this.terminalConnection = null;
        }

        // The application closed the connection itself. Reconnecting here would reopen a connection the
        // application considers closed, so the call goes through and the target driver reports the state.
        if (currentConnection.State == ConnectionState.Closed)
        {
            return await methodFunc().ConfigureAwait(false);
        }

        if (!this.IsExpired(currentConnection))
        {
            return await methodFunc().ConfigureAwait(false);
        }

        // Deferring lets the application commit or roll back on the connection the transaction started on.
        if (this.pluginService.CurrentTransaction != null)
        {
            Logger.LogDebug(Resources.ConnectionLifetimePlugin_ActionDeferred, methodName);
            return await methodFunc().ConfigureAwait(false);
        }

        // An open reader is bound to the current connection: target drivers refuse to move its command to a
        // replacement, so the switch could only fail. The action runs on the next call that starts new work.
        if (PluginMethods.ReaderMethods.Contains(methodName))
        {
            Logger.LogDebug(Resources.ConnectionLifetimePlugin_ActionDeferredForReader, methodName);
            return await methodFunc().ConfigureAwait(false);
        }

        throw this.reconnect
            ? await this.ReconnectAsync(currentConnection).ConfigureAwait(false)
            : await this.CloseAsync(currentConnection).ConfigureAwait(false);
    }

    private static TimeSpan GetConnectionLifetime(Dictionary<string, string> props)
    {
        string? configured = PropertyDefinition.ConnectionLifetimeMs.GetString(props);
        if (long.TryParse(configured?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out long lifetimeMs)
            && lifetimeMs >= 1
            && lifetimeMs <= (long)TimeSpan.MaxValue.TotalMilliseconds)
        {
            return TimeSpan.FromMilliseconds(lifetimeMs);
        }

        throw new ArgumentException(string.Format(Resources.ConnectionLifetimePlugin_InvalidConnectionLifetime, configured));
    }

    private static bool IsReconnectAction(Dictionary<string, string> props)
    {
        string? configured = PropertyDefinition.ConnectionLifetimeAction.GetString(props);
        string? action = configured?.Trim();
        if (string.Equals(action, ReconnectAction, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(action, CloseAction, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        throw new ArgumentException(string.Format(Resources.ConnectionLifetimePlugin_InvalidConnectionLifetimeAction, configured));
    }

    private static ConnectionLifetimeException NewConnectionClosedException(Exception? cause)
    {
        return new ConnectionLifetimeException(
            Resources.ConnectionLifetimePlugin_ConnectionClosed,
            ConnectionLifetimeException.ConnectionNotOpenSqlState,
            cause);
    }

    private bool IsExpired(DbConnection currentConnection)
    {
        if (!ReferenceEquals(currentConnection, this.timedConnection))
        {
            // A connection installed without passing through OpenConnection starts its lifetime when first seen.
            StrongBox<long> openTimestamp = this.openTimestamps.GetValue(
                currentConnection,
                _ => new StrongBox<long>(this.timeProvider.GetTimestamp()));
            this.timedConnection = currentConnection;
            this.timedConnectionOpenTimestamp = openTimestamp.Value;
        }

        return this.timeProvider.GetElapsedTime(this.timedConnectionOpenTimestamp) >= this.connectionLifetime;
    }

    private async Task<ConnectionLifetimeException> CloseAsync(DbConnection currentConnection)
    {
        this.terminalConnection = currentConnection;
        this.openTimestamps.Remove(currentConnection);

        try
        {
            await currentConnection.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Logger.LogWarning(Resources.ConnectionLifetimePlugin_CloseConnectionFailed, exception.Message);
            return NewConnectionClosedException(exception);
        }

        return NewConnectionClosedException(null);
    }

    private async Task<ConnectionLifetimeException> ReconnectAsync(DbConnection previousConnection)
    {
        DbConnection? replacementConnection = null;
        try
        {
            // The configured host, not whatever host routing or failover chose later, so the replacement goes
            // wherever the application's endpoint resolves now.
            HostSpec reconnectHostSpec = this.pluginService.OriginalHostSpec
                ?? throw new InvalidOperationException(Resources.ConnectionLifetimePlugin_OriginalHostUnavailable);

            Logger.LogDebug(Resources.ConnectionLifetimePlugin_Reconnecting, reconnectHostSpec.GetHostAndPort());
            replacementConnection = await this.pluginService.OpenConnection(reconnectHostSpec, this.props, this, true).ConfigureAwait(false);
            long replacementOpenTimestamp = this.timeProvider.GetTimestamp();

            // A routing plugin in the chain may have recorded the instance the endpoint resolved to.
            HostSpec installedHostSpec = this.pluginService.RoutedHostSpec ?? reconnectHostSpec;
            this.pluginService.SetCurrentConnection(replacementConnection, installedHostSpec);

            this.openTimestamps.Remove(previousConnection);
            this.openTimestamps.AddOrUpdate(replacementConnection, new StrongBox<long>(replacementOpenTimestamp));
            this.timedConnection = replacementConnection;
            this.timedConnectionOpenTimestamp = replacementOpenTimestamp;
        }
        catch (Exception exception)
        {
            await this.CleanupFailedReplacementAsync(replacementConnection, previousConnection).ConfigureAwait(false);
            return new ConnectionLifetimeFailedException(exception);
        }
        finally
        {
            this.pluginService.RoutedHostSpec = null;
        }

        return new ConnectionLifetimeSuccessException();
    }

    private async Task CleanupFailedReplacementAsync(DbConnection? replacementConnection, DbConnection previousConnection)
    {
        if (replacementConnection == null || ReferenceEquals(replacementConnection, previousConnection))
        {
            return;
        }

        if (ReferenceEquals(replacementConnection, this.pluginService.CurrentConnection))
        {
            // The switch got far enough to make the replacement current, so neither connection is known to be
            // in a usable state. Close both and refuse further calls until the application opens again.
            this.terminalConnection = replacementConnection;
            await this.CloseQuietlyAsync(replacementConnection, Resources.ConnectionLifetimePlugin_FailedInstallationConnectionCloseFailed).ConfigureAwait(false);
            await this.CloseQuietlyAsync(previousConnection, Resources.ConnectionLifetimePlugin_FailedInstallationConnectionCloseFailed).ConfigureAwait(false);
            return;
        }

        this.openTimestamps.Remove(replacementConnection);
        try
        {
            await replacementConnection.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Logger.LogWarning(Resources.ConnectionLifetimePlugin_ReplacementCloseFailed, exception.Message);
        }
    }

    private async Task CloseQuietlyAsync(DbConnection connection, string failureMessage)
    {
        this.openTimestamps.Remove(connection);
        try
        {
            await connection.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Logger.LogWarning(failureMessage, exception.Message);
        }
    }
}
