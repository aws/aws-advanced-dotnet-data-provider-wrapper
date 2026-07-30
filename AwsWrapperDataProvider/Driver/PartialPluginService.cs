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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// An <see cref="IPluginService"/> for shared background monitors. Monitors are cached
/// process-wide and outlive the connection that created them, so they must not hold a user
/// connection's <see cref="PluginService"/>: doing so pins that connection's plugin chain and
/// state for the monitor's whole lifetime. This service owns its own plugin chain instead and
/// supports only the members monitors need; members tied to an owning connection throw
/// <see cref="NotSupportedException"/>.
/// </summary>
internal class PartialPluginService : IPluginService, IHostListProviderService
{
    private static readonly ILogger<PartialPluginService> Logger = LoggerUtils.GetLogger<PartialPluginService>();

    private readonly FullServicesContainer servicesContainer;
    private readonly ConnectionPluginManager pluginManager;
    private readonly Dictionary<string, string> props;
    // Assigned by InitHostListProvider immediately after construction (see ServiceUtility).
    private volatile IHostListProvider hostListProvider = null!;
    private HostSpec? initialConnectionHostSpec;

    public PartialPluginService(
        FullServicesContainer servicesContainer,
        Dictionary<string, string> props,
        IDialect dialect,
        ITargetConnectionDialect targetConnectionDialect)
    {
        this.servicesContainer = servicesContainer;
        this.pluginManager = servicesContainer.ConnectionPluginManager;
        this.props = props;
        this.Dialect = dialect;
        this.TargetConnectionDialect = targetConnectionDialect;
        this.TelemetryFactory = servicesContainer.TelemetryFactory;
    }

    /// <summary>
    /// Builds this service's host list provider. Separate from the constructor because the
    /// supplier reads the plugin and host list provider services back off the container, so the
    /// caller must register this service into the container's slots first.
    /// </summary>
    internal void InitHostListProvider()
    {
        this.hostListProvider =
            this.Dialect.HostListProviderSupplier(this.props, this.servicesContainer)
            ?? throw new InvalidOperationException();
    }

    // Monitors receive the confirmed dialect as a snapshot at creation time; it never changes.
    public IDialect Dialect { get; }

    public bool IsDialectConfirmed => true;

    public ITargetConnectionDialect TargetConnectionDialect { get; }

    public ITelemetryFactory TelemetryFactory { get; }

    public HostSpecBuilder HostSpecBuilder => new HostSpecBuilder();

    public IList<HostSpec> AllHosts { get; private set; } = [];

    public IHostListProvider? HostListProvider
    {
        get => this.hostListProvider;
        set => this.hostListProvider = value ?? throw new ArgumentNullException(nameof(value));
    }

    public HostSpec? InitialConnectionHostSpec
    {
        get => this.initialConnectionHostSpec;
        set => this.initialConnectionHostSpec = value;
    }

    public HostSpec? OriginalHostSpec => this.initialConnectionHostSpec;

    public HostSpec? CurrentHostSpec => this.initialConnectionHostSpec;

    // There is no user connection behind a monitor-scoped service; monitors manage their own
    // DbConnection lifecycles, so this is genuinely "no current connection" rather than an error.
    public DbConnection? CurrentConnection => null;

    public DbTransaction? CurrentTransaction
    {
        get => null;
        set => throw MethodNotSupported(nameof(this.CurrentTransaction));
    }

    public HostSpec? RoutedHostSpec
    {
        get => null;
        set => throw MethodNotSupported(nameof(this.RoutedHostSpec));
    }

    internal FullServicesContainer ServicesContainer => this.servicesContainer;

    public bool IsStaticHostListProvider()
    {
        return this.HostListProvider is IStaticHostListProvider;
    }

    public void SetCurrentConnection(DbConnection? connection, HostSpec? hostSpec)
    {
        throw MethodNotSupported(nameof(this.SetCurrentConnection));
    }

    public IList<HostSpec> GetHosts()
    {
        // Mirror PluginService.GetHosts: filter AllHosts through the AllowedAndBlockedHosts
        // restrictions plugins like CustomEndpoint publish, keyed by the initial host.
        if (this.initialConnectionHostSpec == null)
        {
            return this.AllHosts;
        }

        var hostPermissions = PluginService.AllowedAndBlockedHostsCache.Get<AllowedAndBlockedHosts>(this.initialConnectionHostSpec.Host);
        if (hostPermissions == null)
        {
            return this.AllHosts;
        }

        IList<HostSpec> filteredHosts = this.AllHosts;
        var allowedHostIds = hostPermissions.AllowedHostIds;
        var blockedHostIds = hostPermissions.BlockedHostIds;
        var requiredRole = hostPermissions.RequiredRole;

        if (allowedHostIds != null && allowedHostIds.Count > 0)
        {
            filteredHosts = filteredHosts
                .Where(host => !string.IsNullOrEmpty(host.HostId) && allowedHostIds.Contains(host.HostId))
                .Where(host => requiredRole == null || host.Role == requiredRole)
                .ToList();
        }

        if (blockedHostIds != null && blockedHostIds.Count > 0)
        {
            filteredHosts = filteredHosts
                .Where(host => string.IsNullOrEmpty(host.HostId) || !blockedHostIds.Contains(host.HostId))
                .Where(host => requiredRole == null || host.Role == requiredRole)
                .ToList();
        }

        return filteredHosts;
    }

    public async Task<HostRole> GetHostRole(DbConnection? connection)
    {
        return await this.Dialect.GetHostRoleAsync(connection!);
    }

    public void SetAvailability(HostSpec hostSpec, HostAvailability availability)
    {
        // Same availability cache the full PluginService writes, so user connections observe
        // availability changes detected by monitor probes. Node-change notification is a
        // per-connection concern and is intentionally skipped here.
        foreach (HostSpec host in this.AllHosts
            .Where(host => (hostSpec.HostId != null && hostSpec.HostId == host.HostId)
                        || (hostSpec.Host != null && string.Equals(hostSpec.Host, host.Host, StringComparison.OrdinalIgnoreCase))))
        {
            host.Availability = availability;
        }

        PluginService.HostAvailabilityExpiringCache.Set(
            hostSpec.GetHostAndPort(), availability, PluginService.DefaultHostAvailabilityCacheExpiration);
    }

    public void SetAllowedAndBlockedHosts(string connectionUrl, AllowedAndBlockedHosts allowedAndBlockedHosts)
    {
        PluginService.AllowedAndBlockedHostsCache.Set(
            connectionUrl, allowedAndBlockedHosts, PluginService.DefaultHostAvailabilityCacheExpiration);
    }

    public async Task RefreshHostListAsync()
    {
        IList<HostSpec> updateHostList = await this.hostListProvider.RefreshAsync();
        UpdateHostAvailability(updateHostList);
        this.AllHosts = updateHostList;
    }

    public async Task ForceRefreshHostListAsync()
    {
        await this.ForceRefreshHostListAsync(false, 5000);
    }

    public async Task<bool> ForceRefreshHostListAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        try
        {
            IList<HostSpec> updateHostList = await this.hostListProvider.ForceRefreshAsync(shouldVerifyWriter, timeoutMs);
            UpdateHostAvailability(updateHostList);
            this.AllHosts = updateHostList;
            return true;
        }
        catch (TimeoutException)
        {
            Logger.LogDebug(Resources.PluginService_ForceRefreshHostListAsync_TimeoutException, timeoutMs);
            return false;
        }
    }

    // Restore each refreshed host's availability from the shared cache, matching PluginService.
    private static void UpdateHostAvailability(IList<HostSpec> hosts)
    {
        foreach (HostSpec host in hosts)
        {
            if (PluginService.HostAvailabilityExpiringCache.TryGetValue(host.GetHostAndPort(), out HostAvailability? availability) && availability.HasValue)
            {
                host.Availability = availability.Value;
            }
        }
    }

    // Monitors open probe connections only via ForceOpenConnection; the normal open path is unused.
    public Task<DbConnection> OpenConnection(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin? pluginToSkip, bool async)
    {
        throw MethodNotSupported(nameof(this.OpenConnection));
    }

    // isInitialConnection is pinned false: the dialect is already confirmed and must not be re-guessed.
    public Task<DbConnection> ForceOpenConnection(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin? pluginToSkip, bool async)
    {
        return this.pluginManager.ForceOpen(hostSpec, props, false, pluginToSkip, async);
    }

    public Task UpdateDialectAsync(DbConnection connection)
    {
        throw MethodNotSupported(nameof(this.UpdateDialectAsync));
    }

    public Task<HostSpec?> IdentifyConnectionAsync(DbConnection connection, DbTransaction? transaction = null)
    {
        return this.hostListProvider.IdentifyConnectionAsync(connection, transaction);
    }

    public Task<HostSpec?> IdentifyConnectionAsync(DbConnection connection, HostSpec connectionHostSpec, DbTransaction? transaction = null)
    {
        return this.servicesContainer.HostIdCacheService.IdentifyConnectionAsync(connection, connectionHostSpec, this, transaction);
    }

    public IConnectionProvider GetConnectionProvider()
    {
        return this.servicesContainer.DefaultConnectionProvider;
    }

    public bool AcceptsStrategy(string strategy)
    {
        throw MethodNotSupported(nameof(this.AcceptsStrategy));
    }

    public HostSpec GetHostSpecByStrategy(HostRole hostRole, string strategy)
    {
        throw MethodNotSupported(nameof(this.GetHostSpecByStrategy));
    }

    public HostSpec GetHostSpecByStrategy(IList<HostSpec> hosts, HostRole hostRole, string strategy)
    {
        throw MethodNotSupported(nameof(this.GetHostSpecByStrategy));
    }

    public bool IsPluginInUse(string pluginCode)
    {
        return this.pluginManager.IsPluginActive(pluginCode);
    }

    public bool IsLoginException(Exception exception)
    {
        return this.Dialect.ExceptionHandler.IsLoginException(exception);
    }

    public bool IsLoginException(string sqlState)
    {
        return this.Dialect.ExceptionHandler.IsLoginException(sqlState);
    }

    public bool IsNetworkException(Exception exception)
    {
        return this.Dialect.ExceptionHandler.IsNetworkException(exception);
    }

    public bool IsNetworkException(string sqlState)
    {
        return this.Dialect.ExceptionHandler.IsNetworkException(sqlState);
    }

    public bool IsReadOnlyConnectionException(Exception exception)
    {
        return this.Dialect.ExceptionHandler.IsReadOnlyConnectionException(exception);
    }

    private static NotSupportedException MethodNotSupported(string memberName)
    {
        return new NotSupportedException(
            string.Format(Resources.Error_PartialPluginServiceMethodNotSupported, memberName));
    }
}
