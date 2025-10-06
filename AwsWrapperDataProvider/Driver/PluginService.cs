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
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver;

public class PluginService : IPluginService, IHostListProviderService
{
    private static readonly TimeSpan DefaultHostAvailabilityCacheExpiration = TimeSpan.FromMinutes(5);
    internal static readonly MemoryCache HostAvailabilityExpiringCache = new(new MemoryCacheOptions());
    private static readonly ILogger<PluginService> Logger = LoggerUtils.GetLogger<PluginService>();

    private readonly object connectionSwitchLock = new();

    private readonly ConnectionPluginManager pluginManager;
    private readonly Dictionary<string, string> props;
    private readonly DialectProvider dialectProvider;
    private volatile IHostListProvider hostListProvider;
    private HostSpec? currentHostSpec;

    private DbTransaction? transaction;

    // private ExceptionManager _exceptionManager;
    // private IExceptionHandler _exceptionHandler;

    public IDialect Dialect { get; private set; }
    public ITargetConnectionDialect TargetConnectionDialect { get; }
    public HostSpec? InitialConnectionHostSpec { get; set; }
    public HostSpec? CurrentHostSpec { get => this.currentHostSpec ?? this.GetCurrentHostSpec(); set => this.currentHostSpec = value; }
    public IList<HostSpec> AllHosts { get; private set; } = [];
    public IHostListProvider? HostListProvider { get => this.hostListProvider; set => this.hostListProvider = value ?? throw new ArgumentNullException(nameof(value)); }
    public HostSpecBuilder HostSpecBuilder { get => new HostSpecBuilder(); }
    public DbConnection? CurrentConnection { get; private set; }

    public DbTransaction? CurrentTransaction
    {
        get => this.transaction;
        set
        {
            try
            {
                this.transaction?.Rollback();
            }
            catch
            {
                // ignore
            }
            finally
            {
                this.transaction?.Dispose();
                this.transaction = value;
            }
        }
    }

    public PluginService(
        Type connectionType,
        ConnectionPluginManager pluginManager,
        Dictionary<string, string> props,
        string connectionString,
        ITargetConnectionDialect? targetConnectionDialect,
        ConfigurationProfile? configurationProfile)
    {
        this.pluginManager = pluginManager;
        this.props = props;
        this.TargetConnectionDialect = configurationProfile?.TargetConnectionDialect ?? targetConnectionDialect ?? throw new ArgumentNullException(nameof(targetConnectionDialect));
        this.dialectProvider = new(this, this.props);
        this.Dialect = configurationProfile?.Dialect ?? this.dialectProvider.GuessDialect();

        this.hostListProvider =
            this.Dialect.HostListProviderSupplier(this.props, this, this)
            ?? throw new InvalidOperationException(); // TODO : throw proper error
    }

    // for testing purpose only
#pragma warning disable CS8618
    internal PluginService() { }
#pragma warning restore CS8618

    public static void ClearCache()
    {
        HostAvailabilityExpiringCache.Clear();
    }

    public bool IsStaticHostListProvider()
    {
        return this.HostListProvider is IStaticHostListProvider;
    }

    public HostSpec GetInitialConnectionHostSpec()
    {
        // TODO implement stub method.
        throw new NotImplementedException();
    }

    public void SetCurrentConnection(DbConnection connection, HostSpec? hostSpec)
    {
        lock (this.connectionSwitchLock)
        {
            DbConnection? oldConnection = this.CurrentConnection;

            this.CurrentConnection = connection;
            this.currentHostSpec = hostSpec;
            Logger.LogTrace("New connection {Type}@{Id} is set.", connection?.GetType().FullName, RuntimeHelpers.GetHashCode(connection));

            try
            {
                if (!ReferenceEquals(connection, oldConnection))
                {
                    oldConnection?.Dispose();
                    Logger.LogTrace("Old connection {Type}@{Id} is disposed.", oldConnection?.GetType().FullName, RuntimeHelpers.GetHashCode(oldConnection));
                }
            }
            catch (DbException exception)
            {
                Logger.LogTrace(string.Format(Resources.PluginService_ErrorClosingOldConnection, exception.Message));
            }
        }
    }

    public IList<HostSpec> GetHosts()
    {
        // TODO: Handle AllowedAndBlockHosts
        return this.AllHosts;
    }

    public HostRole GetHostRole(DbConnection? connection)
    {
        return this.hostListProvider.GetHostRole(connection!);
    }

    public void SetAvailability(ICollection<string> hostAliases, HostAvailability availability)
    {
        if (hostAliases.Count == 0)
        {
            return;
        }

        List<HostSpec> hostsToChange = this.AllHosts
            .Where(host => hostAliases.Contains(host.AsAlias())
                        || host.GetAliases().Any(alias => hostAliases.Contains(alias)))
            .Distinct()
            .ToList();

        if (hostsToChange.Count == 0)
        {
            Logger.LogTrace("There are no changes in the hosts' availability.");
            return;
        }

        var changes = new Dictionary<string, NodeChangeOptions>();

        foreach (HostSpec host in hostsToChange)
        {
            var currentAvailability = host.Availability;
            host.Availability = availability;
            Logger.LogTrace("Host {host} availability changed from {old} to {new}", host, currentAvailability, availability);
            HostAvailabilityExpiringCache.Set(host.GetHostAndPort(), availability, DefaultHostAvailabilityCacheExpiration);

            if (currentAvailability != availability)
            {
                NodeChangeOptions hostChanges;
                switch (availability)
                {
                    case HostAvailability.Available:
                        hostChanges = NodeChangeOptions.WentUp | NodeChangeOptions.NodeChanged;
                        break;
                    default:
                        hostChanges = NodeChangeOptions.WentDown | NodeChangeOptions.NodeChanged;
                        break;
                }

                changes[host.Host] = hostChanges;
            }
        }

        if (changes.Count > 0)
        {
            // TODO: implement NotifyNodeChangeList pipeline
            // this.pluginManager.NotifyNodeChangeList(changes);
        }
    }

    public void RefreshHostList()
    {
        IList<HostSpec> updateHostList = this.hostListProvider.Refresh();
        if (!updateHostList.SequenceEqual(this.AllHosts))
        {
            this.UpdateHostAvailability(updateHostList);
            this.NotifyNodeChangeList(this.AllHosts, updateHostList);
            this.AllHosts = updateHostList;
        }
    }

    public void RefreshHostList(DbConnection connection)
    {
        Logger.LogDebug("PluginService.RefreshHostList() called with connection state = {State}, type = {Type}@{Id}",
            connection.State,
            connection.GetType().FullName,
            RuntimeHelpers.GetHashCode(connection));

        IList<HostSpec> updateHostList = this.hostListProvider.Refresh(connection);
        this.UpdateHostAvailability(updateHostList);
        this.NotifyNodeChangeList(this.AllHosts, updateHostList);
        this.AllHosts = updateHostList;

        Logger.LogDebug("PluginService.RefreshHostList() completed with connection state = {State}", connection.State);
    }

    public void ForceRefreshHostList()
    {
        IList<HostSpec> updateHostList = this.hostListProvider.ForceRefresh();
        this.UpdateHostAvailability(updateHostList);
        this.NotifyNodeChangeList(this.AllHosts, updateHostList);
        this.AllHosts = updateHostList;
    }

    public void ForceRefreshHostList(DbConnection connection)
    {
        IList<HostSpec> updateHostList = this.hostListProvider.ForceRefresh(connection);
        this.UpdateHostAvailability(updateHostList);
        this.NotifyNodeChangeList(this.AllHosts, updateHostList);
        this.AllHosts = updateHostList;
    }

    public void ForceRefreshHostList(bool shouldVerifyWriter, long timeoutMs)
    {
        if (this.HostListProvider is IBlockingHostListProvider blockingHostListProvider)
        {
            IList<HostSpec> updateHostList = blockingHostListProvider.ForceRefresh(shouldVerifyWriter, timeoutMs);
            this.UpdateHostAvailability(updateHostList);
            this.NotifyNodeChangeList(this.AllHosts, updateHostList);
            this.AllHosts = updateHostList;
        }
        else
        {
            throw new InvalidOperationException("[PluginService] Required IBlockingHostListProvider");
        }
    }

    public DbConnection OpenConnection(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        IConnectionPlugin? pluginToSkip)
    {
        return this.pluginManager.Open(hostSpec, props, this.CurrentConnection == null, pluginToSkip);
    }

    public DbConnection ForceOpenConnection(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin? pluginToSkip, bool isInitialConnection)
    {
        return this.pluginManager.ForceOpen(hostSpec, props, isInitialConnection, pluginToSkip);
    }

    public DbConnection ForceOpenConnection(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin? pluginToSkip)
    {
        return this.pluginManager.ForceOpen(hostSpec, props, this.CurrentConnection == null, pluginToSkip);
    }

    public Task OpenConnectionAsync(HostSpec hostSpec, Dictionary<string, string> props)
    {
        throw new NotImplementedException();
    }

    public void UpdateDialect(DbConnection connection)
    {
        IDialect dialect = this.Dialect;
        this.Dialect = this.dialectProvider.UpdateDialect(connection, this.Dialect);
        Logger.LogDebug("Dialect updated to: {dialect}", this.Dialect.GetType().FullName);

        if (dialect != this.Dialect)
        {
            this.hostListProvider = this.Dialect.HostListProviderSupplier(this.props, this, this)
                                     ?? this.hostListProvider;
        }

        this.RefreshHostList(connection);
    }

    public HostSpec? IdentifyConnection(DbConnection connection)
    {
        return this.hostListProvider.IdentifyConnection(connection);
    }

    public void FillAliases(DbConnection connection, HostSpec hostSpec)
    {
        if (hostSpec.GetAliases().Count > 0)
        {
            return;
        }

        hostSpec.AddAlias(hostSpec.AsAlias());
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = this.Dialect.HostAliasQuery;

            using var resultSet = command.ExecuteReader();
            while (resultSet.Read())
            {
                string alias = resultSet.GetString(0);
                hostSpec.AddAlias(alias);
            }
        }
        catch
        {
            // ignore
        }

        HostSpec? existingHostSpec = this.IdentifyConnection(connection);
        if (existingHostSpec != null)
        {
            var aliases = existingHostSpec.AsAliases();
            foreach (string alias in aliases)
            {
                hostSpec.AddAlias(alias);
            }
        }
    }

    public IConnectionProvider GetConnectionProvider()
    {
        throw new NotImplementedException();
    }

    public bool AcceptsStrategy(string strategy)
    {
        return this.pluginManager.AcceptsStrategy(strategy);
    }

    public HostSpec GetHostSpecByStrategy(HostRole hostRole, string strategy)
    {
        return this.pluginManager.GetHostSpecByStrategy(hostRole, strategy, this.props);
    }

    public HostSpec GetHostSpecByStrategy(IList<HostSpec> hosts, HostRole hostRole, string strategy)
    {
        return this.pluginManager.GetHostSpecByStrategy(hosts, hostRole, strategy, this.props);
    }

    private HostSpec GetCurrentHostSpec()
    {
        this.currentHostSpec = this.InitialConnectionHostSpec
            ?? this.AllHosts.FirstOrDefault(h => h.Role == HostRole.Writer)
            ?? this.GetHosts().First();

        ArgumentNullException.ThrowIfNull(this.currentHostSpec);
        return this.currentHostSpec;
    }

    private void UpdateHostAvailability(IList<HostSpec> hosts)
    {
        foreach (HostSpec host in hosts)
        {
            HostAvailabilityExpiringCache.TryGetValue(host.GetHostAndPort(), out HostAvailability? availability);
            if (availability.HasValue)
            {
                host.Availability = availability.Value;
            }
        }
    }

    private void NotifyNodeChangeList(IList<HostSpec> oldHosts, IList<HostSpec> updateHosts)
    {
        // TODO: create NodeChangeList based on changes to hosts and call pluginManager.NotifyNodeChangeList.
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
}
