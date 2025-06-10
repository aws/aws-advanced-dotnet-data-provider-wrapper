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
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;

namespace AwsWrapperDataProvider.Driver;

public class PluginService : IPluginService, IHostListProviderService
{
    private readonly ConnectionPluginManager pluginManager;
    private readonly Dictionary<string, string> props;
    private readonly string originalConnectionString;
    private readonly DialectProvider dialectProvider;
    private volatile IHostListProvider hostListProvider;
    private HostSpec? currentHostSpec;

    // private ExceptionManager _exceptionManager;
    // private IExceptionHandler _exceptionHandler;

    public IDialect Dialect { get; private set; }
    public ITargetConnectionDialect TargetConnectionDialect { get; }
    public HostSpec? InitialConnectionHostSpec { get; set; }
    public HostSpec? CurrentHostSpec { get => this.currentHostSpec ?? this.GetCurrentHostSpec(); }
    public IList<HostSpec> AllHosts { get; private set; } = [];
    public IHostListProvider? HostListProvider { get => this.hostListProvider; set => this.hostListProvider = value ?? throw new ArgumentNullException(nameof(value)); }
    public HostSpecBuilder HostSpecBuilder { get => new HostSpecBuilder(); }
    public DbConnection? CurrentConnection { get; set; }

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
        this.originalConnectionString = connectionString;
        this.TargetConnectionDialect = configurationProfile?.TargetConnectionDialect ?? targetConnectionDialect ?? throw new ArgumentNullException(nameof(targetConnectionDialect));
        this.dialectProvider = new(this);
        this.Dialect = configurationProfile?.Dialect ?? this.dialectProvider.GuessDialect(this.props);
        this.hostListProvider =
            this.Dialect.HostListProviderSupplier(this.props, this, this)
            ?? throw new InvalidOperationException(); // TODO : throw proper error
    }

    // for testing purpose only
#pragma warning disable CS8618
    internal PluginService() { }
#pragma warning restore CS8618

    public bool IsStaticHostListProvider()
    {
        throw new NotImplementedException();
    }

    public HostSpec GetInitialConnectionHostSpec()
    {
        // TODO implement stub method.
        throw new NotImplementedException();
    }

    public void SetCurrentConnection(DbConnection connection, HostSpec? hostSpec)
    {
        // TODO implement stub method.
        this.CurrentConnection = connection;
        this.currentHostSpec = hostSpec;
    }

    public void SetCurrentConnection(DbConnection connection, HostSpec hostSpec, IConnectionPlugin pluginToSkip)
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> GetHosts()
    {
        // TODO: Handle AllowedAndBlockHosts
        return this.AllHosts;
    }

    public HostRole GetHostRole(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public void SetAvailability(ICollection<string> hostAliases, HostAvailability availability)
    {
        // TODO: Implement
        return;
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
        throw new NotImplementedException();
    }

    public void ForceRefreshHostList()
    {
        throw new NotImplementedException();
    }

    public void ForceRefreshHostList(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public void ForceRefreshHostList(bool shouldVerifyWriter, long timeoutMs)
    {
        throw new NotImplementedException();
    }

    public DbConnection Connect(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin pluginToSkip)
    {
        throw new NotImplementedException();
    }

    public DbConnection ForceConnect(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin pluginToSkip)
    {
        throw new NotImplementedException();
    }

    public void UpdateDialect(DbConnection connection)
    {
        IDialect dialect = this.Dialect;
        this.Dialect = this.dialectProvider.UpdateDialect(connection, this.Dialect);

        if (dialect != this.Dialect)
        {
            this.hostListProvider = this.Dialect.HostListProviderSupplier(this.props, this, this)
                                     ?? this.hostListProvider;
        }
    }

    public HostSpec IdentifyConnection(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public void FillAliases(DbConnection connection, HostSpec hostSpec)
    {
        throw new NotImplementedException();
    }

    public IConnectionProvider GetConnectionProvider()
    {
        throw new NotImplementedException();
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
        // TODO: deal with availability.
    }

    private void NotifyNodeChangeList(IList<HostSpec> oldHosts, IList<HostSpec> updateHosts)
    {
        // TODO: create NodeChangeList based on changes to hosts and call pluginManager.NotifyNodeChangeList.
    }
}
