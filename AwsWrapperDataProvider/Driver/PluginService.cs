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
    private readonly ConnectionPluginManager _pluginManager;
    private readonly Dictionary<string, string> _props;
    private readonly string _originalConnectionString;
    private readonly ITargetConnectionDialect _targetConnectionDialect;
    private volatile IHostListProvider _hostListProvider;
    private IDialect _dialect;
    private IList<HostSpec> _allHosts = [];
    private HostSpec? _currentHostSpec;
    private DbConnection? _currentConnection;
    private HostSpec? _initialConnectionHostSpec;
    private ConfigurationProfile? _configurationProfile;

    // private ExceptionManager _exceptionManager;
    // private IExceptionHandler _exceptionHandler;

    public IDialect Dialect { get => this._dialect; }
    public ITargetConnectionDialect TargetConnectionDialect { get => this._targetConnectionDialect; }
    public HostSpec? InitialConnectionHostSpec { get => this._initialConnectionHostSpec; set => this._initialConnectionHostSpec = value; }
    public HostSpec? CurrentHostSpec { get => this._currentHostSpec ?? this.GetCurrentHostSpec(); }
    public IList<HostSpec> AllHosts { get => this._allHosts; }
    public IHostListProvider? HostListProvider { get => this._hostListProvider; set => this._hostListProvider = value ?? throw new ArgumentNullException(nameof(value)); }
    public HostSpecBuilder HostSpecBuilder { get => new HostSpecBuilder(); }
    public DbConnection? CurrentConnection { get => this._currentConnection; set => this._currentConnection = value; }

    public PluginService(
        Type connectionType,
        ConnectionPluginManager pluginManager,
        Dictionary<string, string> props,
        string connectionString,
        ITargetConnectionDialect targetConnectionDialect)
    {
        this._pluginManager = pluginManager;
        this._props = props;
        this._originalConnectionString = connectionString;
        this._targetConnectionDialect = targetConnectionDialect;
        this._dialect = DialectProvider.GuessDialect(this._props);
        this._hostListProvider =
            this._dialect.HostListProviderSupplier(this._props, this, this)
            ?? throw new InvalidOperationException(); // TODO : throw proper error
    }

    public bool IsStaticHostListProvider()
    {
        throw new NotImplementedException();
    }

    public HostSpec GetInitialConnectionHostSpec()
    {
        // TODO implement stub method.
        return new HostSpec("temp", 0000, "temp", HostRole.Reader, HostAvailability.Available);
    }

    public void SetCurrentConnection(DbConnection connection, HostSpec? hostSpec)
    {
        // TODO implement stub method.
        this._currentConnection = connection;
        this._currentHostSpec = hostSpec;
    }

    public void SetCurrentConnection(DbConnection connection, HostSpec hostSpec, IConnectionPlugin pluginToSkip)
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> GetHosts()
    {
        // TODO: Handle AllowedAndBlockHosts
        return this._allHosts;
    }

    public HostRole GetHostRole(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public HostRole SetAvailability(ISet<string> hostAliases, HostAvailability availability)
    {
        throw new NotImplementedException();
    }

    public void RefreshHostList()
    {
        IList<HostSpec> updateHostList = this._hostListProvider.Refresh();
        if (!Equals(updateHostList, this._allHosts))
        {
            this.UpdateHostAvailability(updateHostList);
            this.NotifyNodeChangeList(this._allHosts, updateHostList);
            this._allHosts = updateHostList;
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
        IDialect dialect = this._dialect;
        this._dialect = DialectProvider.UpdateDialect(
            connection,
            this._dialect);

        if (dialect != this._dialect)
        {
            this._hostListProvider = this._dialect.HostListProviderSupplier(this._props, this, this)
                                     ?? this._hostListProvider;
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
        this._currentHostSpec = this._initialConnectionHostSpec
            ?? this._allHosts.FirstOrDefault(h => h.Role == HostRole.Writer)
            ?? this.GetHosts().First();

        ArgumentNullException.ThrowIfNull(this._currentHostSpec);
        return this._currentHostSpec;
    }

    private void UpdateHostAvailability(IList<HostSpec> hosts)
    {
        // TODO: deal with availability.
    }

    private void NotifyNodeChangeList(IList<HostSpec> oldHosts, IList<HostSpec> updateHosts)
    {
        // TODO: create NodeChangeList based on changes to hosts and call _pluginManager.NotifyNodeChangeList.
    }
}
