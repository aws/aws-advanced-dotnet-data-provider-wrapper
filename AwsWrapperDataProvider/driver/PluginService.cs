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
using AwsWrapperDataProvider.driver.connectionProviders;
using AwsWrapperDataProvider.driver.dialects;
using AwsWrapperDataProvider.driver.exceptions;
using AwsWrapperDataProvider.driver.hostInfo;
using AwsWrapperDataProvider.driver.hostListProviders;
using AwsWrapperDataProvider.driver.plugins;
using AwsWrapperDataProvider.driver.targetDriverDialects;

namespace AwsWrapperDataProvider.driver;

public class PluginService : IPluginService, IHostListProviderService
{
    private ConnectionPluginManager _pluginManager;
    private Dictionary<string, string> _props;
    private string _originalConnectionString;
    private volatile IHostListProvider _hostListProvider;
    private DbConnection? _currentConnection;
    private HostSpec? _currentHostSpec;
    private HostSpec _initialConnectionHostSpec;
    private List<HostSpec> _allHosts = [];
    // private ExceptionManager _exceptionManager;
    // private IExceptionHandler _exceptionHandler;
    private IDialect _dialect;
    private ITargetDriverDialect _targetDriverDialect;
    
    public IDialect Dialect { get => _dialect; }
    public ITargetDriverDialect TargetDriverDialect { get => _targetDriverDialect; }
    public HostSpec InitialConnectionHostSpec { get => _initialConnectionHostSpec; set => _initialConnectionHostSpec = value; }
    public HostSpec CurrentHostSpec { get => _currentHostSpec; }
    public IList<HostSpec> AllHosts { get => _allHosts; }
    public IHostListProvider HostListProvider { get => _hostListProvider; set => _hostListProvider = value; }
    public HostSpecBuilder HostSpecBuilder { get; }
    public DbConnection? CurrentConnection { get => _currentConnection; set => _currentConnection = value; }

    public PluginService(
        DbConnection? currentConnection,
        Type connectionType,
        ConnectionPluginManager pluginManager,
        Dictionary<string, string> props,
        string connectionString,
        ITargetDriverDialect targetDriverDialect)
    {
        if (currentConnection != null)
        {
            _currentConnection = currentConnection;
        }
        _pluginManager = pluginManager;
        _props = props;
        _originalConnectionString = connectionString;
        _targetDriverDialect = targetDriverDialect;
        _dialect = DialectProvider.GetDialect(connectionType, props);
    }
    
    public bool IsStaticHostListProvider()
    {
        throw new NotImplementedException();
    }
    
    public HostSpec GetInitialConnectionHostSpec()
    {
        // TODO implement stub method.
        return new HostSpec("temp",0000,"temp",HostRole.Reader,HostAvailability.Available);
    }
    
    public void SetCurrentConnection(DbConnection connection, HostSpec hostSpec)
    {
        // TODO implement stub method.
        this._currentConnection = connection;
    }

    public void SetCurrentConnection(DbConnection connection, HostSpec hostSpec, IConnectionPlugin pluginToSkip)
    {
        throw new NotImplementedException();
    }
    
    public IList<HostSpec> GetHosts()
    {
        throw new NotImplementedException();
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
        // TODO implement stub method.
        return;
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
        throw new NotImplementedException();
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
}