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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Plugins;

public class DefaultConnectionPlugin(
    IPluginService pluginService,
    IConnectionProvider defaultConnProvider,
    IConnectionProvider? effectiveConnProvider) : IConnectionPlugin
{
    public IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "*" };

    private readonly IConnectionProvider defaultConnProvider = defaultConnProvider;
    private readonly IConnectionProvider? effectiveConnPrivider = effectiveConnProvider;
    private readonly IPluginService pluginService = pluginService;

    public T Execute<T>(
        object methodInvokedOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        return methodFunc();
    }

    public DbConnection OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc)
    {
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection);
    }

    public DbConnection ForceOpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc)
    {
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection);
    }

    /// <summary>
    /// Internal connection opening logic that mirrors JDBC wrapper's connectInternal method.
    /// Creates a new connection using the connection provider.
    /// </summary>
    private DbConnection OpenInternal(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        IConnectionProvider connProvider,
        bool isInitialConnection)
    {
        // Create a new connection if it's not the initial connection or CurrentConnection is not null
        DbConnection? conn = isInitialConnection && this.pluginService.CurrentConnection != null
            ? this.pluginService.CurrentConnection
            : connProvider.CreateDbConnection(
                this.pluginService.Dialect,
                this.pluginService.TargetConnectionDialect,
                hostSpec,
                props);

        conn.Open();

        // Set availability and update dialect
        this.pluginService.SetAvailability(hostSpec!.AsAliases(), HostAvailability.Available);
        if (isInitialConnection)
        {
            this.pluginService.UpdateDialect(conn);
        }

        return conn;
    }

    public void InitHostProvider(
        string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        ADONetDelegate initHostProviderFunc)
    {
        // do nothing
    }
}
