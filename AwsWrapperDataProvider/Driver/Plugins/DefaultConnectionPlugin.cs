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
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins;

public class DefaultConnectionPlugin(
    IPluginService pluginService,
    IConnectionProvider defaultConnProvider,
    IConnectionProvider? effectiveConnProvider) : IConnectionPlugin
{
    private const int UpdateDialectMaxRetries = 3;

    private static readonly ILogger<DefaultConnectionPlugin> Logger = LoggerUtils.GetLogger<DefaultConnectionPlugin>();

    public IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "*" };

    private readonly IConnectionProvider defaultConnProvider = defaultConnProvider;
    private readonly IConnectionProvider? effectiveConnPrivider = effectiveConnProvider;
    private readonly IPluginService pluginService = pluginService;

    public Task<T> Execute<T>(
        object methodInvokedOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        Logger.LogTrace(Resources.DefaultConnectionPlugin_Execute_ExecutingMethod, methodName, methodInvokedOn?.GetType().FullName, methodArgs);
        return methodFunc();
    }

    public Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection, false, async);
    }

    public Task<DbConnection> ForceOpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection, true, async);
    }

    /// <summary>
    /// Internal connection opening logic that mirrors JDBC wrapper's connectInternal method.
    /// Creates a new connection using the connection provider.
    /// </summary>
    private async Task<DbConnection> OpenInternal(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        IConnectionProvider connProvider,
        bool isInitialConnection,
        bool isForceOpen,
        bool async)
    {
        DbConnection conn = connProvider.CreateDbConnection(this.pluginService.Dialect, this.pluginService.TargetConnectionDialect, hostSpec, props);

        if (async)
        {
            await conn.OpenAsync();
        }
        else
        {
            conn.Open();
        }

        Logger.LogTrace(Resources.DefaultConnectionPlugin_OpenInternal_ConnectionOpened, conn.GetType().FullName, RuntimeHelpers.GetHashCode(conn), conn.DataSource);

        // TODO: Add configuration to skip ping check. (Not urgent)
        // Ping to check if connection is actually alive.
        // Due to connection pooling, the Open() call may succeed with Open status even if the database is not reachable.
        if (!isForceOpen)
        {
            for (int attempt = 1; attempt <= UpdateDialectMaxRetries; attempt++)
            {
                (bool pingSuccess, Exception? pingException) = this.pluginService.TargetConnectionDialect.Ping(conn);
                if (!pingSuccess)
                {
                    await conn.DisposeAsync();
                    conn = connProvider.CreateDbConnection(this.pluginService.Dialect, this.pluginService.TargetConnectionDialect, hostSpec, props);
                    await conn.OpenAsync();

                    if (attempt == UpdateDialectMaxRetries)
                    {
                        throw new InvalidOpenConnectionException(Resources.Error_UnableToEstablishValidConnectionAfterMultipleAttempts, pingException);
                    }
                }
                else
                {
                    break;
                }
            }
        }

        if (isInitialConnection)
        {
            await this.pluginService.UpdateDialectAsync(conn);
        }

        this.pluginService.SetAvailability(hostSpec!.AsAliases(), HostAvailability.Available);

        return conn;
    }

    public Task InitHostProvider(
        string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        ADONetDelegate initHostProviderFunc)
    {
        return Task.CompletedTask;
    }
}
