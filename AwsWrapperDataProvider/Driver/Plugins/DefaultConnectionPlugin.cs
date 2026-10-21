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
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
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
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection, false, async, CancellationToken.None);
    }

    /// <summary>
    /// Opens the connection the caller asked for, passing <paramref name="cancellationToken"/> to the target driver.
    /// <see cref="IConnectionPlugin.OpenConnection"/> has no token, so <see cref="ConnectionPluginManager"/> calls
    /// this overload directly when it reaches the end of the chain.
    /// </summary>
    internal Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool async,
        CancellationToken cancellationToken)
    {
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection, false, async, cancellationToken);
    }

    public Task<DbConnection> ForceOpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        return this.OpenInternal(hostSpec, props, this.defaultConnProvider, isInitialConnection, true, async, CancellationToken.None);
    }

    /// <summary>
    /// Creates a new connection using the connection provider.
    /// </summary>
    private async Task<DbConnection> OpenInternal(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        IConnectionProvider connProvider,
        bool isInitialConnection,
        bool isForceOpen,
        bool async,
        CancellationToken cancellationToken)
    {
        DbConnection conn = connProvider.CreateDbConnection(this.pluginService.Dialect, this.pluginService.TargetConnectionDialect, hostSpec, props);

        ITelemetryContext targetDriverContext = this.pluginService.TelemetryFactory
            .OpenTelemetryContext(conn.GetType().FullName ?? "target", TelemetryTraceLevel.Nested);
        try
        {
            if (async)
            {
                await OpenTargetConnectionAsync(conn, cancellationToken);
            }
            else
            {
                conn.Open();
            }

            targetDriverContext.SetSuccess(true);
        }
        catch (Exception ex)
        {
            targetDriverContext.SetException(ex);
            targetDriverContext.SetSuccess(false);
            throw;
        }
        finally
        {
            targetDriverContext.CloseContext();
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
                    await OpenTargetConnectionAsync(conn, cancellationToken);

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

        this.pluginService.SetAvailability(hostSpec!, HostAvailability.Available);

        return conn;
    }

    /// <summary>
    /// Opens the target driver connection, reporting a cancelled open as an <see cref="OperationCanceledException"/>
    /// whatever the driver threw.
    /// </summary>
    private static async Task OpenTargetConnectionAsync(DbConnection conn, CancellationToken cancellationToken)
    {
        try
        {
            await conn.OpenAsync(cancellationToken);
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested && ex is not OperationCanceledException)
        {
            // Cancelling a connect partway through can surface as a driver or socket error instead: Npgsql on
            // Windows reports an aborted DNS lookup as an NpgsqlException wrapping a SocketException. The plugins
            // above would read that as a host failure (connect failover and the initial connection strategy both
            // act on it), so report it as the cancellation it is. The driver error is logged rather than chained,
            // because the plugins classify an exception by walking its inner exceptions.
            Logger.LogDebug(ex, Resources.DefaultConnectionPlugin_OpenTargetConnection_CancelledWithDriverError, conn.GetType().FullName);
            throw new OperationCanceledException(cancellationToken);
        }
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
