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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins.Failover.Exceptions;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;

public class ReadWriteSplittingPlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<ReadWriteSplittingPlugin> Logger = LoggerUtils.GetLogger<ReadWriteSplittingPlugin>();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly string readerHostSelectorStrategy;

    private int inReadWriteSplit = 0;

    private bool InReadWriteSplit
    {
        get => Volatile.Read(ref this.inReadWriteSplit) == 1;
        set => Interlocked.Exchange(ref this.inReadWriteSplit, value ? 1 : 0);
    }

    private IHostListProviderService? hostListProviderService;

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>()
    {
        "DbConnection.Open",
        "DbConnection.OpenAsync",

        "DbCommand.ExecuteNonQuery",
        "DbCommand.ExecuteNonQueryAsync",
        "DbCommand.ExecuteReader",
        "DbCommand.ExecuteReaderAsync",
        "DbCommand.ExecuteScalar",
        "DbCommand.ExecuteScalarAsync",

        "DbBatch.ExecuteNonQuery",
        "DbBatch.ExecuteNonQueryAsync",
        "DbBatch.ExecuteNonQuery",
        "DbBatch.ExecuteNonQueryAsync",
        "DbBatch.ExecuteScalar",
        "DbBatch.ExecuteScalarAsync",

        "initHostProvider",
    };

    public ReadWriteSplittingPlugin(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.readerHostSelectorStrategy = PropertyDefinition.RWSplittingReaderHostSelectorStrategy.GetString(props)!;
    }

    public override async Task<T> Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        var query = WrapperUtils.GetQueryFromSqlObject(methodInvokedOn);
        var (readOnly, found) = WrapperUtils.DoesSetReadOnly(query, this.pluginService.Dialect);
        Logger.LogDebug("ReadOnly is " + (found ? "set to" + (readOnly ? "read only" : "read write") : "not set"));
        if (found)
        {
            await this.SwitchConnectionIfRequired(readOnly);
        }

        try
        {
            return await methodFunc();
        }
        catch (FailoverException)
        {
            Logger.LogTrace(string.Format(Resources.ReadWriteSplittingPlugin_ExceptionWhileExecutingCommand, methodName));
            throw;
        }
    }

    public override Task InitHostProvider(string initialUrl, Dictionary<string, string> props, IHostListProviderService hostListProviderService, ADONetDelegate initHostProviderFunc)
    {
        this.hostListProviderService = hostListProviderService;
        initHostProviderFunc();
        return Task.CompletedTask;
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        if (!this.pluginService.AcceptsStrategy(this.readerHostSelectorStrategy))
        {
            throw new NotSupportedException(string.Format(Resources.Error_DriverDoesNotSupportRequestedHostSelectionStrategy, this.readerHostSelectorStrategy));
        }

        DbConnection conn = await methodFunc();
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);
        if (!isInitialConnection || this.hostListProviderService.IsStaticHostListProvider())
        {
            return conn;
        }

        HostRole currentRole = await this.pluginService.GetHostRole(conn);
        if (currentRole == HostRole.Unknown)
        {
            throw new InvalidOperationException(Resources.Error_InvalidHostRole);
        }

        HostSpec currentHostSpec = this.pluginService.InitialConnectionHostSpec!;
        if (currentRole == currentHostSpec.Role)
        {
            return conn;
        }

        HostSpec updatedHostSpec = new(currentHostSpec, currentRole);
        this.hostListProviderService.InitialConnectionHostSpec = updatedHostSpec;
        return conn;
    }

    private async Task SwitchConnectionIfRequired(bool readOnly)
    {
        DbConnection? currentConnection = this.pluginService.CurrentConnection;

        if (currentConnection != null && currentConnection.State == ConnectionState.Closed)
        {
            throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_SetReadOnlyOnClosedConnection);
        }

        if (this.IsConnectionUsable(currentConnection))
        {
            try
            {
                await this.pluginService.RefreshHostListAsync();
            }
            catch (DbException)
            {
                // ignore
            }
        }

        var hosts = this.pluginService.GetHosts();
        if (hosts is null || hosts.Count == 0)
        {
            throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_EmptyHostList);
        }

        var currentHost = this.pluginService.CurrentHostSpec!;
        if (readOnly)
        {
            // Not in a transaction and currently not on a reader, try switch to reader
            if (this.pluginService.CurrentTransaction == null && currentHost.Role != HostRole.Reader)
            {
                try
                {
                    await this.SwitchToReaderConnection(hosts);
                }
                catch (DbException ex)
                {
                    if (!this.IsConnectionUsable(currentConnection))
                    {
                        throw new ReadWriteSplittingDbException(string.Format(Resources.ReadWriteSplittingPlugin_ErrorSwitchingToReader, ex.Message), ex);
                    }

                    // Failed to switch to a reader, fallback to the current writer
                    Logger.LogInformation(Resources.ReadWriteSplittingPlugin_ErrorSwitchingToReader, ex.Message, this.pluginService.CurrentHostSpec!.GetHostAndPort());
                }
            }
        }
        else
        {
            // TODO: What if transaciton is started via raw SQL
            if (currentHost.Role != HostRole.Writer && this.pluginService.CurrentTransaction != null)
            {
                throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_SetReadOnlyFalseInTransaction);
            }

            if (currentHost.Role != HostRole.Writer)
            {
                try
                {
                    await this.SwitchToWriterConnection(hosts);
                }
                catch (DbException ex)
                {
                    throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_ErrorSwitchingToWriter, ex);
                }
            }
        }
    }

    private async Task SwitchToWriterConnection(IList<HostSpec> hosts)
    {
        var currentConnection = this.pluginService.CurrentConnection;
        var currentHost = this.pluginService.CurrentHostSpec!;
        Logger.LogDebug(currentHost.ToString());

        if (currentHost.Role == HostRole.Writer && this.IsConnectionUsable(currentConnection))
        {
            return;
        }

        var writerHost = WrapperUtils.GetWriter(hosts) ?? throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_NoWriterFound);
        this.InReadWriteSplit = true;

        await this.InitializeWriterConnection(writerHost);

        Logger.LogTrace(Resources.ReadWriteSplittingPlugin_SwitchedFromReaderToWriter, writerHost.GetHostAndPort());
    }

    private void SwitchCurrentConnectionTo(DbConnection? newConnection, HostSpec newConnectionHost)
    {
        if (this.pluginService.CurrentConnection == newConnection)
        {
            return;
        }

        this.pluginService.SetCurrentConnection(newConnection, newConnectionHost);
        Logger.LogTrace(Resources.ReadWriteSplittingPlugin_SettingCurrentConnection, newConnectionHost.GetHostAndPort());
    }

    private async Task InitializeWriterConnection(HostSpec writerHost)
    {
        var connection = await this.pluginService.OpenConnection(writerHost, this.props, this, true);
        Logger.LogInformation(Resources.ReadWriteSplittingPlugin_SetWriterConnection, writerHost.GetHostAndPort());
        this.SwitchCurrentConnectionTo(connection, writerHost);
    }

    private async Task SwitchToReaderConnection(IList<HostSpec> hosts)
    {
        var currentConnection = this.pluginService.CurrentConnection;
        var currentHost = this.pluginService.CurrentHostSpec!;
        if (currentHost.Role == HostRole.Reader && this.IsConnectionUsable(currentConnection))
        {
            return;
        }

        this.InReadWriteSplit = true;
        await this.InitializeReaderConnection(hosts);
    }

    private async Task InitializeReaderConnection(IList<HostSpec> hosts)
    {
        if (hosts.Count == 1)
        {
            HostSpec writerHost = WrapperUtils.GetWriter(hosts) ?? throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_NoWriterFound);
            await this.InitializeWriterConnection(writerHost);
            Logger.LogWarning(string.Format(Resources.ReadWriteSplittingPlugin_NoReadersFound), writerHost.GetHostAndPort());
        }
        else
        {
            await this.OpenNewReaderConnection();
        }
    }

    private async Task OpenNewReaderConnection()
    {
        DbConnection? connection = null;
        HostSpec? readerHost = null;
        int attempts = this.pluginService.GetHosts().Count * 2;
        for (int i = 0; i < attempts; i++)
        {
            HostSpec hostSpec = this.pluginService.GetHostSpecByStrategy(HostRole.Reader, this.readerHostSelectorStrategy);
            try
            {
                connection = await this.pluginService.OpenConnection(hostSpec, this.props, this, true);
                readerHost = hostSpec;
                break;
            }
            catch (DbException ex)
            {
                Logger.LogWarning(ex, string.Format(Resources.ReadWriteSplittingPlugin_FailedToConnectToReader, hostSpec.GetHostAndPort()));
            }
        }

        if (connection == null || readerHost == null)
        {
            throw new ReadWriteSplittingDbException(Resources.ReadWriteSplittingPlugin_NoReadersAvailable);
        }

        Logger.LogTrace(string.Format(Resources.ReadWriteSplittingPlugin_SuccessfullyConnectedToReader, readerHost.GetHostAndPort()));
        this.SwitchCurrentConnectionTo(connection, readerHost);
    }

    private bool IsConnectionUsable(DbConnection? connection)
    {
        return connection is not null && connection.State == ConnectionState.Open;
    }
}
