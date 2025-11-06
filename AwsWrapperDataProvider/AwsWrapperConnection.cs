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
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider;

public class AwsWrapperConnection : DbConnection
{
    private static readonly ILogger<AwsWrapperConnection> Logger = LoggerUtils.GetLogger<AwsWrapperConnection>();

    protected readonly IPluginService pluginService;
    private readonly IHostListProviderService hostListProviderService;
    private Type targetType;
    private string connectionString;
    private string? database;

    internal ConnectionPluginManager PluginManager { get; }

    internal Dictionary<string, string> ConnectionProperties { get; private set; }

    internal DbConnection? TargetDbConnection => this.pluginService.CurrentConnection;

    internal readonly List<AwsWrapperCommand> ActiveWrapperCommands = new();

    [AllowNull]
    public override string ConnectionString
    {
        get => this.pluginService.CurrentConnection?.ConnectionString ?? this.connectionString ?? string.Empty;
        set
        {
            this.connectionString = value ?? string.Empty;
            this.ConnectionProperties = ConnectionPropertiesUtils.ParseConnectionStringParameters(this.connectionString);
            this.targetType = this.GetTargetType(this.ConnectionProperties);

            if (this.pluginService.CurrentConnection != null)
            {
                this.pluginService.CurrentConnection.ConnectionString = value;
            }
        }
    }

    public override int ConnectionTimeout => this.TargetDbConnection?.ConnectionTimeout ?? base.ConnectionTimeout;

    public AwsWrapperConnection(DbConnection connection, ConfigurationProfile? profile) : this(
        connection.GetType(),
        connection.ConnectionString,
        profile)
    {
        this.pluginService!.SetCurrentConnection(connection, this.pluginService.InitialConnectionHostSpec);
        this.database = connection.Database;
    }

    public AwsWrapperConnection(DbConnection connection) : this(connection, null)
    { }

    public AwsWrapperConnection(string connectionString, ConfigurationProfile? profile) : this(
        null,
        connectionString,
        profile)
    { }

    public AwsWrapperConnection(string connectionString) : this(null, connectionString) { }

    public AwsWrapperConnection(Type? targetType, string connectionString) : this(
        targetType,
        connectionString,
        null)
    { }

    public AwsWrapperConnection(Type? targetType, string connectionString, ConfigurationProfile? profile) : base()
    {
        this.connectionString = connectionString;
        this.ConnectionProperties = profile?.Properties ?? ConnectionPropertiesUtils.ParseConnectionStringParameters(this.connectionString);
        this.targetType = targetType ?? this.GetTargetType(this.ConnectionProperties);
        this.ConnectionProperties[PropertyDefinition.TargetConnectionType.Name] = this.targetType.AssemblyQualifiedName!;

        ITargetConnectionDialect connectionDialect = TargetConnectionDialectProvider.GetDialect(this.targetType, this.ConnectionProperties);

        DbConnectionProvider connectionProvider = new();

        this.PluginManager = new(connectionProvider, null, this, profile);

        PluginService pluginService = new(this, this.PluginManager, this.ConnectionProperties, connectionDialect, profile);

        this.pluginService = pluginService;
        this.hostListProviderService = pluginService;

        this.PluginManager.InitConnectionPluginChain(this.pluginService, this.ConnectionProperties);

        this.pluginService.RefreshHostListAsync().GetAwaiter().GetResult();
        this.pluginService.SetCurrentConnection(
            connectionProvider.CreateDbConnection(
                this.pluginService.Dialect,
                this.pluginService.TargetConnectionDialect,
                null,
                this.ConnectionProperties),
            this.pluginService.InitialConnectionHostSpec);
    }

    public override string Database => this.pluginService.CurrentConnection?.Database ?? this.database ?? string.Empty;

    public override string DataSource => this.pluginService.CurrentConnection is DbConnection dbConnection
        ? dbConnection.DataSource
        : string.Empty;

    public override string ServerVersion => this.pluginService.CurrentConnection is DbConnection dbConnection
        ? dbConnection.ServerVersion
        : string.Empty;

    public override ConnectionState State => this.pluginService.CurrentConnection?.State ?? ConnectionState.Closed;

    public override void ChangeDatabase(string databaseName)
    {
        this.database = databaseName;
        WrapperUtils.RunWithPlugins(
            this.PluginManager,
            this.pluginService.CurrentConnection!,
            "DbConnection.ChangeDatabase",
            () =>
            {
                this.pluginService.CurrentConnection!.ChangeDatabase(databaseName);
                return Task.CompletedTask;
            },
            databaseName).GetAwaiter().GetResult();
    }

    public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
    {
        this.database = databaseName;
        return WrapperUtils.RunWithPlugins(
            this.PluginManager,
            this.pluginService.CurrentConnection!,
            "DbConnection.ChangeDatabaseAsync",
            () => this.pluginService.CurrentConnection!.ChangeDatabaseAsync(databaseName),
            databaseName);
    }

    public override void Close()
    {
        WrapperUtils.RunWithPlugins(
            this.PluginManager,
            this.pluginService.CurrentConnection!,
            "DbConnection.Close",
            () =>
            {
                this.pluginService.CurrentConnection!.Close();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
    }

    public override Task CloseAsync()
    {
        return WrapperUtils.RunWithPlugins(
            this.PluginManager,
            this.pluginService.CurrentConnection!,
            "DbConnection.CloseAsync",
            () => this.pluginService.CurrentConnection!.CloseAsync());
    }

    public override void Open()
    {
        this.OpenInternal(CancellationToken.None, false).GetAwaiter().GetResult();
    }

    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        return this.OpenInternal(cancellationToken, true);
    }

    private async Task OpenInternal(CancellationToken cancellationToken, bool async)
    {
        if (this.State != ConnectionState.Closed)
        {
            throw new InvalidOperationException(Properties.Resources.Error_ConnectionAlreadyOpen);
        }

        ArgumentNullException.ThrowIfNull(this.pluginService);
        ArgumentNullException.ThrowIfNull(this.PluginManager);
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);

        await this.PluginManager.InitHostProvider(this.connectionString, this.ConnectionProperties, this.hostListProviderService);

        var currentHostSpec = this.pluginService.CurrentHostSpec;

        DbConnection connection = await WrapperUtils.OpenWithPlugins(
            this.PluginManager,
            currentHostSpec,
            this.ConnectionProperties,
            true,
            async);
        this.pluginService.SetCurrentConnection(connection, currentHostSpec);
        await this.pluginService.RefreshHostListAsync();
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        ArgumentNullException.ThrowIfNull(this.pluginService);
        ArgumentNullException.ThrowIfNull(this.pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this.PluginManager);

        DbTransaction targetTransaction = WrapperUtils.ExecuteWithPlugins(
            this.PluginManager,
            this.pluginService.CurrentConnection,
            "DbConnection.BeginDbTransaction",
            () => Task.FromResult(this.pluginService.CurrentConnection!.BeginTransaction(isolationLevel)),
            isolationLevel)
            .GetAwaiter().GetResult();

        this.pluginService.CurrentTransaction = targetTransaction;
        return new AwsWrapperTransaction(this, this.pluginService, this.PluginManager);
    }

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(this.pluginService);
        ArgumentNullException.ThrowIfNull(this.pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this.PluginManager);

        DbTransaction targetTransaction = await WrapperUtils.ExecuteWithPlugins(
            this.PluginManager,
            this.pluginService.CurrentConnection,
            "DbConnection.BeginDbTransactionAsync",
            async () => await this.pluginService.CurrentConnection!.BeginTransactionAsync(isolationLevel, cancellationToken),
            isolationLevel,
            cancellationToken);

        this.pluginService.CurrentTransaction = targetTransaction;
        return new AwsWrapperTransaction(this, this.pluginService, this.PluginManager);
    }

    protected override DbCommand CreateDbCommand() => this.CreateCommand<DbCommand>();

    public AwsWrapperCommand<TCommand> CreateCommand<TCommand>() where TCommand : DbCommand
    {
        ArgumentNullException.ThrowIfNull(this.pluginService);
        ArgumentNullException.ThrowIfNull(this.pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this.PluginManager);

        TCommand command = WrapperUtils.ExecuteWithPlugins<TCommand>(
            this.PluginManager,
            this.pluginService.CurrentConnection,
            "DbConnection.CreateCommand",
            () => Task.FromResult((TCommand)this.pluginService.CurrentConnection.CreateCommand()))
            .GetAwaiter().GetResult();
        Logger.LogDebug("DbCommand created for DbConnection@{Id}", RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection));

        this.ConnectionProperties[PropertyDefinition.TargetCommandType.Name] = typeof(TCommand).AssemblyQualifiedName!;
        var wrapperCommand = new AwsWrapperCommand<TCommand>(command, this, this.PluginManager);
        this.ActiveWrapperCommands.Add(wrapperCommand);
        return wrapperCommand;
    }

    protected override DbBatch CreateDbBatch() => this.CreateBatch();

    public new AwsWrapperBatch CreateBatch()
    {
        DbBatch batch = this.TargetDbConnection!.CreateBatch();
        return new AwsWrapperBatch(batch, this, this.PluginManager);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Logger.LogTrace("Disposing target db connection@{id}", RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection));
            this.pluginService.CurrentConnection?.Dispose();
            this.pluginService.SetCurrentConnection(null, null);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (this.pluginService.CurrentConnection is not null)
        {
            Logger.LogTrace("Disposing target db connection@{id}", RuntimeHelpers.GetHashCode(this.pluginService.CurrentConnection));
            await this.pluginService.CurrentConnection.DisposeAsync().ConfigureAwait(false);
            this.pluginService.SetCurrentConnection(null, null);
        }
    }

    private Type GetTargetType(Dictionary<string, string> props)
    {
        string? targetConnectionTypeString = PropertyDefinition.TargetConnectionType.GetString(props);
        if (!string.IsNullOrEmpty(targetConnectionTypeString))
        {
            try
            {
                Type? targetType = Type.GetType(targetConnectionTypeString);
                if (targetType == null)
                {
                    throw new Exception(string.Format(Properties.Resources.Error_CantLoadTargetConnectionType, targetConnectionTypeString));
                }

                return targetType;
            }
            catch
            {
                throw new Exception(string.Format(Properties.Resources.Error_CantLoadTargetConnectionType, targetConnectionTypeString));
            }
        }

        throw new Exception(string.Format(Properties.Resources.Error_CantLoadTargetConnectionType, targetConnectionTypeString));
    }

    internal void UnregisterWrapperCommand(AwsWrapperCommand command)
    {
        this.ActiveWrapperCommands.Remove(command);
    }
}

public class AwsWrapperConnection<TConn> : AwsWrapperConnection where TConn : DbConnection
{
    public AwsWrapperConnection(string connectionString) : base(typeof(TConn), connectionString) { }

    public AwsWrapperConnection(string connectionString, ConfigurationProfile profile) : base(
        typeof(TConn),
        connectionString,
        profile)
    { }

    internal new TConn? TargetDbConnection => this.pluginService?.CurrentConnection as TConn;
}
