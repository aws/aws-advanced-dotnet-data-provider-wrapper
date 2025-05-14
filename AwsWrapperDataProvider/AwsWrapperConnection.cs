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
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperConnection : DbConnection
{
    protected readonly IPluginService? _pluginService;
    private readonly ConnectionPluginManager? _pluginManager;
    private readonly IHostListProviderService? _hostListProviderService;
    private Type? _targetType;
    private string? _connectionString;
    private string? _database;
    private Dictionary<string, string>? _props;

    public DbConnection? TargetConnection => this._pluginService?.CurrentConnection;

    [AllowNull]
    public override string ConnectionString
    {
        get => this._pluginService!.CurrentConnection?.ConnectionString ?? this._connectionString ?? string.Empty;
        set
        {
            this._connectionString = value ?? string.Empty;
            this._props = ConnectionPropertiesUtils.ParseConnectionStringParameters(this._connectionString);
            this._targetType = this.GetTargetType(this._props);

            if (this._pluginService?.CurrentConnection != null)
            {
                this._pluginService.CurrentConnection.ConnectionString = value;
            }
        }
    }

    public AwsWrapperConnection(DbConnection connection) : this(connection.GetType(), connection.ConnectionString)
    {
        Debug.Assert(connection != null);
        this._pluginService!.SetCurrentConnection(connection, this._pluginService.InitialConnectionHostSpec);
        this._database = connection.Database;
    }

    public AwsWrapperConnection(string connectionString) : this(null, connectionString) { }

    public AwsWrapperConnection(Type? targetType, string connectionString) : base()
    {
        this._connectionString = connectionString;
        this._props = ConnectionPropertiesUtils.ParseConnectionStringParameters(this._connectionString);
        this._targetType = targetType ?? this.GetTargetType(this._props);
        this._props.Add(PropertyDefinition.TargetConnectionType.Name, this._targetType.AssemblyQualifiedName!);

        ITargetConnectionDialect connectionDialect = TargetConnectionDialectProvider.GetDialect(this._targetType, this._props);

        DbConnectionProvider connectionProvider = new();

        this._pluginManager = new(connectionProvider, null, this);

        PluginService pluginService = new(this._targetType, this._pluginManager, this._props, this._connectionString, connectionDialect);

        this._pluginService = pluginService;
        this._hostListProviderService = pluginService;

        this._pluginManager.InitConnectionPluginChain(this._pluginService, this._props);

        this._pluginService.SetCurrentConnection(
            connectionProvider.CreateDbConnection(
                this._pluginService.Dialect,
                this._pluginService.TargetConnectionDialect,
                null,
                this._props),
            this._pluginService.InitialConnectionHostSpec);
    }

    // for testing purpose only
    public AwsWrapperConnection() : base() { }

    public override string Database => this._pluginService!.CurrentConnection?.Database ?? this._database ?? string.Empty;

    public override string DataSource => this._pluginService!.CurrentConnection is DbConnection dbConnection
        ? dbConnection.DataSource
        : string.Empty;

    public override string ServerVersion => this._pluginService!.CurrentConnection is DbConnection dbConnection
        ? dbConnection.ServerVersion
        : string.Empty;

    public override ConnectionState State => this._pluginService!.CurrentConnection?.State ?? ConnectionState.Closed;

    public override void ChangeDatabase(string databaseName)
    {
        this._database = databaseName;
        this._pluginService!.CurrentConnection?.ChangeDatabase(databaseName);
    }

    public override void Close()
    {
        WrapperUtils.RunWithPlugins(
            this._pluginManager!,
            this._pluginService!.CurrentConnection!,
            "DbConnection.Close",
            () => this._pluginService.CurrentConnection!.Close(),
            []);
    }

    public override void Open()
    {
        if (this.State != ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection is already open.");
        }

        ArgumentNullException.ThrowIfNull(this._pluginService);
        ArgumentNullException.ThrowIfNull(this._pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this._pluginManager);
        ArgumentNullException.ThrowIfNull(this._hostListProviderService);

        this._pluginManager.InitHostProvider(this._connectionString!, this._props!, this._hostListProviderService);
        this._pluginService.RefreshHostList();

        WrapperUtils.OpenWithPlugins(
            this._pluginManager,
            this._pluginService.InitialConnectionHostSpec,
            this._props!,
            true,
            null,
            () => this._pluginService.CurrentConnection!.Open());
    }

    // TODO: switch method to use pluginService
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        DbTransaction result = this._pluginService!.CurrentConnection!.BeginTransaction(isolationLevel);
        Console.WriteLine("AwsWrapperConnection.BeginDbTransaction()");
        return result;
    }

    protected override DbCommand CreateDbCommand() => this.CreateCommand();

    public new AwsWrapperCommand CreateCommand()
    {
        ArgumentNullException.ThrowIfNull(this._pluginService);
        ArgumentNullException.ThrowIfNull(this._pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this._pluginManager);

        DbCommand command = WrapperUtils.ExecuteWithPlugins<DbCommand>(
            this._pluginManager,
            this._pluginService.CurrentConnection,
            "DbConnection.CreateCommand",
            () => this._pluginService.CurrentConnection.CreateCommand(),
            []);

        var result = new AwsWrapperCommand(command, this, this._pluginManager);
        Console.WriteLine("AwsWrapperConnection.CreateCommand()");
        return result;
    }

    public AwsWrapperCommand<TCommand> CreateCommand<TCommand>() where TCommand : DbCommand
    {
        ArgumentNullException.ThrowIfNull(this._pluginService);
        ArgumentNullException.ThrowIfNull(this._pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this._pluginManager);

        TCommand command = WrapperUtils.ExecuteWithPlugins<TCommand>(
            this._pluginManager,
            this._pluginService.CurrentConnection,
            "DbConnection.CreateCommand",
            () => (TCommand)this._pluginService.CurrentConnection.CreateCommand(),
            []);
        return new AwsWrapperCommand<TCommand>(command, this, this._pluginManager);
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
                    throw new Exception("Can't load target connection type " + targetConnectionTypeString);
                }

                return targetType;
            }
            catch
            {
                throw new Exception("Can't load target connection type " + targetConnectionTypeString);
            }
        }

        throw new Exception($"Can't load target connection type {targetConnectionTypeString}");
    }
}

public class AwsWrapperConnection<TConn> : AwsWrapperConnection where TConn : DbConnection
{
    public AwsWrapperConnection(string connectionString) : base(typeof(TConn), connectionString)
    {
    }

    public new TConn? TargetConnection => this._pluginService?.CurrentConnection as TConn;
}
