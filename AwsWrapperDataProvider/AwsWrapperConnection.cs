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
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperConnection : DbConnection
{
    protected readonly IPluginService pluginService;
    private readonly IHostListProviderService hostListProviderService;
    private Type targetType;
    private string connectionString;
    private string? database;

    internal ConnectionPluginManager PluginManager { get; }

    internal Dictionary<string, string> ConnectionProperties { get; private set; }

    internal DbConnection? TargetDbConnection => this.pluginService?.CurrentConnection;

    [AllowNull]
    public override string ConnectionString
    {
        get => this.pluginService!.CurrentConnection?.ConnectionString ?? this.connectionString ?? string.Empty;
        set
        {
            this.connectionString = value ?? string.Empty;
            this.ConnectionProperties = ConnectionPropertiesUtils.ParseConnectionStringParameters(this.connectionString);
            this.targetType = this.GetTargetType(this.ConnectionProperties);

            if (this.pluginService?.CurrentConnection != null)
            {
                this.pluginService.CurrentConnection.ConnectionString = value;
            }
        }
    }

    public AwsWrapperConnection(DbConnection connection) : this(connection.GetType(), connection.ConnectionString)
    {
        Debug.Assert(connection != null);
        this.pluginService!.SetCurrentConnection(connection, this.pluginService.InitialConnectionHostSpec);
        this.database = connection.Database;
    }

    public AwsWrapperConnection(string connectionString) : this(null, connectionString) { }

    public AwsWrapperConnection(Type? targetType, string connectionString) : base()
    {
        this.connectionString = connectionString;
        this.ConnectionProperties = ConnectionPropertiesUtils.ParseConnectionStringParameters(this.connectionString);
        this.targetType = targetType ?? this.GetTargetType(this.ConnectionProperties);
        this.ConnectionProperties[PropertyDefinition.TargetConnectionType.Name] = this.targetType.AssemblyQualifiedName!;

        ITargetConnectionDialect connectionDialect = TargetConnectionDialectProvider.GetDialect(this.targetType, this.ConnectionProperties);

        DbConnectionProvider connectionProvider = new();

        this.PluginManager = new(connectionProvider, null, this);

        PluginService pluginService = new(this.targetType, this.PluginManager, this.ConnectionProperties, this.connectionString, connectionDialect);

        this.pluginService = pluginService;
        this.hostListProviderService = pluginService;

        this.PluginManager.InitConnectionPluginChain(this.pluginService, this.ConnectionProperties);

        this.pluginService.RefreshHostList();
        this.pluginService.SetCurrentConnection(
            connectionProvider.CreateDbConnection(
                this.pluginService.Dialect,
                this.pluginService.TargetConnectionDialect,
                null,
                this.ConnectionProperties),
            this.pluginService.InitialConnectionHostSpec);
    }

    public override string Database => this.pluginService!.CurrentConnection?.Database ?? this.database ?? string.Empty;

    public override string DataSource => this.pluginService!.CurrentConnection is DbConnection dbConnection
        ? dbConnection.DataSource
        : string.Empty;

    public override string ServerVersion => this.pluginService!.CurrentConnection is DbConnection dbConnection
        ? dbConnection.ServerVersion
        : string.Empty;

    public override ConnectionState State => this.pluginService!.CurrentConnection?.State ?? ConnectionState.Closed;

    public override void ChangeDatabase(string databaseName)
    {
        this.database = databaseName;
        WrapperUtils.RunWithPlugins(
            this.PluginManager!,
            this.pluginService!.CurrentConnection!,
            "DbConnection.ChangeDatabase",
            () => this.pluginService.CurrentConnection!.ChangeDatabase(databaseName),
            databaseName);
    }

    public override void Close()
    {
        WrapperUtils.RunWithPlugins(
            this.PluginManager!,
            this.pluginService!.CurrentConnection!,
            "DbConnection.Close",
            () => this.pluginService.CurrentConnection!.Close());
    }

    public override void Open()
    {
        if (this.State != ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection is already open.");
        }

        ArgumentNullException.ThrowIfNull(this.pluginService);
        ArgumentNullException.ThrowIfNull(this.pluginService.CurrentConnection);
        ArgumentNullException.ThrowIfNull(this.PluginManager);
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);

        this.PluginManager.InitHostProvider(this.connectionString!, this.ConnectionProperties!, this.hostListProviderService);

        WrapperUtils.OpenWithPlugins(
            this.PluginManager,
            this.pluginService.InitialConnectionHostSpec,
            this.ConnectionProperties!,
            true,
            () => this.pluginService.CurrentConnection!.Open());
        this.pluginService.RefreshHostList();
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
            () => this.pluginService.CurrentConnection!.BeginTransaction(isolationLevel),
            isolationLevel);

        return new AwsWrapperTransaction(this, targetTransaction, this.PluginManager);
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
            () => (TCommand)this.pluginService.CurrentConnection.CreateCommand());

        this.ConnectionProperties[PropertyDefinition.TargetCommandType.Name] = typeof(TCommand).AssemblyQualifiedName!;
        return new AwsWrapperCommand<TCommand>(command, this, this.PluginManager);
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

    public new TConn? TargetDbConnection => this.pluginService?.CurrentConnection as TConn;
}
