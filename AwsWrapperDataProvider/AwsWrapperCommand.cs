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
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider;

public class AwsWrapperCommand : DbCommand
{
    private static readonly ILogger<AwsWrapperCommand> Logger = LoggerUtils.GetLogger<AwsWrapperCommand>();

    internal DbCommand? TargetDbCommand;
    internal DbConnection? TargetDbConnection;

    protected Type? targetDbCommandType;
    protected AwsWrapperConnection? wrapperConnection;
    protected string? commandText;
    protected int? commandTimeout;
    protected AwsWrapperTransaction? wrapperTransaction;
    protected ConnectionPluginManager? pluginManager;

    public AwsWrapperCommand()
    {
    }

    internal AwsWrapperCommand(DbCommand command, AwsWrapperConnection connection, ConnectionPluginManager pluginManager)
    {
        this.TargetDbCommand = command;
        this.targetDbCommandType = this.TargetDbCommand.GetType();
        this.wrapperConnection = connection;
        this.TargetDbConnection = connection.TargetDbConnection;
        this.pluginManager = pluginManager;
    }

    public AwsWrapperCommand(DbCommand command, DbConnection? connection)
    {
        this.TargetDbCommand = command;
        this.targetDbCommandType = this.TargetDbCommand.GetType();
        this.TargetDbConnection = connection;
        if (connection is AwsWrapperConnection awsWrapperConnection)
        {
            this.wrapperConnection = awsWrapperConnection;
            this.TargetDbConnection = awsWrapperConnection.TargetDbConnection;
            this.pluginManager = awsWrapperConnection.PluginManager;
        }
    }

    public AwsWrapperCommand(DbCommand command) : this(command, null) { }

    public AwsWrapperCommand(Type targetDbCommandType, string? commandText) : this(targetDbCommandType, commandText, null) { }

    public AwsWrapperCommand(Type targetDbCommandType) : this(targetDbCommandType, null, null) { }

    public AwsWrapperCommand(Type targetDbCommandType, AwsWrapperConnection connection) : this(targetDbCommandType, null, connection) { }

    public AwsWrapperCommand(Type targetDbCommandType, string? commandText, AwsWrapperConnection? connection)
    {
        this.targetDbCommandType = targetDbCommandType;
        this.commandText = commandText;
        if (connection != null)
        {
            this.wrapperConnection = connection;
            this.TargetDbConnection = connection.TargetDbConnection;
            this.pluginManager = connection.PluginManager;
            this.EnsureTargetDbCommandCreated();
            this.TargetDbCommand!.Connection = this.TargetDbConnection;
        }
    }

    [AllowNull]
    public override string CommandText
    {
        get => this.commandText ?? string.Empty;
        set
        {
            this.commandText = value;
            if (this.TargetDbCommand != null)
            {
                this.TargetDbCommand.CommandText = value ?? string.Empty;
            }
        }
    }

    public override int CommandTimeout
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this.TargetDbCommand!.CommandTimeout;
        }

        set
        {
            this.EnsureTargetDbCommandCreated();
            this.TargetDbCommand!.CommandTimeout = value;
        }
    }

    public override CommandType CommandType
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this.TargetDbCommand!.CommandType;
        }

        set
        {
            this.EnsureTargetDbCommandCreated();
            this.TargetDbCommand!.CommandType = value;
        }
    }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this.TargetDbCommand!.UpdatedRowSource;
        }

        set
        {
            this.EnsureTargetDbCommandCreated();
            this.TargetDbCommand!.UpdatedRowSource = value;
        }
    }

    protected override DbConnection? DbConnection
    {
        get => this.wrapperConnection;
        set
        {
            if (value == null)
            {
                this.wrapperConnection = null;
                this.TargetDbConnection = null;
                this.pluginManager = null;
                return;
            }

            if (!IsTypeAwsWrapperConnection(value.GetType()))
            {
                throw new InvalidOperationException("Provided connection is not of type AwsWrapperConnection.");
            }

            this.wrapperConnection = (AwsWrapperConnection)value;
            this.TargetDbConnection = this.wrapperConnection.TargetDbConnection;
            this.pluginManager = this.wrapperConnection.PluginManager;
            if (this.TargetDbCommand != null)
            {
                this.TargetDbCommand.Connection = this.wrapperConnection?.TargetDbConnection;
            }
        }
    }

    protected static bool IsTypeAwsWrapperConnection(Type type)
    {
        // check if type is AwsWrapperConnection or AwsWrapperConnection<T>
        return type == typeof(AwsWrapperConnection) || (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(AwsWrapperConnection<>));
    }

    protected override DbParameterCollection DbParameterCollection
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this.TargetDbCommand!.Parameters;
        }
    }

    protected override DbTransaction? DbTransaction
    {
        get => this.wrapperTransaction;
        set
        {
            if (value == null)
            {
                this.wrapperTransaction = null;
                this.TargetDbCommand!.Transaction = null;
                return;
            }

            if (value is not AwsWrapperTransaction)
            {
                throw new InvalidOperationException("Provided DbTransaction is not of type AwsWrapperTransaction.");
            }

            this.wrapperTransaction = (AwsWrapperTransaction)value;
            this.EnsureTargetDbCommandCreated();
            this.TargetDbCommand!.Transaction = this.wrapperTransaction.TargetDbTransaction;
        }
    }

    public override void Cancel()
    {
        this.EnsureTargetDbCommandCreated();
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.Cancel",
            () =>
            {
                this.TargetDbCommand!.Cancel();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
    }

    public override int ExecuteNonQuery()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteNonQuery",
            () => Task.FromResult(this.TargetDbCommand!.ExecuteNonQuery())).GetAwaiter().GetResult();
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteNonQueryAsync",
            () => this.TargetDbCommand!.ExecuteNonQueryAsync(cancellationToken));
    }

    public override object? ExecuteScalar()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteScalar",
            () => Task.FromResult(this.TargetDbCommand!.ExecuteScalar())).GetAwaiter().GetResult();
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteScalarAsync",
            () => this.TargetDbCommand!.ExecuteScalarAsync(cancellationToken));
    }

    public override void Prepare()
    {
        this.EnsureTargetDbCommandCreated();
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.Prepare",
            () =>
            {
                this.TargetDbCommand!.Prepare();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.PrepareAsync",
            () => this.TargetDbCommand!.PrepareAsync(cancellationToken));
    }

    protected override DbParameter CreateDbParameter()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.CreateParameter",
            () => Task.FromResult(this.TargetDbCommand!.CreateParameter()))
            .GetAwaiter().GetResult();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        this.EnsureTargetDbCommandCreated();
        DbDataReader reader = WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteReader",
            () => Task.FromResult(this.TargetDbCommand!.ExecuteReader(behavior)))
            .GetAwaiter().GetResult();
        return new AwsWrapperDataReader(reader, this.pluginManager!);
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        this.EnsureTargetDbCommandCreated();
        DbDataReader reader = await WrapperUtils.ExecuteWithPlugins(
            this.pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteReaderAsync",
            () => this.TargetDbCommand!.ExecuteReaderAsync(behavior, cancellationToken));
        return new AwsWrapperDataReader(reader, this.pluginManager!);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            this.wrapperConnection?.UnregisterWrapperCommand(this);
            if (this.TargetDbCommand != null)
            {
                this.TargetDbCommand.Dispose();
                this.TargetDbCommand = null;
            }
        }
    }

    public override async ValueTask DisposeAsync()
    {
        this.wrapperConnection?.UnregisterWrapperCommand(this);
        if (this.TargetDbCommand is not null)
        {
            await this.TargetDbCommand.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected void EnsureTargetDbCommandCreated()
    {
        if (this.TargetDbCommand == null)
        {
            if (this.targetDbCommandType != null)
            {
                this.TargetDbCommand = Activator.CreateInstance(this.targetDbCommandType) as DbCommand;
                if (this.TargetDbCommand == null)
                {
                    throw new Exception("Provided type doesn't implement IDbCommand.");
                }
            }
            else if (this.TargetDbConnection != null)
            {
                this.TargetDbCommand = this.TargetDbConnection.CreateCommand();
                if (this.targetDbCommandType == null)
                {
                    this.targetDbCommandType = this.TargetDbCommand?.GetType();
                }
            }

            if (this.TargetDbConnection != null)
            {
                this.TargetDbCommand!.Connection = this.TargetDbConnection;
            }

            if (this.commandText != null)
            {
                this.TargetDbCommand!.CommandText = this.commandText;
            }

            if (this.commandTimeout != null)
            {
                this.TargetDbCommand!.CommandTimeout = this.commandTimeout.Value;
            }

            if (this.wrapperTransaction != null)
            {
                this.TargetDbCommand!.Transaction = this.wrapperTransaction.TargetDbTransaction;
            }
        }
    }

    internal void SetCurrentConnection(DbConnection? connection)
    {
        Logger.LogTrace("Target connection is updating to {Type}@{Id} from {Id2} for AwsWrapperCommand@{Id3}",
            connection?.GetType().FullName,
            RuntimeHelpers.GetHashCode(connection),
            RuntimeHelpers.GetHashCode(this.TargetDbConnection),
            RuntimeHelpers.GetHashCode(this));
        this.EnsureTargetDbCommandCreated();
        this.TargetDbConnection = connection;
        this.TargetDbCommand!.Connection = connection;
    }
}

public class AwsWrapperCommand<TCommand> : AwsWrapperCommand where TCommand : IDbCommand
{
    internal AwsWrapperCommand(
        DbCommand command,
        AwsWrapperConnection wrapperConnection,
        ConnectionPluginManager pluginManager) : base(command, wrapperConnection, pluginManager) { }

    internal AwsWrapperCommand(DbCommand command, AwsWrapperConnection wrapperConnection) : base(command, wrapperConnection) { }

    public AwsWrapperCommand() : base(typeof(TCommand)) { }

    public AwsWrapperCommand(string? commandText) : base(typeof(TCommand), commandText) { }

    public AwsWrapperCommand(AwsWrapperConnection wrapperConnection) : base(typeof(TCommand), wrapperConnection) { }

    public AwsWrapperCommand(string? commandText, AwsWrapperConnection wrapperConnection) : base(typeof(TCommand), commandText, wrapperConnection) { }
}
