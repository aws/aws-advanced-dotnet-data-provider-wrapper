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
    protected Type? _targetDbCommandType;
    internal DbConnection? TargetDbConnection;
    protected AwsWrapperConnection? _wrapperConnection;
    internal DbCommand? TargetDbCommand;
    protected string? _commandText;
    protected int? _commandTimeout;
    protected AwsWrapperTransaction? wrapperTransaction;
    protected ConnectionPluginManager? _pluginManager;

    public AwsWrapperCommand()
    {
    }

    internal AwsWrapperCommand(DbCommand command, AwsWrapperConnection connection, ConnectionPluginManager pluginManager)
    {
        this.TargetDbCommand = command;
        this._targetDbCommandType = this.TargetDbCommand.GetType();
        this._wrapperConnection = connection;
        this.TargetDbConnection = connection.TargetDbConnection;
        this._pluginManager = pluginManager;
    }

    public AwsWrapperCommand(DbCommand command, DbConnection? connection)
    {
        this.TargetDbCommand = command;
        this._targetDbCommandType = this.TargetDbCommand.GetType();
        this.TargetDbConnection = connection;
        if (connection is AwsWrapperConnection awsWrapperConnection)
        {
            this._wrapperConnection = awsWrapperConnection;
            this.TargetDbConnection = awsWrapperConnection.TargetDbConnection;
            this._pluginManager = awsWrapperConnection.PluginManager;
        }
        else
        {
            throw new InvalidOperationException(Properties.Resources.Error_NotAwsWrapperConnection);
        }
    }

    public AwsWrapperCommand(DbCommand command) : this(command, null) { }

    public AwsWrapperCommand(Type targetDbCommandType, string? commandText) : this(targetDbCommandType, commandText, null) { }

    public AwsWrapperCommand(Type targetDbCommandType) : this(targetDbCommandType, null, null) { }

    public AwsWrapperCommand(Type targetDbCommandType, AwsWrapperConnection connection) : this(targetDbCommandType, null, connection) { }

    public AwsWrapperCommand(Type targetDbCommandType, string? commandText, AwsWrapperConnection? connection)
    {
        this._targetDbCommandType = targetDbCommandType;
        this._commandText = commandText;
        if (connection != null)
        {
            this._wrapperConnection = connection;
            this.TargetDbConnection = connection.TargetDbConnection;
            this._pluginManager = connection.PluginManager;
            this.EnsureTargetDbCommandCreated();
            this.TargetDbCommand!.Connection = this.TargetDbConnection;
        }
    }

    [AllowNull]
    public override string CommandText
    {
        get => this._commandText ?? string.Empty;
        set
        {
            this._commandText = value;
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
        get => this._wrapperConnection;
        set
        {
            if (value == null)
            {
                this._wrapperConnection = null;
                this.TargetDbConnection = null;
                this._pluginManager = null;
                return;
            }

            if (!IsTypeAwsWrapperConnection(value.GetType()))
            {
                throw new InvalidOperationException("Provided connection is not of type AwsWrapperConnection.");
            }

            this._wrapperConnection = (AwsWrapperConnection)value;
            this.TargetDbConnection = this._wrapperConnection.TargetDbConnection;
            this._pluginManager = this._wrapperConnection.PluginManager;
            if (this.TargetDbCommand != null)
            {
                this.TargetDbCommand.Connection = this._wrapperConnection?.TargetDbConnection;
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
            this._pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.Cancel",
            () => this.TargetDbCommand!.Cancel());
    }

    public override int ExecuteNonQuery()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteNonQuery",
            () => this.TargetDbCommand!.ExecuteNonQuery());
    }

    public override object? ExecuteScalar()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteScalar",
            () => this.TargetDbCommand!.ExecuteScalar());
    }

    public override void Prepare()
    {
        this.EnsureTargetDbCommandCreated();
        WrapperUtils.RunWithPlugins(
            this._pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.Prepare",
            () => this.TargetDbCommand!.Prepare());
    }

    protected override DbParameter CreateDbParameter()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.CreateParameter",
            () => this.TargetDbCommand!.CreateParameter());
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        this.EnsureTargetDbCommandCreated();
        DbDataReader reader = WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this.TargetDbCommand!,
            "DbCommand.ExecuteReader",
            () => this.TargetDbCommand!.ExecuteReader(behavior));
        return new AwsWrapperDataReader(reader, this._pluginManager!);
    }

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            this.EnsureTargetDbCommandCreated();
            DbDataReader reader = WrapperUtils.ExecuteWithPlugins(
                this._pluginManager!,
                this.TargetDbCommand!,
                "DbCommand.ExecuteReaderAsync",
                () => this.TargetDbCommand!.ExecuteReaderAsync(behavior, cancellationToken).GetAwaiter().GetResult());
            return (DbDataReader)new AwsWrapperDataReader(reader, this._pluginManager!);
        });
    }

    protected void EnsureTargetDbCommandCreated()
    {
        if (this.TargetDbCommand == null)
        {
            if (this._targetDbCommandType != null)
            {
                this.TargetDbCommand = Activator.CreateInstance(this._targetDbCommandType) as DbCommand;
                if (this.TargetDbCommand == null)
                {
                    throw new Exception("Provided type doesn't implement IDbCommand.");
                }
            }
            else if (this.TargetDbConnection != null)
            {
                this.TargetDbCommand = this.TargetDbConnection.CreateCommand();
                if (this._targetDbCommandType == null)
                {
                    this._targetDbCommandType = this.TargetDbCommand?.GetType();
                }
            }

            if (this.TargetDbConnection != null)
            {
                this.TargetDbCommand!.Connection = this.TargetDbConnection;
            }

            if (this._commandText != null)
            {
                this.TargetDbCommand!.CommandText = this._commandText;
            }

            if (this._commandTimeout != null)
            {
                this.TargetDbCommand!.CommandTimeout = this._commandTimeout.Value;
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

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            this._wrapperConnection?.UnregisterWrapperCommand(this);
            this.TargetDbCommand?.Dispose();
        }

        base.Dispose(disposing);
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
