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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperCommand : DbCommand
{
    protected Type? _targetDbCommandType;
    protected DbConnection? _targetDbConnection;
    protected AwsWrapperConnection? _wrapperConnection;
    protected DbCommand? _targetDbCommand;
    protected string? _commandText;
    protected int? _commandTimeout;
    protected AwsWrapperTransaction? wrapperTransaction;
    protected ConnectionPluginManager? _pluginManager;

    public AwsWrapperCommand()
    {
    }

    internal AwsWrapperCommand(DbCommand command, AwsWrapperConnection connection, ConnectionPluginManager pluginManager)
    {
        this._targetDbCommand = command;
        this._targetDbCommandType = this._targetDbCommand.GetType();
        this._wrapperConnection = connection;
        this._targetDbConnection = connection.TargetDbConnection;
        this._pluginManager = pluginManager;
    }

    public AwsWrapperCommand(DbCommand command, DbConnection? connection)
    {
        this._targetDbCommand = command;
        this._targetDbCommandType = this._targetDbCommand.GetType();
        this._targetDbConnection = connection;
        if (connection is AwsWrapperConnection awsWrapperConnection)
        {
            this._wrapperConnection = awsWrapperConnection;
            this._targetDbConnection = awsWrapperConnection.TargetDbConnection;
            this._pluginManager = awsWrapperConnection.PluginManager;
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
            this._targetDbConnection = connection.TargetDbConnection;
            this._pluginManager = connection.PluginManager;
            this.EnsureTargetDbCommandCreated();
            this._targetDbCommand!.Connection = this._targetDbConnection;
        }
    }

    [AllowNull]
    public override string CommandText
    {
        get => this._commandText ?? string.Empty;
        set
        {
            this._commandText = value;
            if (this._targetDbCommand != null)
            {
                this._targetDbCommand.CommandText = value ?? string.Empty;
            }
        }
    }

    public override int CommandTimeout
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this._targetDbCommand!.CommandTimeout;
        }

        set
        {
            this.EnsureTargetDbCommandCreated();
            this._targetDbCommand!.CommandTimeout = value;
        }
    }

    public override CommandType CommandType
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this._targetDbCommand!.CommandType;
        }

        set
        {
            this.EnsureTargetDbCommandCreated();
            this._targetDbCommand!.CommandType = value;
        }
    }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource
    {
        get
        {
            this.EnsureTargetDbCommandCreated();
            return this._targetDbCommand!.UpdatedRowSource;
        }

        set
        {
            this.EnsureTargetDbCommandCreated();
            this._targetDbCommand!.UpdatedRowSource = value;
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
                this._targetDbConnection = null;
                this._pluginManager = null;
                return;
            }

            if (!IsTypeAwsWrapperConnection(value.GetType()))
            {
                throw new InvalidOperationException("Provided connection is not of type AwsWrapperConnection.");
            }

            this._wrapperConnection = (AwsWrapperConnection)value;
            this._targetDbConnection = this._wrapperConnection.TargetDbConnection;
            this._pluginManager = this._wrapperConnection.PluginManager;
            if (this._targetDbCommand != null)
            {
                this._targetDbCommand.Connection = this._wrapperConnection?.TargetDbConnection;
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
            return this._targetDbCommand!.Parameters;
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
                this._targetDbCommand!.Transaction = null;
                return;
            }

            if (value is not AwsWrapperTransaction)
            {
                throw new InvalidOperationException("Provided DbTransaction is not of type AwsWrapperTransaction.");
            }

            this.wrapperTransaction = (AwsWrapperTransaction)value;
            this.EnsureTargetDbCommandCreated();
            this._targetDbCommand!.Transaction = this.wrapperTransaction.TargetDbTransaction;
        }
    }

    public override void Cancel()
    {
        this.EnsureTargetDbCommandCreated();
        WrapperUtils.RunWithPlugins(
            this._pluginManager!,
            this._targetDbCommand!,
            "DbCommand.Cancel",
            () => this._targetDbCommand!.Cancel());
    }

    public override int ExecuteNonQuery()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this._targetDbCommand!,
            "DbCommand.ExecuteNonQuery",
            () => this._targetDbCommand!.ExecuteNonQuery());
    }

    public override object? ExecuteScalar()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this._targetDbCommand!,
            "DbCommand.ExecuteScalar",
            () => this._targetDbCommand!.ExecuteScalar());
    }

    public override void Prepare()
    {
        this.EnsureTargetDbCommandCreated();
        WrapperUtils.RunWithPlugins(
            this._pluginManager!,
            this._targetDbCommand!,
            "DbCommand.Prepare",
            () => this._targetDbCommand!.Prepare());
    }

    protected override DbParameter CreateDbParameter()
    {
        this.EnsureTargetDbCommandCreated();
        return WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this._targetDbCommand!,
            "DbCommand.CreateDbParameter",
            () => this._targetDbCommand!.CreateParameter());
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        this.EnsureTargetDbCommandCreated();
        DbDataReader reader = WrapperUtils.ExecuteWithPlugins(
            this._pluginManager!,
            this._targetDbCommand!,
            "DbCommand.ExecuteWrapperReader",
            () => this._targetDbCommand!.ExecuteReader(behavior));

        return new AwsWrapperDataReader(reader, this._pluginManager!);
    }

    protected void EnsureTargetDbCommandCreated()
    {
        if (this._targetDbCommand == null)
        {
            if (this._targetDbCommandType != null)
            {
                this._targetDbCommand = Activator.CreateInstance(this._targetDbCommandType) as DbCommand;
                if (this._targetDbCommand == null)
                {
                    throw new Exception("Provided type doesn't implement IDbCommand.");
                }
            }
            else if (this._targetDbConnection != null)
            {
                this._targetDbCommand = this._targetDbConnection.CreateCommand();
                if (this._targetDbCommandType == null)
                {
                    this._targetDbCommandType = this._targetDbCommand?.GetType();
                }
            }

            if (this._targetDbConnection != null)
            {
                this._targetDbCommand!.Connection = this._targetDbConnection;
            }

            if (this._commandText != null)
            {
                this._targetDbCommand!.CommandText = this._commandText;
            }

            if (this._commandTimeout != null)
            {
                this._targetDbCommand!.CommandTimeout = this._commandTimeout.Value;
            }

            if (this.wrapperTransaction != null)
            {
                this._targetDbCommand!.Transaction = this.wrapperTransaction.TargetDbTransaction;
            }
        }
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
