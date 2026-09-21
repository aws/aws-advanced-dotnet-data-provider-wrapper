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
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider;

public class AwsWrapperBatch : DbBatch, IWrapper
{
    private static readonly ILogger<AwsWrapperBatch> Logger = LoggerUtils.GetLogger<AwsWrapperBatch>();

    protected DbBatch targetBatch;
    protected AwsWrapperConnection? wrapperConnection;
    protected AwsWrapperTransaction? wrapperTransaction;
    private ConnectionPluginManager? connectionPluginManager;

    internal AwsWrapperBatch(DbBatch targetBatch, AwsWrapperConnection connection, ConnectionPluginManager connectionPluginManager)
    {
        this.targetBatch = targetBatch;
        this.wrapperConnection = connection;
        this.connectionPluginManager = connectionPluginManager;
        connection.RegisterWrapperBatch(this);
    }

    internal AwsWrapperBatch(DbBatch targetBatch, DbConnection? connection)
    {
        this.targetBatch = targetBatch;
        connection ??= this.targetBatch.Connection;

        if (connection is AwsWrapperConnection awsWrapperConnection)
        {
            this.wrapperConnection = awsWrapperConnection;
            this.connectionPluginManager = awsWrapperConnection.PluginManager;
            awsWrapperConnection.RegisterWrapperBatch(this);
        }
        else
        {
            throw new InvalidOperationException(Properties.Resources.Error_NotAwsWrapperConnection);
        }
    }

    internal DbBatch TargetDbBatch => this.targetBatch;

    protected override DbBatchCommandCollection DbBatchCommands => this.targetBatch.BatchCommands;

    public override int Timeout
    {
        get => this.targetBatch.Timeout;
        set => this.targetBatch.Timeout = value;
    }

    protected override DbConnection? DbConnection
    {
        get => this.wrapperConnection;
        set
        {
            if (value == null)
            {
                this.wrapperConnection?.UnregisterWrapperBatch(this);
                this.wrapperConnection = null;
                this.targetBatch.Connection = null;
                this.connectionPluginManager = null;
                return;
            }

            if (value is not AwsWrapperConnection newConnection)
            {
                throw new InvalidOperationException(Resources.Error_ProvidedConnectionNotAwsWrapperConnection);
            }

            if (!ReferenceEquals(newConnection, this.wrapperConnection))
            {
                this.wrapperConnection?.UnregisterWrapperBatch(this);
                newConnection.RegisterWrapperBatch(this);
            }

            this.wrapperConnection = newConnection;
            this.connectionPluginManager = newConnection.PluginManager;
            this.targetBatch.Connection = newConnection.TargetDbConnection;
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
                this.targetBatch.Transaction = null;
                return;
            }

            if (value is not AwsWrapperTransaction newTransaction)
            {
                throw new InvalidOperationException(Resources.Error_ProvidedTransactionNotAwsWrapperTransaction);
            }

            this.wrapperTransaction = newTransaction;
            this.targetBatch.Transaction = newTransaction.TargetDbTransaction;
        }
    }

    /// <summary>
    /// Re-points this batch at <paramref name="connection"/> after a plugin switched the current
    /// connection, so that a batch the application already built still executes.
    /// </summary>
    internal void SetCurrentConnection(DbConnection? connection)
    {
        Logger.LogTrace(Resources.AwsWrapperBatch_SetCurrentConnection_TargetConnectionUpdating,
            connection?.GetType().FullName,
            RuntimeHelpers.GetHashCode(connection),
            RuntimeHelpers.GetHashCode(this.targetBatch.Connection),
            RuntimeHelpers.GetHashCode(this));
        this.targetBatch.Connection = connection;
    }

    protected override DbBatchCommand CreateDbBatchCommand()
    {
        DbBatchCommand batchCommand = WrapperUtils.ExecuteWithPlugins<DbBatchCommand>(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.CreateBatchCommand",
            () => Task.FromResult(this.targetBatch.CreateBatchCommand()))
            .GetAwaiter().GetResult();

        return batchCommand;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => this.ExecuteReader(behavior);

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        AwsWrapperDataReader dataReader = await this.ExecuteReaderAsync(behavior, cancellationToken);
        return dataReader;
    }

    public virtual new AwsWrapperDataReader ExecuteReader(CommandBehavior behavior)
    {
        DbDataReader dataReader = WrapperUtils.ExecuteWithPlugins<DbDataReader>(
                this.connectionPluginManager!,
                this.targetBatch,
                "DbBatch.ExecuteReader",
                () => Task.FromResult(this.targetBatch.ExecuteReader(behavior))).GetAwaiter().GetResult();

        return new AwsWrapperDataReader(dataReader, this.connectionPluginManager!);
    }

    public new async Task<AwsWrapperDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        DbDataReader dataReader = await WrapperUtils.ExecuteWithPlugins(
                this.connectionPluginManager!,
                this.targetBatch,
                "DbBatch.ExecuteReaderAsync",
                () => this.targetBatch.ExecuteReaderAsync(behavior, cancellationToken));

        return new AwsWrapperDataReader(dataReader, this.connectionPluginManager!);
    }

    public override int ExecuteNonQuery()
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.ExecuteNonQuery",
            () => Task.FromResult(this.targetBatch.ExecuteNonQuery())).GetAwaiter().GetResult();
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.ExecuteNonQueryAsync",
            () => this.targetBatch.ExecuteNonQueryAsync(cancellationToken),
            cancellationToken);
    }

    public override object? ExecuteScalar()
    {
        return WrapperUtils.ExecuteWithPlugins<object?>(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.ExecuteScalar",
            () => Task.FromResult(this.targetBatch.ExecuteScalar())).GetAwaiter().GetResult();
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.ExecuteScalarAsync",
            () => this.targetBatch.ExecuteScalarAsync(cancellationToken),
            cancellationToken);
    }

    public override void Prepare()
    {
        WrapperUtils.RunWithPlugins(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.Prepare",
            () =>
            {
                this.targetBatch.Prepare();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        return WrapperUtils.RunWithPlugins(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.PrepareAsync",
            () => this.targetBatch.PrepareAsync(cancellationToken));
    }

    public override void Cancel()
    {
        WrapperUtils.RunWithPlugins(
            this.connectionPluginManager!,
            this.targetBatch,
            "DbBatch.Cancel",
            () =>
            {
                this.targetBatch.Cancel();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
    }

    public override void Dispose()
    {
        this.wrapperConnection?.UnregisterWrapperBatch(this);
        this.targetBatch?.Dispose();
    }

    public override async ValueTask DisposeAsync()
    {
        this.wrapperConnection?.UnregisterWrapperBatch(this);
        if (this.targetBatch is not null)
        {
            await this.targetBatch.DisposeAsync().ConfigureAwait(false);
        }
    }

    public T Unwrap<T>() where T : class
    {
        if (this.targetBatch is T batchAsT)
        {
            return batchAsT;
        }

        throw new ArgumentException(string.Format(Resources.Error_CannotUnwrap, typeof(AwsWrapperBatch).Name, typeof(T).Name));
    }

    public bool IsWrapperFor<T>() where T : class
    {
        return this.targetBatch is T;
    }
}
