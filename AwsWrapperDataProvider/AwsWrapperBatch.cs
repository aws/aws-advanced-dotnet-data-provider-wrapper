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
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperBatch : DbBatch
{
    protected DbBatch targetBatch;
    protected DbConnection? targetConnection;
    protected DbTransaction? targetTransaction;

    private readonly ConnectionPluginManager connectionPluginManager;

    public AwsWrapperBatch(DbBatch targetBatch, ConnectionPluginManager connectionPluginManager)
    {
        this.targetBatch = targetBatch;
        this.connectionPluginManager = connectionPluginManager;
    }

    internal DbBatch TargetDbBatch => this.targetBatch;

    protected override DbBatchCommandCollection DbBatchCommands => WrapperUtils.ExecuteWithPlugins<DbBatchCommandCollection>(
        this.connectionPluginManager,
        this.targetBatch,
        "DbBatch.BatchCommands",
        () => this.targetBatch.BatchCommands);

    public override int Timeout
    {
        get => WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Timeout",
            () => this.targetBatch.Timeout);
        set => WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Timeout",
            () => this.targetBatch.Timeout = value);
    }

    protected override DbConnection? DbConnection
    {
        get => WrapperUtils.ExecuteWithPlugins<DbConnection?>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Connection",
            () => this.targetBatch.Connection);
        set => WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Connection",
            () => this.targetBatch.Connection = value);
    }

    protected override DbTransaction? DbTransaction
    {
        get => WrapperUtils.ExecuteWithPlugins<DbTransaction?>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Transaction",
            () => this.targetBatch.Transaction);
        set => WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Transaction",
            () => this.targetBatch.Transaction = value);
    }

    protected override DbBatchCommand CreateDbBatchCommand()
    {
        DbBatchCommand batchCommand = WrapperUtils.ExecuteWithPlugins<DbBatchCommand>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.CreateBatchCommand",
            () => this.targetBatch.CreateBatchCommand());

        return batchCommand;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => this.ExecuteReader(behavior);

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        AwsWrapperDataReader dataReader = await this.ExecuteReaderAsync(behavior, cancellationToken);
        return (DbDataReader)dataReader;
    }

    public new AwsWrapperDataReader ExecuteReader(CommandBehavior behavior)
    {
        DbDataReader dataReader = WrapperUtils.ExecuteWithPlugins<DbDataReader>(
                this.connectionPluginManager,
                this.targetBatch,
                "DbBatch.ExecuteReader",
                () => this.targetBatch.ExecuteReader(behavior));

        return new AwsWrapperDataReader(dataReader, this.connectionPluginManager);
    }

    public new async Task<AwsWrapperDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        DbDataReader dataReader = await WrapperUtils.ExecuteWithPlugins<Task<DbDataReader>>(
                this.connectionPluginManager,
                this.targetBatch,
                "DbBatch.ExecuteReaderAsync",
                () => this.targetBatch.ExecuteReaderAsync(behavior, cancellationToken));

        return new AwsWrapperDataReader(dataReader, this.connectionPluginManager);
    }

    public override int ExecuteNonQuery()
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.ExecuteNonQuery",
            () => this.targetBatch.ExecuteNonQuery());
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        return await WrapperUtils.ExecuteWithPlugins<Task<int>>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.ExecuteNonQueryAsync",
            () => this.targetBatch.ExecuteNonQueryAsync(cancellationToken));
    }

    public override object? ExecuteScalar()
    {
        return WrapperUtils.ExecuteWithPlugins<object?>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.ExecuteScalar",
            () => this.targetBatch.ExecuteScalar());
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        return await WrapperUtils.ExecuteWithPlugins<Task<object?>>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.ExecuteScalarAsync",
            () => this.targetBatch.ExecuteScalarAsync(cancellationToken));
    }

    public override void Prepare()
    {
        WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Prepare",
            () => this.targetBatch.Prepare());
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        return WrapperUtils.ExecuteWithPlugins<Task>(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.PrepareAsync",
            () => this.targetBatch.PrepareAsync(cancellationToken));
    }

    public override void Cancel()
    {
        WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetBatch,
            "DbBatch.Cancel",
            () => this.targetBatch.Cancel());
    }
}
