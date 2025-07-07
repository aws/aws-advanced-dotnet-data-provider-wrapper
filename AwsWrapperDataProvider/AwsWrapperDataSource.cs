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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperDataSource : DbDataSource
{
    protected DbDataSource targetDataSource;

    private readonly ConnectionPluginManager connectionPluginManager;

    internal AwsWrapperDataSource(DbDataSource targetDataSource, ConnectionPluginManager connectionPluginManager)
    {
        this.targetDataSource = targetDataSource;
        this.connectionPluginManager = connectionPluginManager;
    }

    internal DbDataSource TargetDbDataSource => this.targetDataSource;

    public override string ConnectionString => WrapperUtils.ExecuteWithPlugins(
        this.connectionPluginManager,
        this.targetDataSource,
        "DbDataSource.ConnectionString",
        () => this.targetDataSource.ConnectionString);

    protected override DbBatch CreateDbBatch()
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataSource,
            "DbDataSource.CreateDbBatch",
            () => this.targetDataSource.CreateBatch());
    }

    protected override DbCommand CreateDbCommand(string? commandText = default)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataSource,
            "DbDataSource.CreateDbCommand",
            () => this.targetDataSource.CreateCommand(commandText));
    }

    protected override DbConnection CreateDbConnection()
    {
        return new AwsWrapperConnection(WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataSource,
            "DbDataSource.CreateDbConnection",
            () => this.targetDataSource.CreateConnection()));
    }

    protected override ValueTask DisposeAsyncCore()
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataSource,
            "DbDataSource.DisposeAsyncCore",
            () => this.targetDataSource.DisposeAsync());
    }

    protected override DbConnection OpenDbConnection()
    {
        return new AwsWrapperConnection(WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataSource,
            "DbDataSource.OpenDbConnection",
            () => this.targetDataSource.OpenConnection()));
    }

    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
    {
        DbConnection conn = await WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataSource,
            "DbDataSource.OpenDbConnectionAsync",
            () => this.targetDataSource.OpenConnectionAsync(cancellationToken));

        return new AwsWrapperConnection(conn);
    }
}
