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
    public override string ConnectionString { get; }

    private readonly Type? targetConnectionType;

    private AwsWrapperConnection? mostRecentConnection;

    public AwsWrapperDataSource(Type? targetConnectionType, string connectionString)
    {
        this.targetConnectionType = targetConnectionType;
        this.ConnectionString = connectionString;
    }

    protected override DbBatch CreateDbBatch() => this.CreateBatch();

    public new AwsWrapperBatch CreateBatch()
    {
        return this.mostRecentConnection!.CreateBatch();
    }

    protected override DbCommand CreateDbCommand(string? commandText = default) => this.CreateCommand<DbCommand>(commandText);

    public AwsWrapperCommand<TCommand> CreateCommand<TCommand>(string? commandText = default) where TCommand : DbCommand
    {
        AwsWrapperCommand<TCommand> command = this.mostRecentConnection!.CreateCommand<TCommand>();
        command.CommandText = commandText;
        return command;
    }

    protected override DbConnection CreateDbConnection() => this.CreateConnection();

    public new AwsWrapperConnection CreateConnection()
    {
        this.mostRecentConnection = new AwsWrapperConnection(this.targetConnectionType, this.ConnectionString);
        return this.mostRecentConnection;
    }

    protected override void Dispose(bool disposing)
    {
        this.mostRecentConnection = null;
    }

    protected override ValueTask DisposeAsyncCore()
    {
        this.mostRecentConnection = null;
        return ValueTask.CompletedTask;
    }

    protected override DbConnection OpenDbConnection() => this.OpenConnection();

    public new AwsWrapperConnection OpenConnection()
    {
        this.mostRecentConnection = this.CreateConnection();
        this.mostRecentConnection.Open();
        return this.mostRecentConnection;
    }

    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
    {
        AwsWrapperConnection connection = await this.OpenConnectionAsync(cancellationToken);
        return connection;
    }

    public new async ValueTask<AwsWrapperConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        this.mostRecentConnection = this.CreateConnection();
        await this.mostRecentConnection.OpenAsync(cancellationToken);
        return this.mostRecentConnection;
    }
}
