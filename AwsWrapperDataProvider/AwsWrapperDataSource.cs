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
using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider;

public class AwsWrapperDataSource : DbDataSource, IWrapper
{
    private readonly DbDataSource targetDataSource;

    public AwsWrapperDataSource(DbDataSource targetDataSource)
    {
        this.targetDataSource = targetDataSource;
    }

    public override string ConnectionString => this.targetDataSource.ConnectionString;

    public new AwsWrapperConnection CreateConnection() => (AwsWrapperConnection)base.CreateConnection();

    public new AwsWrapperConnection OpenConnection() => (AwsWrapperConnection)base.OpenConnection();

    public async ValueTask<AwsWrapperConnection> OpenConnectionAsync()
    {
        return (AwsWrapperConnection)await base.OpenConnectionAsync();
    }

    public new DbCommand CreateCommand(string? commandText = null) => base.CreateCommand(commandText);

    public new DbBatch CreateBatch() => base.CreateBatch();

    protected override DbConnection CreateDbConnection()
    {
        DbConnection connection = this.targetDataSource.CreateConnection();

        return new AwsWrapperConnection(connection);
    }

    protected override DbCommand CreateDbCommand(string? commandText = null)
    {
        var connection = this.CreateDbConnection();

        if (connection is AwsWrapperConnection awsWrapperConnection)
        {
            AwsWrapperCommand<DbCommand> command = awsWrapperConnection.CreateCommand<DbCommand>();
            command.CommandText = commandText;
            return command;
        }
        else
        {
            throw new InvalidOperationException(Properties.Resources.Error_NotAwsWrapperConnection);
        }
    }

    protected override DbBatch CreateDbBatch()
    {
        var connection = this.CreateDbConnection();

        if (connection is AwsWrapperConnection awsWrapperConnection)
        {
            AwsWrapperBatch batch = awsWrapperConnection.CreateBatch();
            return batch;
        }
        else
        {
            throw new InvalidOperationException(Properties.Resources.Error_NotAwsWrapperConnection);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            this.targetDataSource?.Dispose();
        }
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await this.targetDataSource.DisposeAsync().ConfigureAwait(false);
    }

    public T Unwrap<T>() where T : class
    {
        if (this.targetDataSource is T dataSourceAsT)
        {
            return dataSourceAsT;
        }

        throw new ArgumentException(string.Format(Resources.Error_CannotUnwrap, typeof(AwsWrapperDataSource).Name, typeof(T).Name));
    }

    public bool IsWrapperFor<T>() where T : class
    {
        return this.targetDataSource is T;
    }
}
