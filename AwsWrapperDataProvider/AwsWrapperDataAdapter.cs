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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperDataAdapter : DbDataAdapter
{
    protected DbDataAdapter targetDataAdapter;

    private readonly ConnectionPluginManager connectionPluginManager;

    internal AwsWrapperDataAdapter(DbDataAdapter targetDataAdapter, ConnectionPluginManager connectionPluginManager)
    {
        this.targetDataAdapter = targetDataAdapter;
        this.connectionPluginManager = connectionPluginManager;
    }

    public AwsWrapperDataAdapter(DbDataAdapter targetDataAdapter, AwsWrapperConnection connection)
    {
        this.targetDataAdapter = targetDataAdapter;
        this.connectionPluginManager = connection.PluginManager;
    }

    internal DbDataAdapter TargetDbDataAdapter => this.targetDataAdapter;

    public new DbCommand? DeleteCommand
    {
        set
        {
            if (value == null)
            {
                this.targetDataAdapter.DeleteCommand = null;
                return;
            }

            if (value is not AwsWrapperCommand)
            {
                throw new InvalidOperationException("Provided command is not of type AwsWrapperCommand.");
            }

            this.targetDataAdapter.DeleteCommand = value;
        }
    }

    public new DbCommand? InsertCommand
    {
        set
        {
            if (value == null)
            {
                this.targetDataAdapter.InsertCommand = null;
                return;
            }

            if (value is not AwsWrapperCommand)
            {
                throw new InvalidOperationException("Provided command is not of type AwsWrapperCommand.");
            }

            this.targetDataAdapter.InsertCommand = value;
        }
    }

    public new DbCommand? SelectCommand
    {
        set
        {
            if (value == null)
            {
                this.targetDataAdapter.SelectCommand = null;
                return;
            }

            if (value is not AwsWrapperCommand)
            {
                throw new InvalidOperationException("Provided command is not of type AwsWrapperCommand.");
            }

            this.targetDataAdapter.SelectCommand = value;
        }
    }

    public override int UpdateBatchSize => this.targetDataAdapter.UpdateBatchSize;

    public new DbCommand? UpdateCommand
    {
        set
        {
            if (value == null)
            {
                this.targetDataAdapter.UpdateCommand = null;
                return;
            }

            if (value is not AwsWrapperCommand)
            {
                throw new InvalidOperationException("Provided command is not of type AwsWrapperCommand.");
            }

            this.targetDataAdapter.UpdateCommand = value;
        }
    }

    public override int Update(DataSet dataSet)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Update",
            () => Task.FromResult(this.targetDataAdapter.Update(dataSet))).GetAwaiter().GetResult();
    }

    public new int Update(DataRow[] dataRows)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Update",
            () => Task.FromResult(this.targetDataAdapter.Update(dataRows))).GetAwaiter().GetResult();
    }

    public new int Update(DataTable dataTable)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Update",
            () => Task.FromResult(this.targetDataAdapter.Update(dataTable))).GetAwaiter().GetResult();
    }

    public new int Update(DataSet dataSet, string srcTable)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Update",
            () => Task.FromResult(this.targetDataAdapter.Update(dataSet, srcTable))).GetAwaiter().GetResult();
    }

    protected override int Fill(DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Fill",
            () => Task.FromResult(this.targetDataAdapter.Fill(dataSet, startRecord, maxRecords, srcTable))).GetAwaiter().GetResult();
    }

    protected override int Fill(DataTable[] dataTables, int startRecord, int maxRecords, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Fill",
            () => Task.FromResult(this.targetDataAdapter.Fill(startRecord, maxRecords, dataTables))).GetAwaiter().GetResult();
    }

    protected override int Fill(DataTable dataTable, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Fill",
            () => Task.FromResult(this.targetDataAdapter.Fill(dataTable))).GetAwaiter().GetResult();
    }

    protected override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType, IDbCommand command, string srcTable, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<DataTable[]>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.FillSchema",
            () => Task.FromResult(this.targetDataAdapter.FillSchema(dataSet, schemaType, srcTable))).GetAwaiter().GetResult();
    }

    protected override DataTable? FillSchema(DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<DataTable?>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.FillSchema",
            () => Task.FromResult(this.targetDataAdapter.FillSchema(dataTable, schemaType))).GetAwaiter().GetResult();
    }

    protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Update",
            () => Task.FromResult(this.targetDataAdapter.Update(dataRows))).GetAwaiter().GetResult();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            this.targetDataAdapter?.Dispose();
        }
    }
}

public class AwsWrapperDataAdapter<TDataAdapter> : AwsWrapperDataAdapter where TDataAdapter : DbDataAdapter
{
    public AwsWrapperDataAdapter(string query, AwsWrapperConnection connection)
        : base((DbDataAdapter)Activator.CreateInstance(typeof(TDataAdapter))!, connection)
    {
        DbCommand command = connection.CreateCommand<DbCommand>();
        command.CommandText = query;
        this.SelectCommand = command;
    }
}
