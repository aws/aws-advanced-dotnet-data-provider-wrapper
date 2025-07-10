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

    public AwsWrapperDataAdapter(DbDataAdapter targetDataAdapter, ConnectionPluginManager connectionPluginManager)
    {
        this.targetDataAdapter = targetDataAdapter;
        this.connectionPluginManager = connectionPluginManager;
    }

    internal DbDataAdapter TargetDbDataAdapter => this.targetDataAdapter;

    public new AwsWrapperCommand? DeleteCommand
    {
        set => this.targetDataAdapter.DeleteCommand = value;
    }

    public new AwsWrapperCommand? InsertCommand
    {
        set => this.targetDataAdapter.InsertCommand = value;
    }

    public new AwsWrapperCommand? SelectCommand
    {
        set => this.targetDataAdapter.SelectCommand = value;
    }

    public override int UpdateBatchSize => WrapperUtils.ExecuteWithPlugins(
        this.connectionPluginManager,
        this.targetDataAdapter,
        "DbDataAdapter.UpdateBatchSize",
        () => this.targetDataAdapter.UpdateBatchSize);

    public new AwsWrapperCommand? UpdateCommand
    {
        set => this.targetDataAdapter.UpdateCommand = value;
    }

    protected override int Fill(DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Fill",
            () => this.targetDataAdapter.Fill(dataSet, startRecord, maxRecords, srcTable));
    }

    protected override int Fill(DataTable[] dataTables, int startRecord, int maxRecords, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Fill",
            () => this.targetDataAdapter.Fill(startRecord, maxRecords, dataTables));
    }

    protected override int Fill(DataTable dataTable, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Fill",
            () => this.targetDataAdapter.Fill(dataTable));
    }

    protected override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType, IDbCommand command, string srcTable, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<DataTable[]>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.FillSchema",
            () => this.targetDataAdapter.FillSchema(dataSet, schemaType, srcTable));
    }

    protected override DataTable? FillSchema(DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior)
    {
        return WrapperUtils.ExecuteWithPlugins<DataTable?>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.FillSchema",
            () => this.targetDataAdapter.FillSchema(dataTable, schemaType));
    }

    protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping)
    {
        return WrapperUtils.ExecuteWithPlugins<int>(
            this.connectionPluginManager,
            this.targetDataAdapter,
            "DbDataAdapter.Update",
            () => this.targetDataAdapter.Update(dataRows));
    }
}
