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
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Tests.Driver;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperDataAdapterTests
{
    private readonly Mock<DbDataAdapter> mockTargetDataAdapter;
    private readonly Mock<ConnectionPluginManager> mockPluginManager;
    private readonly AwsWrapperDataAdapter wrapper;

    public AwsWrapperDataAdapterTests()
    {
        NpgsqlDialectLoader.Load();
        AwsWrapperConnection<NpgsqlConnection> connection = new("Server=192.0.0.1;Database=test;User Id=user;Password=password;");

        this.mockTargetDataAdapter = new Mock<DbDataAdapter>();
        this.mockPluginManager = new Mock<ConnectionPluginManager>(
            new Mock<IConnectionProvider>().Object,
            new Mock<IConnectionProvider>().Object,
            new List<IConnectionPlugin> { new TestPluginOne([]) },
            connection)
        {
            CallBase = true,
        };

        this.wrapper = new(this.mockTargetDataAdapter.Object, this.mockPluginManager.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Fill_WithDataSet_PerformsExecutePipeline()
    {
        _ = this.wrapper.Fill(new DataSet());
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Fill");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Fill_WithDataTable_PerformsExecutePipeline()
    {
        _ = this.wrapper.Fill(new DataTable());
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Fill");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Fill_WithDataSetAndString_PerformsExecutePipeline()
    {
        _ = this.wrapper.Fill(new DataSet(), string.Empty);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Fill");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Fill_WithBoundsAndTables_PerformsExecutePipeline()
    {
        _ = this.wrapper.Fill(0, 1, []);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Fill");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Fill_WithDataSetAndBoundsAndString_PerformsExecutePipeline()
    {
        _ = this.wrapper.Fill(new DataSet(), 0, 1, string.Empty);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Fill");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FillSchema_WithDataSetAndSchemaType_PerformsExecutePipeline()
    {
        _ = this.wrapper.FillSchema(new DataSet(), SchemaType.Source);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, DataTable[]>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.FillSchema");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FillSchema_WithDataTableAndSchemaType_PerformsExecutePipeline()
    {
        _ = this.wrapper.FillSchema(new DataTable(), SchemaType.Source);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, DataTable?>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.FillSchema");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FillSchema_WithDataSetAndSchemaTypeAndString_PerformsExecutePipeline()
    {
        _ = this.wrapper.FillSchema(new DataSet(), SchemaType.Source, string.Empty);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, DataTable[]>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.FillSchema");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Update_WithDataRows_PerformsExecutePipeline()
    {
        _ = this.wrapper.Update([]);
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Update");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Update_WithDataSet_PerformsExecutePipeline()
    {
        _ = this.wrapper.Update(new DataSet());
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Update");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Update_WithDataTable_PerformsExecutePipeline()
    {
        _ = this.wrapper.Update(new DataTable());
        TestUtils.VerifyDelegatesToExecutePipeline<DbDataAdapter, int>(this.mockPluginManager, this.mockTargetDataAdapter, "DbDataAdapter.Update");
    }
}
