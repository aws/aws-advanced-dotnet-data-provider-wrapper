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
    public void UpdateBatchSize_DelegatesToTargetDataAdapter()
    {
        var result = this.wrapper.UpdateBatchSize;
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataAdapter, r => r.UpdateBatchSize);
    }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void FillWithDataSet_DelegatesToTargetDataAdapter()
    // {
    //     var result = this.wrapper.Fill(new System.Data.DataSet());
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataAdapter, r => r.Fill(new System.Data.DataSet()));
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void FillWithDataTables_DelegatesToTargetDataAdapter()
    // {
    //     System.Data.DataTable[] tables = [];
    //     var result = this.wrapper.Fill(0, 1, tables);
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataAdapter, r => r.Fill(0, 1, tables));
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void FillWithDataTable_DelegatesToTargetDataAdapter()
    // {
    //     var result = this.wrapper.Fill(new System.Data.DataTable());
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataAdapter, r => r.Fill(new System.Data.DataTable()));
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void Update_DelegatesToTargetDataAdapter()
    // {
    //     System.Data.DataRow[] dataRows = [];
    //     var result = this.wrapper.Update(dataRows);
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataAdapter, r => r.Update(dataRows));
    // }
}
