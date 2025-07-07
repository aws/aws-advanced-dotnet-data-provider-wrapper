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

// using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperDataSourceTests
{
    private readonly Mock<DbDataSource> mockTargetDataSource;
    private readonly Mock<ConnectionPluginManager> mockPluginManager;
    private readonly AwsWrapperDataSource wrapper;

    public AwsWrapperDataSourceTests()
    {
        AwsWrapperConnection<NpgsqlConnection> connection = new("Server=192.0.0.1;Database=test;User Id=user;Password=password;");

        this.mockTargetDataSource = new Mock<DbDataSource>();
        this.mockPluginManager = new Mock<ConnectionPluginManager>(
            new Mock<IConnectionProvider>().Object,
            new Mock<IConnectionProvider>().Object,
            new List<IConnectionPlugin> { new TestPluginOne([]) },
            connection)
        {
            CallBase = true,
        };

        this.wrapper = new(this.mockTargetDataSource.Object, this.mockPluginManager.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionString_DelegatesToTargetDataSource()
    {
        var result = this.wrapper.ConnectionString;
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataSource, r => r.ConnectionString);
    }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void CreateBatch_DelegatesToTargetDataSource()
    // {
    //     var result = this.wrapper.CreateBatch();
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataSource, r => r.CreateBatch());
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void CreateCommand_DelegatesToTargetDataSource()
    // {
    //     var result = this.wrapper.CreateCommand("SELECT 1");
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataSource, r => r.CreateCommand("SELECT 1"));
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void CreateConnection_DelegatesToTargetDataSource()
    // {
    //     var result = this.wrapper.CreateConnection();
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataSource, r => r.CreateConnection());
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void Dispose_DelegatesToTargetDataSource()
    // {
    //     this.wrapper.Dispose();
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataSource, r => r.Dispose());
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void OpenConnection_DelegatesToTargetDataSource()
    // {
    //     var result = this.wrapper.OpenConnection();
    //     TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetDataSource, r => r.OpenConnection());
    // }
}
