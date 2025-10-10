//// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
////
//// Licensed under the Apache License, Version 2.0 (the "License").
//// You may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//using System.Data.Common;
//using AwsWrapperDataProvider.Driver;
//using AwsWrapperDataProvider.Driver.ConnectionProviders;
//using AwsWrapperDataProvider.Driver.Plugins;
//using AwsWrapperDataProvider.Tests.Driver;
//using AwsWrapperDataProvider.Tests.Driver.Plugins;
//using Moq;
//using Npgsql;

//namespace AwsWrapperDataProvider.Tests;

//public class AwsWrapperBatchTests
//{
//    private readonly Mock<DbBatch> mockTargetBatch;
//    private readonly Mock<ConnectionPluginManager> mockPluginManager;
//    private readonly AwsWrapperBatch wrapper;

//    public AwsWrapperBatchTests()
//    {
//        AwsWrapperConnection<NpgsqlConnection> connection = new("Server=192.0.0.1;Database=test;User Id=user;Password=password;");

//        this.mockTargetBatch = new Mock<DbBatch>();
//        this.mockPluginManager = new Mock<ConnectionPluginManager>(
//            new Mock<IConnectionProvider>().Object,
//            new Mock<IConnectionProvider>().Object,
//            new List<IConnectionPlugin> { new TestPluginOne([]) },
//            connection)
//        {
//            CallBase = true,
//        };

//        this.wrapper = new(this.mockTargetBatch.Object, connection, this.mockPluginManager.Object);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void ExecuteNonQuery_DelegatesToTargetBatch()
//    {
//        var result = this.wrapper.ExecuteNonQuery();
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.ExecuteNonQuery());
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task ExecuteNonQueryAsync_DelegatesToTargetBatch()
//    {
//        var result = await this.wrapper.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void ExecuteScalar_DelegatesToTargetBatch()
//    {
//        var result = this.wrapper.ExecuteScalar();
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.ExecuteScalar());
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task ExecuteScalarAsync_DelegatesToTargetBatch()
//    {
//        var result = await this.wrapper.ExecuteScalarAsync(TestContext.Current.CancellationToken);
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.ExecuteScalarAsync(TestContext.Current.CancellationToken));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void Prepare_DelegatesToTargetBatch()
//    {
//        this.wrapper.Prepare();
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.Prepare());
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task PrepareAsync_DelegatesToTargetBatch()
//    {
//        await this.wrapper.PrepareAsync(TestContext.Current.CancellationToken);
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.PrepareAsync(TestContext.Current.CancellationToken));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void Cancel_DelegatesToTargetBatch()
//    {
//        this.wrapper.Cancel();
//        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetBatch, r => r.Cancel());
//    }
//}
