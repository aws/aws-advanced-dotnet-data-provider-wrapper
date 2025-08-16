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
using System.Linq.Expressions;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Tests.Driver;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperTransactionTests
{
    private const string SavepointName = "test_savepoint";

    private readonly Mock<DbTransaction> mockTargetTransaction;
    private readonly Mock<ConnectionPluginManager> mockPluginManager;
    private readonly AwsWrapperTransaction wrapper;

    public AwsWrapperTransactionTests()
    {
        AwsWrapperConnection<NpgsqlConnection> connection = new("Server=192.0.0.1;Database=test;User Id=user;Password=password;");

        this.mockTargetTransaction = new Mock<DbTransaction>();
        this.mockPluginManager = new Mock<ConnectionPluginManager>(
            new Mock<IConnectionProvider>().Object,
            new Mock<IConnectionProvider>().Object,
            new List<IConnectionPlugin> { new TestPluginOne([]) },
            connection)
        {
            CallBase = true,
        };

        this.wrapper = new AwsWrapperTransaction(connection, this.mockTargetTransaction.Object, this.mockPluginManager.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Commit_DelegatesToTargetTransaction()
    {
        this.wrapper.Commit();
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetTransaction, t => t.Commit());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Rollback_DelegatesToTargetTransaction()
    {
        this.wrapper.Rollback();
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetTransaction, t => t.Rollback());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Save_DelegatesToTargetTransaction()
    {
        this.wrapper.Save(SavepointName);
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetTransaction, t => t.Save(SavepointName));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RollbackWithSavepoint_DelegatesToTargetTransaction()
    {
        this.wrapper.Rollback(SavepointName);
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetTransaction, t => t.Rollback(SavepointName));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Release_DelegatesToTargetTransaction()
    {
        this.wrapper.Release(SavepointName);
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetTransaction, t => t.Release(SavepointName));
    }
}
