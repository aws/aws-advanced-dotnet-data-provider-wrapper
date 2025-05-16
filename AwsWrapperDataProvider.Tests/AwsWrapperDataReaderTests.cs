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

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using Moq;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperDataReaderTests
{
    private readonly Mock<DbDataReader> _mockReader;
    private readonly Mock<ConnectionPluginManager> _mockPluginManager;
    private readonly AwsWrapperDataReader _wrapper;

    public AwsWrapperDataReaderTests()
    {
        Mock<IConnectionProvider> mockConnectionProvider = new();
        AwsWrapperConnection<NpgsqlConnection> connection =
            new("Server=192.0.0.1;Database=test;User Id=user;Password=password;");

        this._mockReader = new Mock<DbDataReader>();
        this._mockPluginManager = new Mock<ConnectionPluginManager>(mockConnectionProvider.Object, null!, connection);

        this._wrapper = new AwsWrapperDataReader(this._mockReader.Object, this._mockPluginManager.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Depth_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<int>();
        var result = this._wrapper.Depth;
        this.VerifyDelegatesToExecutePipeline<int>(r => r.Depth);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsClosed_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<bool>();
        var result = this._wrapper.IsClosed;
        this.VerifyDelegatesToExecutePipeline<bool>(r => r.IsClosed);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RecordsAffected_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<int>();
        var result = this._wrapper.RecordsAffected;
        this.VerifyDelegatesToExecutePipeline<int>(r => r.RecordsAffected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Close_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<object>();
        this._wrapper.Close();
        this.VerifyDelegatesToExecutePipeline(r => r.Close());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetSchemaTable_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<DataTable?>();
        this._wrapper.GetSchemaTable();
        this.VerifyDelegatesToExecutePipeline<DataTable?>(r => r.GetSchemaTable());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NextResult_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<bool>();
        this._wrapper.NextResult();
        this.VerifyDelegatesToExecutePipeline<bool>(r => r.NextResult());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Read_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<bool>();
        this._wrapper.Read();
        this.VerifyDelegatesToExecutePipeline<bool>(r => r.Read());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetBoolean_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<bool>();
        this._wrapper.GetBoolean(1);
        this.VerifyDelegatesToExecutePipeline<bool>(r => r.GetBoolean(1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetByte_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<byte>();
        this._wrapper.GetByte(1);
        this.VerifyDelegatesToExecutePipeline<byte>(r => r.GetByte(1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetString_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<string>();
        this._wrapper.GetString(0);
        this.VerifyDelegatesToExecutePipeline<string>(r => r.GetString(0));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Indexer_ByName_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<object>();
        var result = this._wrapper["foo"];
        this.VerifyDelegatesToExecutePipeline<object>(r => r["foo"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Indexer_ByIndex_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<object>();
        var result = this._wrapper[0];
        this.VerifyDelegatesToExecutePipeline<object>(r => r[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetEnumerator_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<IEnumerator>();
        this._wrapper.GetEnumerator();
        this.VerifyDelegatesToExecutePipeline<IEnumerator>(r => r.GetEnumerator());
    }

    private void SetupConnectionPluginManagerExecute<T>()
    {
        this._mockPluginManager.Setup(p => p.Execute<T>(
                It.IsAny<object>(),
                It.IsAny<string>(),
                It.IsAny<ADONetDelegate<T>>(),
                It.IsAny<object[]>()))
            .Returns((
                object methodInvokeOn,
                string methodName,
                ADONetDelegate<T> methodFunc,
                object[] methodArgs) => methodFunc());
    }

    private void VerifyDelegatesToExecutePipeline<T>(Expression<Func<DbDataReader, T>> expression)
    {
        this._mockPluginManager.Verify(r => r.Execute(
                It.IsAny<object>(),
                It.IsAny<string>(),
                It.IsAny<ADONetDelegate<T>>(),
                It.IsAny<object[]>()),
            Times.Once);
        this._mockReader.Verify(expression, Times.Once);
    }

    private void VerifyDelegatesToExecutePipeline(Expression<Action<DbDataReader>> expression)
    {
        this._mockPluginManager.Verify(r => r.Execute<object>(
                It.IsAny<object>(),
                It.IsAny<string>(),
                It.IsAny<ADONetDelegate<object>>(),
                It.IsAny<object[]>()),
            Times.Once);
        this._mockReader.Verify(expression, Times.Once);
    }
}
