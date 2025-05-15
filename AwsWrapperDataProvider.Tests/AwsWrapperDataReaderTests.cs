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
using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using Moq;
using Npgsql;
using Org.BouncyCastle.Pqc.Crypto.Crystals.Dilithium;

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
        this._mockReader.Verify(r => r.Depth, Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Read_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<bool>();
        this._wrapper.Read();
        this._mockReader.Verify(r => r.Read(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Close_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<int>();
        this._wrapper.Close();
        this._mockReader.Verify(r => r.Close(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetBoolean_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<bool>();
        this._wrapper.GetBoolean(1);
        this._mockReader.Verify(r => r.GetBoolean(1), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetByte_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<byte>();
        this._wrapper.GetByte(1);
        this._mockReader.Verify(r => r.GetByte(1), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetString_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<string>();
        this._wrapper.GetString(0);
        this._mockReader.Verify(r => r.GetString(0), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Indexer_ByName_DelegatesToTargetReader()
    {
        this._mockReader.Setup(r => r["foo"]).Returns("bar");
        var result = this._wrapper["foo"];
        Assert.Equal("bar", result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Indexer_ByIndex_DelegatesToTargetReader()
    {
        this._mockReader.Setup(r => r[0]).Returns(123);
        var result = this._wrapper[0];
        Assert.Equal(123, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetEnumerator_DelegatesToTargetReader()
    {
        this.SetupConnectionPluginManagerExecute<IEnumerator>();
        this._wrapper.GetEnumerator();
        this._mockReader.Verify(r => r.GetEnumerator(), Times.Once);
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
}
