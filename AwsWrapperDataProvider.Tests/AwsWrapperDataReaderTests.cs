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
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Tests.Driver;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperDataReaderTests
{
    private readonly Mock<DbDataReader> mockTargetReader;
    private readonly Mock<ConnectionPluginManager> mockPluginManager;
    private readonly AwsWrapperDataReader wrapper;

    public AwsWrapperDataReaderTests()
    {
        NpgsqlDialectLoader.Load();
        AwsWrapperConnection<NpgsqlConnection> connection = new("Server=192.0.0.1;Database=test;User Id=user;Password=password;");

        this.mockTargetReader = new Mock<DbDataReader>();
        this.mockPluginManager = new Mock<ConnectionPluginManager>(
            new Mock<IConnectionProvider>().Object,
            new Mock<IConnectionProvider>().Object,
            new List<IConnectionPlugin> { new TestPluginOne([]) },
            connection)
        {
            CallBase = true,
        };

        this.wrapper = new AwsWrapperDataReader(this.mockTargetReader.Object, this.mockPluginManager.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Depth_DelegatesToTargetReader()
    {
        var result = this.wrapper.Depth;
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.Depth);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsClosed_DelegatesToTargetReader()
    {
        var result = this.wrapper.IsClosed;
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.IsClosed);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RecordsAffected_DelegatesToTargetReader()
    {
        var result = this.wrapper.RecordsAffected;
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.RecordsAffected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Close_DelegatesToTargetReader()
    {
        this.wrapper.Close();
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetReader, r => r.Close());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetSchemaTable_DelegatesToTargetReader()
    {
        this.wrapper.GetSchemaTable();
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.GetSchemaTable());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NextResult_DelegatesToTargetReader()
    {
        this.wrapper.NextResult();
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetReader, r => r.NextResult());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Read_DelegatesToTargetReader()
    {
        this.wrapper.Read();
        TestUtils.VerifyDelegatesToExecutePipeline(this.mockPluginManager, this.mockTargetReader, r => r.Read());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetBoolean_DelegatesToTargetReader()
    {
        this.wrapper.GetBoolean(1);
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.GetBoolean(1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetByte_DelegatesToTargetReader()
    {
        this.wrapper.GetByte(1);
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.GetByte(1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetString_DelegatesToTargetReader()
    {
        this.wrapper.GetString(0);
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.GetString(0));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Indexer_ByName_DelegatesToTargetReader()
    {
        var result = this.wrapper["foo"];
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r["foo"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Indexer_ByIndex_DelegatesToTargetReader()
    {
        var result = this.wrapper[0];
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetEnumerator_DelegatesToTargetReader()
    {
        this.wrapper.GetEnumerator();
        TestUtils.VerifyDelegatesToTargetObject(this.mockTargetReader, r => r.GetEnumerator());
    }
}
