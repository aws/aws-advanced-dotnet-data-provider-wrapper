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

using Apps72.Dev.Data.DbMocker;
using AwsWrapperDataProvider.Driver.Dialects;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectTests
{
    private readonly MockDbConnection mockConnection;

    public DialectTests()
    {
        this.mockConnection = new MockDbConnection();
        this.mockConnection.Open();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_MySQL_Success()
    {
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "MySQL Community Server (GPL)"));
        IDialect mysqlDialect = new MySqlDialect();
        Assert.True(await mysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_MySQL_EmptyReader()
    {
        IDialect mysqlDialect = new MySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await mysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_MySQL_ExceptionThrown()
    {
        IDialect mysqlDialect = new MySqlDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await mysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_MySQL_InvalidVersionComment()
    {
        IDialect mysqlDialect = new MySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "invalid"));
        Assert.False(await mysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMySQL_Success()
    {
        IDialect rdsMysqlDialect = new RdsMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "Source distribution"));
        Assert.True(await rdsMysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMySQL_EmptyReader()
    {
        IDialect rdsMysqlDialect = new RdsMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMySQL_ExceptionThrown()
    {
        IDialect rdsMysqlDialect = new RdsMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMySQL_BaseReturnsTrue()
    {
        IDialect rdsMysqlDialect = new RdsMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "MySQL Community Server (GPL)"));
        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMySQL_InvalidVersionComment()
    {
        IDialect rdsMysqlDialect = new RdsMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "invalid"));
        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_PG_Success()
    {
        IDialect pgDialect = new PgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("?column?").AddRow("1"));
        Assert.True(await pgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_PG_ExceptionThrown()
    {
        IDialect pgDialect = new PgDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await pgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_PG_EmptyReader()
    {
        IDialect pgDialect = new PgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await pgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsPG_Success()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("rds_tools", "aurora_stat_utils").AddRow(true, false));
        Assert.True(await rdsPgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsPG_ExceptionThrown()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await rdsPgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsPG_EmptyReader()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await rdsPgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsPG_IsAurora()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("rds_tools", "aurora_stat_utils").AddRow(true, true));
        Assert.False(await rdsPgDialect.IsDialect(this.mockConnection));
    }
}
