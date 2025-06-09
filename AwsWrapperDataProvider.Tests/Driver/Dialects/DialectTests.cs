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
using AwsWrapperDataProvider.Driver.Dialects;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectTests
{
    private readonly Mock<IDbConnection> mockConnection;
    private readonly Mock<IDbCommand> mockCommand;
    private readonly Mock<DbDataReader> mockReader;

    public DialectTests()
    {
        this.mockConnection = new Mock<IDbConnection>();
        this.mockCommand = new Mock<IDbCommand>();
        this.mockReader = new Mock<DbDataReader>();

        this.mockConnection.Setup(conn => conn.CreateCommand()).Returns(this.mockCommand.Object);
        this.mockCommand.Setup(cmd => cmd.ExecuteReader()).Returns(this.mockReader.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_MySQL_Success()
    {
        IDialect mysqlDialect = new MysqlDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(true);
        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
        this.mockReader.Setup(reader => reader.GetString(0)).Returns("MySQL Community Server (GPL)");
        Assert.True(mysqlDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_MySQL_EmptyReader()
    {
        IDialect mysqlDialect = new MysqlDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(false);
        Assert.False(mysqlDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_MySQL_ExceptionThrown()
    {
        IDialect mysqlDialect = new MysqlDialect();
        this.mockReader.Setup(reader => reader.Read()).Throws(new MockDbException());
        Assert.False(mysqlDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_MySQL_InvalidVersionComment()
    {
        IDialect mysqlDialect = new MysqlDialect();
        this.mockReader.SetupSequence(reader => reader.Read()).Returns(true).Returns(false);
        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
        this.mockReader.Setup(reader => reader.GetString(0)).Returns("Invalid");
        Assert.False(mysqlDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsMySQL_Success()
    {
        IDialect rdsMysqlDialect = new RdsMysqlDialect();
        this.mockReader.SetupSequence(reader => reader.Read()).Returns(true).Returns(false).Returns(true).Returns(false);
        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
        this.mockReader.Setup(reader => reader.GetString(0)).Returns("Source distribution");
        Assert.True(rdsMysqlDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsMySQL_EmptyReader()
    {
        IDialect rdsMysqlDialect = new RdsMysqlDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(false);
        Assert.False(rdsMysqlDialect.IsDialect(this.mockConnection.Object));
        this.mockReader.Verify(reader => reader.Read(), Times.Exactly(2));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsMySQL_ExceptionThrown()
    {
        IDialect rdsMysqlDialect = new RdsMysqlDialect();
        this.mockReader.Setup(reader => reader.Read()).Throws(new MockDbException());
        Assert.False(rdsMysqlDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsMySQL_BaseReturnsTrue()
    {
        IDialect rdsMysqlDialect = new RdsMysqlDialect();
        this.mockReader.SetupSequence(reader => reader.Read()).Returns(true).Returns(false);
        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
        this.mockReader.Setup(reader => reader.GetString(0)).Returns("MySQL Community Server (GPL)");
        Assert.False(rdsMysqlDialect.IsDialect(this.mockConnection.Object));
        this.mockReader.Verify(reader => reader.Read(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsMySQL_InvalidVersionComment()
    {
        IDialect rdsMysqlDialect = new RdsMysqlDialect();
        this.mockReader.SetupSequence(reader => reader.Read()).Returns(true).Returns(false).Returns(true).Returns(false);
        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
        this.mockReader.Setup(reader => reader.GetString(0)).Returns("Invalid");
        Assert.False(rdsMysqlDialect.IsDialect(this.mockConnection.Object));
        this.mockReader.Verify(reader => reader.Read(), Times.Exactly(4));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_PG_Success()
    {
        IDialect pgDialect = new PgDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(true);
        Assert.True(pgDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_PG_ExceptionThrown()
    {
        IDialect pgDialect = new PgDialect();
        this.mockCommand.Setup(cmd => cmd.ExecuteReader()).Throws(new MockDbException());
        Assert.False(pgDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_PG_EmptyReader()
    {
        IDialect pgDialect = new PgDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(false);
        Assert.False(pgDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsPG_Success()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(true);
        this.mockReader.Setup(reader => reader.GetOrdinal("rds_tools")).Returns(0);
        this.mockReader.Setup(reader => reader.GetBoolean(0)).Returns(true);
        this.mockReader.Setup(reader => reader.GetOrdinal("aurora_stat_utils")).Returns(1);
        this.mockReader.Setup(reader => reader.GetBoolean(1)).Returns(false);
        Assert.True(rdsPgDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsPG_ExceptionThrown()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockCommand.Setup(cmd => cmd.ExecuteReader()).Throws(new MockDbException());
        Assert.False(rdsPgDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsPG_EmptyReader()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockReader.Setup(reader => reader.Read()).Returns(false);
        Assert.False(rdsPgDialect.IsDialect(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsDialect_RdsPG_IsAurora()
    {
        IDialect rdsPgDialect = new RdsPgDialect();
        this.mockReader.SetupSequence(reader => reader.Read()).Returns(true).Returns(false);
        this.mockReader.Setup(reader => reader.GetOrdinal("rds_tools")).Returns(0);
        this.mockReader.Setup(reader => reader.GetBoolean(0)).Returns(true);
        this.mockReader.Setup(reader => reader.GetOrdinal("aurora_stat_utils")).Returns(1);
        this.mockReader.Setup(reader => reader.GetBoolean(1)).Returns(true);
        Assert.False(rdsPgDialect.IsDialect(this.mockConnection.Object));
    }
}
