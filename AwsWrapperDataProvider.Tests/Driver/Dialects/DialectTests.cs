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

//using System.Data;
//using System.Data.Common;
//using AwsWrapperDataProvider.Driver.Dialects;
//using Moq;

//namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

//public class DialectTests
//{
//    private readonly Mock<DbConnection> mockConnection;
//    private readonly Mock<DbCommand> mockCommand;
//    private readonly Mock<DbDataReader> mockReader;

//    public DialectTests()
//    {
//        this.mockConnection = new Mock<DbConnection>();
//        this.mockCommand = new Mock<DbCommand>();
//        this.mockReader = new Mock<DbDataReader>();

//        this.mockConnection.Setup(conn => conn.CreateCommand()).Returns(this.mockCommand.Object);
//        this.mockConnection.Setup(conn => conn.State).Returns(ConnectionState.Open);
//        this.mockCommand.Setup(cmd => cmd.ExecuteReaderAsync()).ReturnsAsync(this.mockReader.Object);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_MySQL_Success()
//    {
//        IDialect mysqlDialect = new MySqlDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(true);
//        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
//        this.mockReader.Setup(reader => reader.GetString(0)).Returns("MySQL Community Server (GPL)");
//        Assert.True(await mysqlDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_MySQL_EmptyReader()
//    {
//        IDialect mysqlDialect = new MySqlDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(false);
//        Assert.False(await mysqlDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_MySQL_ExceptionThrown()
//    {
//        IDialect mysqlDialect = new MySqlDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).Throws(new MockDbException());
//        Assert.False(await mysqlDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_MySQL_InvalidVersionComment()
//    {
//        IDialect mysqlDialect = new MySqlDialect();
//        this.mockReader.SetupSequence(reader => reader.ReadAsync()).ReturnsAsync(true).ReturnsAsync(false);
//        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
//        this.mockReader.Setup(reader => reader.GetString(0)).Returns("Invalid");
//        Assert.False(await mysqlDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsMySQL_Success()
//    {
//        IDialect rdsMysqlDialect = new RdsMySqlDialect();
//        this.mockReader.SetupSequence(reader => reader.ReadAsync()).ReturnsAsync(true).ReturnsAsync(false).ReturnsAsync(true).ReturnsAsync(false);
//        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
//        this.mockReader.Setup(reader => reader.GetString(0)).Returns("Source distribution");
//        Assert.True(await rdsMysqlDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsMySQL_EmptyReader()
//    {
//        IDialect rdsMysqlDialect = new RdsMySqlDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(false);
//        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection.Object));
//        this.mockReader.Verify(reader => reader.ReadAsync(), Times.Exactly(2));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsMySQL_ExceptionThrown()
//    {
//        IDialect rdsMysqlDialect = new RdsMySqlDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).Throws(new MockDbException());
//        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsMySQL_BaseReturnsTrue()
//    {
//        IDialect rdsMysqlDialect = new RdsMySqlDialect();
//        this.mockReader.SetupSequence(reader => reader.ReadAsync()).ReturnsAsync(true).ReturnsAsync(false);
//        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
//        this.mockReader.Setup(reader => reader.GetString(0)).Returns("MySQL Community Server (GPL)");
//        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection.Object));
//        this.mockReader.Verify(reader => reader.ReadAsync(), Times.Once);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsMySQL_InvalidVersionComment()
//    {
//        IDialect rdsMysqlDialect = new RdsMySqlDialect();
//        this.mockReader.SetupSequence(reader => reader.ReadAsync()).ReturnsAsync(true).ReturnsAsync(false).ReturnsAsync(true).ReturnsAsync(false);
//        this.mockReader.Setup(reader => reader.FieldCount).Returns(1);
//        this.mockReader.Setup(reader => reader.GetString(0)).Returns("Invalid");
//        Assert.False(await rdsMysqlDialect.IsDialect(this.mockConnection.Object));
//        this.mockReader.Verify(reader => reader.ReadAsync(), Times.Exactly(4));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_PG_Success()
//    {
//        IDialect pgDialect = new PgDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(true);
//        Assert.True(await pgDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_PG_ExceptionThrown()
//    {
//        IDialect pgDialect = new PgDialect();
//        this.mockCommand.Setup(cmd => cmd.ExecuteReaderAsync()).Throws(new MockDbException());
//        Assert.False(await pgDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_PG_EmptyReader()
//    {
//        IDialect pgDialect = new PgDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(false);
//        Assert.False(await pgDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsPG_Success()
//    {
//        IDialect rdsPgDialect = new RdsPgDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(true);
//        this.mockReader.Setup(reader => reader.GetOrdinal("rds_tools")).Returns(0);
//        this.mockReader.Setup(reader => reader.GetBoolean(0)).Returns(true);
//        this.mockReader.Setup(reader => reader.GetOrdinal("aurora_stat_utils")).Returns(1);
//        this.mockReader.Setup(reader => reader.GetBoolean(1)).Returns(false);
//        Assert.True(await rdsPgDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsPG_ExceptionThrown()
//    {
//        IDialect rdsPgDialect = new RdsPgDialect();
//        this.mockCommand.Setup(cmd => cmd.ExecuteReaderAsync()).Throws(new MockDbException());
//        Assert.False(await rdsPgDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsPG_EmptyReader()
//    {
//        IDialect rdsPgDialect = new RdsPgDialect();
//        this.mockReader.Setup(reader => reader.ReadAsync()).ReturnsAsync(false);
//        Assert.False(await rdsPgDialect.IsDialect(this.mockConnection.Object));
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public async Task IsDialect_RdsPG_IsAurora()
//    {
//        IDialect rdsPgDialect = new RdsPgDialect();
//        this.mockReader.SetupSequence(reader => reader.ReadAsync()).ReturnsAsync(true).ReturnsAsync(false);
//        this.mockReader.Setup(reader => reader.GetOrdinal("rds_tools")).Returns(0);
//        this.mockReader.Setup(reader => reader.GetBoolean(0)).Returns(true);
//        this.mockReader.Setup(reader => reader.GetOrdinal("aurora_stat_utils")).Returns(1);
//        this.mockReader.Setup(reader => reader.GetBoolean(1)).Returns(true);
//        Assert.False(await rdsPgDialect.IsDialect(this.mockConnection.Object));
//    }
//}
