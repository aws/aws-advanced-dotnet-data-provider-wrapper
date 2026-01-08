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
    public async Task IsDialect_AuroraMySQL_Success()
    {
        IDialect dialect = new AuroraMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("aurora_version", "3.08.2"));
        Assert.True(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraMySQL_EmptyReader()
    {
        IDialect dialect = new AuroraMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraMySQL_ExceptionThrown()
    {
        IDialect dialect = new AuroraMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzMySql_Success()
    {
        IDialect dialect = new RdsMultiAzDbClusterMySqlDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(RdsMultiAzDbClusterMySqlDialect.TopologyTableExistQuery) ||
                         cmd.CommandText.Contains(RdsMultiAzDbClusterMySqlDialect.TopologyQuery))
            .ReturnsTable(MockTable.WithColumns("1").AddRow("1"));
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(RdsMultiAzDbClusterMySqlDialect.IsDialectQuery))
            .ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("report_host", "ip-10-0-2-123.ec2.internal"));
        Assert.True(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzMySqlL_EmptyReader()
    {
        IDialect dialect = new RdsMultiAzDbClusterMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzMySql_ExceptionThrown()
    {
        IDialect dialect = new RdsMultiAzDbClusterMySqlDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await dialect.IsDialect(this.mockConnection));
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

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraPG_Success()
    {
        IDialect dialect = new AuroraPgDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(PgDialect.PGSelect1Query))
            .ReturnsTable(MockTable.WithColumns("?column?").AddRow("1"));
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(AuroraPgDialect.ExtensionsSql) &&
                         cmd.CommandText.Contains(AuroraPgDialect.TopologySql))
            .ReturnsDataset(
        [
            MockTable.WithColumns("aurora_stat_utils").AddRow(true),
            MockTable.WithColumns("?column?").AddRow("1"),
        ]);
        Assert.True(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraPG_ExceptionThrownInBase()
    {
        IDialect dialect = new AuroraPgDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraPG_ExceptionThrown()
    {
        IDialect dialect = new AuroraPgDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(PgDialect.PGSelect1Query))
            .ReturnsTable(MockTable.WithColumns("?column?").AddRow("1"));
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraPG_HasExtensions_NoTopology()
    {
        IDialect dialect = new AuroraPgDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(PgDialect.PGSelect1Query))
            .ReturnsTable(MockTable.WithColumns("?column?").AddRow("1"));
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(AuroraPgDialect.ExtensionsSql) &&
                         cmd.CommandText.Contains(AuroraPgDialect.TopologySql))
            .ReturnsTable(MockTable.WithColumns("aurora_stat_utils").AddRow(true));
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraPG_NoExtensions_HasTopology()
    {
        IDialect dialect = new AuroraPgDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(PgDialect.PGSelect1Query))
            .ReturnsTable(MockTable.WithColumns("?column?").AddRow("1"));
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(AuroraPgDialect.ExtensionsSql) &&
                         cmd.CommandText.Contains(AuroraPgDialect.TopologySql))
            .ReturnsTable(MockTable.WithColumns("?column?").AddRow("1"));
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_AuroraPG_EmptyReader()
    {
        IDialect dialect = new AuroraPgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await dialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzPG_Success()
    {
        IDialect pgDialect = new RdsMultiAzDbClusterPgDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(RdsMultiAzDbClusterPgDialect.HasRdsToolsExtensionQuery))
            .ReturnsTable(MockTable.WithColumns("exists").AddRow(true));
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(RdsMultiAzDbClusterPgDialect.IsRdsClusterQuery))
            .ReturnsTable(MockTable.WithColumns("exists").AddRow(true));
        Assert.True(await pgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzPG_NoRdsTools()
    {
        IDialect pgDialect = new RdsMultiAzDbClusterPgDialect();
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText.Contains(RdsMultiAzDbClusterPgDialect.HasRdsToolsExtensionQuery))
            .ReturnsTable(MockTable.WithColumns("exists").AddRow(false));
        Assert.False(await pgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzPG_ExceptionThrown()
    {
        IDialect pgDialect = new RdsMultiAzDbClusterPgDialect();
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());
        Assert.False(await pgDialect.IsDialect(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsDialect_RdsMultiAzPG_EmptyReader()
    {
        IDialect pgDialect = new RdsMultiAzDbClusterPgDialect();
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());
        Assert.False(await pgDialect.IsDialect(this.mockConnection));
    }
}
