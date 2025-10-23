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
using Apps72.Dev.Data.DbMocker;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectProviderTests
{
    private readonly MockDbConnection mockConnection;
    private readonly Mock<PluginService> mockPluginService;
    private readonly DialectProvider dialectProvider;

    public DialectProviderTests()
    {
        this.mockPluginService = new Mock<PluginService>();
        this.mockPluginService.Object.InitialConnectionHostSpec = new HostSpecBuilder().WithHost("anyHost").Build();
        this.dialectProvider = new DialectProvider(this.mockPluginService.Object);

        this.mockConnection = new MockDbConnection();
        this.mockConnection.Open();
    }

    public static TheoryData<Dictionary<string, string>, Type> GuessDialectTestData =>
        new()
        {
        {
            new Dictionary<string, string>
            {
                { PropertyDefinition.TargetDialect.Name, typeof(TestCustomDialect).AssemblyQualifiedName! },
            }, typeof(TestCustomDialect)
        },
        {
            new Dictionary<string, string>
        {
            { PropertyDefinition.Host.Name, "192.168.1.1" },
            { PropertyDefinition.TargetConnectionType.Name, typeof(NpgsqlConnection).AssemblyQualifiedName! },
        }, typeof(PgDialect)
        },
        {
            new Dictionary<string, string>
        {
            { PropertyDefinition.Server.Name, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com" },
            { PropertyDefinition.TargetConnectionType.Name, typeof(MySqlConnection).AssemblyQualifiedName! },
        }, typeof(AuroraMySqlDialect)
        },
        {
            new Dictionary<string, string>
        {
            { PropertyDefinition.Server.Name, "some-unknown-host.com" },
            { PropertyDefinition.TargetConnectionType.Name, typeof(DbConnection).AssemblyQualifiedName! },
        }, typeof(UnknownDialect)
        },
        };

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(GuessDialectTestData))]
    public void GuessDialectTest<T>(Dictionary<string, string> props, Type dialectType)
    {
        var dialect = this.dialectProvider.GuessDialect(props);
        Assert.IsType(dialectType, dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithInvalidCustomDialect_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.TargetDialect.Name, "NonExistentType" }, };
        Assert.Throws<InvalidOperationException>(() => this.dialectProvider.GuessDialect(props));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_PgToRdsPg()
    {
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText == PgDialect.PGSelect1Query)
            .ReturnsTable(MockTable.WithColumns("?column?").AddRow(1));
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText == RdsPgDialect.ExtensionsSql)
            .ReturnsTable(MockTable.WithColumns("rds_tools", "aurora_stat_utils").AddRow(true, false));
        var initialDialect = new PgDialect();
        var updatedDialect = await this.dialectProvider.UpdateDialectAsync(this.mockConnection, initialDialect);
        Assert.IsType<RdsPgDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_WithNoMatchingDialectCandidate_ReturnsOriginalDialect()
    {
        var initialDialectMock = new Mock<IDialect>();
        initialDialectMock.Setup(d => d.DialectUpdateCandidates).Returns(
        [
            typeof(AuroraPgDialect),
            typeof(PgDialect),
        ]);
        initialDialectMock.Setup(d => d.IsDialect(this.mockConnection)).ReturnsAsync(true);
        var updatedDialect = await this.dialectProvider.UpdateDialectAsync(this.mockConnection, initialDialectMock.Object);

        Assert.Same(initialDialectMock.Object, updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_WithUnknownDialect_ThrowsArgumentException()
    {
        var unknownDialect = new UnknownDialect();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            this.dialectProvider.UpdateDialectAsync(this.mockConnection, unknownDialect));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_WithUnknownDialect_ReturnsMysqlDialect()
    {
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText == new MySqlDialect().ServerVersionQuery)
            .ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "MySQL Community Server (GPL)"));

        var unknownDialect = new UnknownDialect();

        var updatedDialect = await this.dialectProvider.UpdateDialectAsync(this.mockConnection, unknownDialect);
        Assert.IsType<MySqlDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_WithMysqlDialect_ReturnsAuroraMysqlDialect()
    {
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText == new MySqlDialect().ServerVersionQuery)
            .ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "Source distribution"));

        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText == AuroraMySqlDialect.IsDialectQuery)
            .ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("aurora_version", "3.08.2"));

        var mysqlDialect = new MySqlDialect();

        var updatedDialect = await this.dialectProvider.UpdateDialectAsync(this.mockConnection, mysqlDialect);

        Assert.IsType<AuroraMySqlDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_WithMysqlDialect_ReturnsRdsMysqlDialect()
    {
        this.mockConnection.Mocks
            .When(cmd => cmd.CommandText == new MySqlDialect().ServerVersionQuery)
            .ReturnsTable(MockTable.WithColumns("Variable_name", "Value").AddRow("version_comment", "Source distribution"));

        var mysqlDialect = new MySqlDialect();

        var updatedDialect = await this.dialectProvider.UpdateDialectAsync(this.mockConnection, mysqlDialect);

        Assert.IsType<RdsMySqlDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UpdateDialect_CannotUpdate_ReturnsSameDialect()
    {
        this.mockConnection.Mocks.WhenAny().ThrowsException(new MockDbException());

        var mysqlDialect = new MySqlDialect();
        var updatedDialect = await this.dialectProvider.UpdateDialectAsync(this.mockConnection, mysqlDialect);
        Assert.IsType<MySqlDialect>(updatedDialect);
    }

    // Custom dialect class for testing
    private class TestCustomDialect : IDialect
    {
        public int DefaultPort => 1234;
        public string HostAliasQuery => "SELECT 'test'";
        public string ServerVersionQuery => "SELECT 'version'";
        public IList<Type> DialectUpdateCandidates => new List<Type>();
        public HostListProviderSupplier HostListProviderSupplier => (props, service, pluginService) => null;

        public IExceptionHandler ExceptionHandler => new GenericExceptionHandler();

        public Task<bool> IsDialect(DbConnection conn) => Task.FromResult(true);

        public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
        {
        }
    }
}
