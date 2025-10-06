using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;
using MySqlConnector;
using Npgsql;
using Xunit;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectProviderTests
{
    private readonly Mock<DbConnection> mockConnection;
    private readonly Mock<DbCommand> mockCommand;
    private readonly Mock<DbDataReader> mockReader;
    private readonly Mock<PluginService> mockPluginService;
    private readonly DialectProvider dialectProvider;

    public static TheoryData<Dictionary<string, string>, Type> GuessDialectTestData =>
        new()
        {
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
                    { PropertyDefinition.Server.Name, "some-unknown-host.com" },
                    { PropertyDefinition.TargetConnectionType.Name, typeof(DbConnection).AssemblyQualifiedName! },
                }, typeof(UnknownDialect)
            },
        };

    public class TestCustomDialect : IDialect
    {
        public int DefaultPort => 3306;
        public string HostAliasQuery => string.Empty;
        public string ServerVersionQuery => string.Empty;
        public IList<Type> DialectUpdateCandidates => new List<Type>();
        public HostListProviderSupplier HostListProviderSupplier => (_, _, _) => null;
        public IExceptionHandler ExceptionHandler => new GenericExceptionHandler();
        public bool IsDialect(IDbConnection connection) => false;
        public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec) { }
    }

    public DialectProviderTests()
    {
        this.mockConnection = new Mock<DbConnection>();
        this.mockConnection.SetupAllProperties();
        this.mockConnection.Object.ConnectionString = "anyHost";
        this.mockConnection.Setup(conn => conn.State).Returns(ConnectionState.Open);
        this.mockCommand = new Mock<DbCommand>();
        this.mockReader = new Mock<DbDataReader>();

        this.mockPluginService = new Mock<PluginService>();
        this.mockPluginService.Object.InitialConnectionHostSpec = new HostSpecBuilder().WithHost("anyHost").Build();

        this.dialectProvider = new DialectProvider(this.mockPluginService.Object, new Dictionary<string, string>());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_PgToRdsPg()
    {
        var initialDialect = new PgDialect();
        DbConnection connection = this.mockConnection.Object;
        var updatedDialect = this.dialectProvider.UpdateDialect(ref connection, initialDialect);
        Assert.IsType<PgDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_CannotUpdate_ReturnsSameDialect()
    {
        var initialDialectMock = new Mock<IDialect>();
        initialDialectMock.Setup(d => d.DialectUpdateCandidates).Returns(new List<Type> { typeof(PgDialect) });
        initialDialectMock.Setup(d => d.IsDialect(It.IsAny<IDbConnection>())).Returns(true);
        DbConnection connection = this.mockConnection.Object;
        var updatedDialect = this.dialectProvider.UpdateDialect(ref connection, initialDialectMock.Object);
        Assert.Same(initialDialectMock.Object, updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithUnknownDialect_ThrowsArgumentException()
    {
        var unknownDialect = new UnknownDialect();
        DbConnection connection = this.mockConnection.Object;
        Assert.Throws<ArgumentException>(() => this.dialectProvider.UpdateDialect(ref connection, unknownDialect));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithMysqlDialect_ReturnsAuroraMysqlDialect()
    {
        var mysqlDialect = new MySqlDialect();
        DbConnection connection = this.mockConnection.Object;
        var result = this.dialectProvider.UpdateDialect(ref connection, mysqlDialect);
        Assert.Equal(mysqlDialect, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithMysqlDialect_ReturnsRdsMysqlDialect()
    {
        var mysqlDialect = new MySqlDialect();
        DbConnection connection = this.mockConnection.Object;
        var result = this.dialectProvider.UpdateDialect(ref connection, mysqlDialect);
        Assert.Equal(mysqlDialect, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithNoMatchingDialectCandidate_ReturnsOriginalDialect()
    {
        var mysqlDialect = new MySqlDialect();
        DbConnection connection = this.mockConnection.Object;
        var result = this.dialectProvider.UpdateDialect(ref connection, mysqlDialect);
        Assert.Equal(mysqlDialect, result);
    }

    [Theory]
    [MemberData(nameof(GuessDialectTestData))]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithValidInputs_ReturnsExpectedDialect(Dictionary<string, string> props, Type expectedDialectType)
    {
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, props);
        var dialect = dialectProvider.GuessDialect();
        Assert.IsType(expectedDialectType, dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialectTest_WithCustomDialect()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.TargetDialect.Name, typeof(TestCustomDialect).AssemblyQualifiedName! } };
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, props);
        var dialect = dialectProvider.GuessDialect();
        Assert.IsType<TestCustomDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialectTest_WithAuroraMySql()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.Server.Name, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com" }, { PropertyDefinition.TargetConnectionType.Name, typeof(MySqlConnection).AssemblyQualifiedName! } };
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, props);
        var dialect = dialectProvider.GuessDialect();
        Assert.IsType<AuroraMySqlDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithInvalidCustomDialect_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.TargetDialect.Name, "InvalidDialectType" } };
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, props);
        Assert.Throws<InvalidOperationException>(() => dialectProvider.GuessDialect());
    }
}
