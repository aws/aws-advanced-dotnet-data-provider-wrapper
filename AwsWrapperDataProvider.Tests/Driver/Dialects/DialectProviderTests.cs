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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;
using Npgsql;
using MySqlConnection = MySqlConnector.MySqlConnection;
using MySqlDataMySqlConnection = MySql.Data.MySqlClient.MySqlConnection;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectProviderTests
{
    private readonly Mock<PluginService> mockPluginService;

    private readonly Mock<IDbConnection> mockConnection;
    private readonly Mock<IDbCommand> mockCommand;
    private readonly Mock<IDataReader> mockReader;

    private readonly Dictionary<string, string> defaultProps = new();

    public DialectProviderTests()
    {
        this.mockPluginService = new Mock<PluginService>();
        this.mockPluginService.Object.InitialConnectionHostSpec = new HostSpecBuilder().WithHost("anyHost").Build();

        this.mockConnection = new Mock<IDbConnection>();
        this.mockConnection.SetupAllProperties();
        this.mockConnection.Object.ConnectionString = "anyHost";
        this.mockConnection.Setup(conn => conn.State).Returns(ConnectionState.Open);
        this.mockCommand = new Mock<IDbCommand>();
        this.mockReader = new Mock<IDataReader>();
        this.mockConnection.Setup(c => c.CreateCommand()).Returns(this.mockCommand.Object);
        this.mockCommand.Setup(c => c.ExecuteReader()).Returns(this.mockReader.Object);
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
                { PropertyDefinition.TargetConnectionType.Name, typeof(MySqlDataMySqlConnection).AssemblyQualifiedName! },
            }, typeof(MySqlDialect)
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
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, props);
        var dialect = dialectProvider.GuessDialect();
        Assert.IsType(dialectType, dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithInvalidCustomDialect_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.TargetDialect.Name, "NonExistentType" }, };
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, props);
        Assert.Throws<InvalidOperationException>(() => dialectProvider.GuessDialect());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_PgToRdsPg()
    {
        string query = string.Empty;
        this.mockCommand.SetupSet(c => c.CommandText = It.IsAny<string>()).Callback<string>(commandText => query = commandText);
        this.mockReader.Setup(reader => reader.Read()).Returns(() => query switch
        {
            "SELECT 1 FROM pg_proc LIMIT 1" => true,
            "SELECT (setting LIKE '%rds_tools%') AS rds_tools, (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils FROM pg_settings WHERE name='rds.extensions'" => true,
            _ => false,
        });
        this.mockReader.Setup(reader => reader.GetOrdinal("rds_tools")).Returns(0);
        this.mockReader.Setup(reader => reader.GetBoolean(0)).Returns(true);
        this.mockReader.Setup(reader => reader.GetOrdinal("aurora_stat_utils")).Returns(1);
        this.mockReader.Setup(reader => reader.GetBoolean(1)).Returns(false);

        var initialDialect = new PgDialect();
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        var updatedDialect = dialectProvider.UpdateDialect(this.mockConnection.Object, initialDialect);
        Assert.IsType<RdsPgDialect>(updatedDialect);
        this.mockConnection.Verify(c => c.CreateCommand(), Times.Exactly(5));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithNoMatchingDialectCandidate_ReturnsOriginalDialect()
    {
        this.mockReader.Setup(r => r.Read()).Returns(false);

        var initialDialectMock = new Mock<IDialect>();
        initialDialectMock.Setup(d => d.DialectUpdateCandidates).Returns(
        [
            typeof(AuroraPgDialect),
            typeof(PgDialect),
        ]);
        initialDialectMock.Setup(d => d.IsDialect(this.mockConnection.Object)).Returns(true);

        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        var updatedDialect = dialectProvider.UpdateDialect(this.mockConnection.Object, initialDialectMock.Object);

        Assert.Same(initialDialectMock.Object, updatedDialect);
        this.mockConnection.Verify(c => c.CreateCommand(), Times.AtLeastOnce);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithUnknownDialect_ThrowsArgumentException()
    {
        var unknownDialect = new UnknownDialect();

        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        Assert.Throws<ArgumentException>(() =>
            dialectProvider.UpdateDialect(this.mockConnection.Object, unknownDialect));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithUnknownDialect_ReturnsMysqlDialect()
    {
        string query = string.Empty;
        this.mockCommand.SetupSet(c => c.CommandText = It.IsAny<string>()).Callback<string>(commandText => query = commandText);
        this.mockReader.Setup(r => r.Read()).Returns(() => query switch
            {
                "SHOW VARIABLES LIKE 'version_comment'" => true,
                _ => false,
            });
        this.mockReader.Setup(r => r.FieldCount).Returns(1);
        this.mockReader.Setup(r => r.GetString(0)).Returns("MySQL Community Server (GPL)");
        var unknownDialect = new UnknownDialect();

        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        var updatedDialect = dialectProvider.UpdateDialect(this.mockConnection.Object, unknownDialect);
        Assert.IsType<MySqlDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithMysqlDialect_ReturnsAuroraMysqlDialect()
    {
        string query = string.Empty;
        this.mockCommand.SetupSet(c => c.CommandText = It.IsAny<string>()).Callback<string>(commandText => query = commandText);
        this.mockReader.Setup(r => r.Read()).Returns(() => query switch
        {
            "SHOW VARIABLES LIKE 'aurora_version'" => true,
            _ => false,
        });
        this.mockReader.Setup(r => r.FieldCount).Returns(1);
        this.mockReader.Setup(r => r.GetString(0)).Returns("Source distribution");

        var mysqlDialect = new MySqlDialect();

        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        var updatedDialect = dialectProvider.UpdateDialect(this.mockConnection.Object, mysqlDialect);

        Assert.IsType<AuroraMySqlDialect>(updatedDialect);
        this.mockConnection.Verify(c => c.CreateCommand(), Times.Exactly(2)); // Updated to account for RdsMultiAzDbClusterListProvider check
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithMysqlDialect_ReturnsRdsMysqlDialect()
    {
        var returnSequence = new List<bool>([true, false]);
        int returnIndex = 0;
        string query = string.Empty;
        this.mockCommand.SetupSet(c => c.CommandText = It.IsAny<string>()).Callback<string>(commandText => query = commandText);
        this.mockReader.Setup(r => r.Read()).Returns(() =>
        {
            switch (query)
            {
                case "SHOW VARIABLES LIKE 'version_comment'":
                    {
                        var result = returnSequence[returnIndex];
                        returnIndex = (returnIndex + 1) % returnSequence.Count;
                        return result;
                    }

                default:
                    return false;
            }
        });
        this.mockReader.Setup(r => r.FieldCount).Returns(1);
        this.mockReader.Setup(r => r.GetString(0)).Returns("Source distribution");

        var mysqlDialect = new MySqlDialect();

        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        var updatedDialect = dialectProvider.UpdateDialect(this.mockConnection.Object, mysqlDialect);

        Assert.IsType<RdsMySqlDialect>(updatedDialect);
        this.mockConnection.Verify(c => c.CreateCommand(), Times.Exactly(4));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_CannotUpdate_ReturnsSameDialect()
    {
        this.mockCommand.Setup(c => c.ExecuteReader()).Throws(new MockDbException());

        var mysqlDialect = new MySqlDialect();
        var dialectProvider = new DialectProvider(this.mockPluginService.Object, this.defaultProps);
        var updatedDialect = dialectProvider.UpdateDialect(this.mockConnection.Object, mysqlDialect);
        Assert.IsType<MySqlDialect>(updatedDialect);
        this.mockConnection.Verify(c => c.CreateCommand(), Times.AtLeastOnce);
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

        public bool IsDialect(IDbConnection conn) => true;

        public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
        {
        }
    }
}
