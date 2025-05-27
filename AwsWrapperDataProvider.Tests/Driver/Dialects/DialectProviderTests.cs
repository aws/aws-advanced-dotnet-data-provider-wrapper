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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectProviderTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithCustomDialect_ReturnsCustomDialect()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.TargetDialect.Name, typeof(TestCustomDialect).AssemblyQualifiedName! },
        };

        var dialect = DialectProvider.GuessDialect(props);
        Assert.IsType<TestCustomDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithInvalidCustomDialect_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.TargetDialect.Name, "NonExistentType" }, };
        Assert.Throws<InvalidOperationException>(() => DialectProvider.GuessDialect(props));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithPgConnectionAndIpAddress_ReturnsPgDialect()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.Host.Name, "192.168.1.1" },
            { PropertyDefinition.TargetConnectionType.Name, typeof(NpgsqlConnection).AssemblyQualifiedName! },
        };
        var dialect = DialectProvider.GuessDialect(props);
        Assert.IsType<PgDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithMySqlConnectionAndRdsInstance_ReturnsMysqlDialect()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.Server.Name, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com" },
            { PropertyDefinition.TargetConnectionType.Name, typeof(MySqlConnection).AssemblyQualifiedName! },
        };
        var dialect = DialectProvider.GuessDialect(props);
        Assert.IsType<MysqlDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GuessDialect_WithUnknownMapping_ReturnsUnknownDialect()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.Server.Name, "some-unknown-host.com" },
            { PropertyDefinition.TargetConnectionType.Name, typeof(DbConnection).AssemblyQualifiedName! },
        };
        var dialect = DialectProvider.GuessDialect(props);
        Assert.IsType<UnknownDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_PgToRdsPg()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);
        mockReader.Setup(r => r.HasRows).Returns(true);
        mockReader.SetupSequence(reader => reader.Read()).Returns(true).Returns(false);
        mockReader.Setup(reader => reader.GetOrdinal("rds_tools")).Returns(0);
        mockReader.Setup(reader => reader.GetBoolean(0)).Returns(true);
        mockReader.Setup(reader => reader.GetOrdinal("aurora_stat_utils")).Returns(1);
        mockReader.Setup(reader => reader.GetBoolean(1)).Returns(false);

        var initialDialect = new PgDialect();
        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, initialDialect);
        Assert.IsType<RdsPgDialect>(updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.Exactly(2));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithNoMatchingDialectCandidate_ReturnsOriginalDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);
        mockReader.Setup(r => r.HasRows).Returns(false);

        var initialDialectMock = new Mock<IDialect>();
        initialDialectMock.Setup(d => d.DialectUpdateCandidates).Returns(
        [
            typeof(AuroraPgDialect),
            typeof(PgDialect),
        ]);
        initialDialectMock.Setup(d => d.IsDialect(mockConnection.Object)).Returns(true);
        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, initialDialectMock.Object);

        Assert.Same(initialDialectMock.Object, updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.AtLeastOnce);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithUnknownDialect_ThrowsArgumentException()
    {
        var mockConnection = new Mock<IDbConnection>();
        var unknownDialect = new UnknownDialect();

        Assert.Throws<ArgumentException>(() =>
            DialectProvider.UpdateDialect(mockConnection.Object, unknownDialect));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithUnknownDialect_ReturnsMysqlDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();
        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);
        mockReader.Setup(r => r.Read()).Returns(true);
        mockReader.Setup(r => r.FieldCount).Returns(1);
        mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockReader.Setup(r => r.GetString(0)).Returns("MySQL Community Server (GPL)");
        var unknownDialect = new UnknownDialect();

        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, unknownDialect);
        Assert.IsType<MysqlDialect>(updatedDialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithMysqlDialect_ReturnsAuroraMysqlDialect()
    {
        // TODO: Implement after AuroraMysqlDialect.IsDialect is implemented
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithMysqlDialect_ReturnsRdsMysqlDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();
        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);
        mockReader.SetupSequence(r => r.Read()).Returns(true).Returns(false).Returns(true).Returns(false);
        mockReader.Setup(r => r.FieldCount).Returns(1);
        mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockReader.Setup(r => r.GetString(0)).Returns("Source distribution");

        var mysqlDialect = new MysqlDialect();

        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, mysqlDialect);

        Assert.IsType<RdsMysqlDialect>(updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.Exactly(2));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UpdateDialect_WithInvalidMysqlDialect_ThrowsArgumentError()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Throws(new Exception("Connection error"));

        var mysqlDialect = new MysqlDialect();
        Assert.Throws<ArgumentException>(() => DialectProvider.UpdateDialect(mockConnection.Object, mysqlDialect));
        mockConnection.Verify(c => c.CreateCommand(), Times.AtLeastOnce);
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
