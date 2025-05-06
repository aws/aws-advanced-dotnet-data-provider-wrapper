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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.Dialects;

public class DialectProviderTests
{
    [Fact]
    public void GuessDialect_WithCustomDialect_ReturnsCustomDialect()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.CustomDialect.Name, typeof(TestCustomDialect).AssemblyQualifiedName! },
        };

        var dialect = DialectProvider.GuessDialect(props);
        Assert.IsType<TestCustomDialect>(dialect);
    }

    [Fact]
    public void GuessDialect_WithInvalidCustomDialect_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string> { { PropertyDefinition.CustomDialect.Name, "NonExistentType" }, };
        Assert.Throws<InvalidOperationException>(() => DialectProvider.GuessDialect(props));
    }

    [Fact]
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
    public void UpdateDialect_WithMatchingDialectCandidate_ReturnsUpdatedDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);
        mockReader.Setup(r => r.HasRows).Returns(true);

        var initialDialect = new PgDialect();
        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, initialDialect);
        Assert.IsType<AuroraPgDialect>(updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.Once);
    }

    [Fact]
    public void UpdateDialect_WithNoMatchingDialectCandidate_ReturnsOriginalDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);
        mockReader.Setup(r => r.HasRows).Returns(false);

        var initialDialect = new PgDialect();
        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, initialDialect);

        Assert.Same(initialDialect, updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.AtLeastOnce);
    }

    [Fact]
    public void UpdateDialect_WithUnknownDialect_ThrowsArgumentException()
    {
        var mockConnection = new Mock<IDbConnection>();
        var unknownDialect = new UnknownDialect();

        Assert.Throws<NullReferenceException>(() =>
            DialectProvider.UpdateDialect(mockConnection.Object, unknownDialect));
    }

    [Fact]
    public void UpdateDialect_WithMysqlDialect_ReturnsAuroraMysqlDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();
        var mockReader = new Mock<DbDataReader>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Returns(mockReader.Object);

        mockReader.Setup(r => r.Read()).Returns(true);
        mockReader.Setup(r => r.FieldCount).Returns(1);
        mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockReader.Setup(r => r.GetString(0)).Returns("MySQL Community Server - Aurora");

        var mysqlDialect = new MysqlDialect();
        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, mysqlDialect);

        Assert.IsType<AuroraMysqlDialect>(updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.Once);
    }

    [Fact]
    public void UpdateDialect_WithMysqlDialect_ReturnsRdsMysqlDialect()
    {
        var mockConnection = new Mock<IDbConnection>();

        var mockCommand1 = new Mock<IDbCommand>();
        var mockReader1 = new Mock<DbDataReader>();
        mockCommand1.Setup(c => c.ExecuteReader()).Returns(mockReader1.Object);
        mockReader1.Setup(r => r.Read()).Returns(false);

        // Second command for RdsMysqlDialect check (succeeds)
        var mockCommand2 = new Mock<IDbCommand>();
        var mockReader2 = new Mock<DbDataReader>();
        mockCommand2.Setup(c => c.ExecuteReader()).Returns(mockReader2.Object);
        mockReader2.Setup(r => r.Read()).Returns(true);
        mockReader2.Setup(r => r.FieldCount).Returns(1);
        mockReader2.Setup(r => r.IsDBNull(0)).Returns(false);
        mockReader2.Setup(r => r.GetString(0)).Returns("MySQL on RDS");

        // Setup the connection to return different commands on subsequent calls
        mockConnection.SetupSequence(c => c.CreateCommand())
            .Returns(mockCommand1.Object)
            .Returns(mockCommand2.Object);

        var mysqlDialect = new MysqlDialect();

        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, mysqlDialect);

        Assert.IsType<RdsMysqlDialect>(updatedDialect);
        mockConnection.Verify(c => c.CreateCommand(), Times.Exactly(2));
    }

    [Fact]
    public void UpdateDialect_WithMysqlDialect_HandlesExceptionAndReturnsOriginalDialect()
    {
        var mockConnection = new Mock<IDbConnection>();
        var mockCommand = new Mock<IDbCommand>();

        mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
        mockCommand.Setup(c => c.ExecuteReader()).Throws(new Exception("Connection error"));

        var mysqlDialect = new MysqlDialect();
        var updatedDialect = DialectProvider.UpdateDialect(mockConnection.Object, mysqlDialect);

        Assert.Same(mysqlDialect, updatedDialect);
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

        public bool IsDialect(IDbConnection conn) => true;

        public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
        {
        }
    }
}
