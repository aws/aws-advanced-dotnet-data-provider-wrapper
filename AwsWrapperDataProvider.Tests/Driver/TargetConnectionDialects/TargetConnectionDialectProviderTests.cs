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
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.TargetConnectionDialects;

public class TargetConnectionDialectProviderTests
{
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(typeof(NpgsqlConnection), typeof(PgTargetConnectionDialect))]
    [InlineData(typeof(MySqlConnection), typeof(MySqlTargetConnectionDialect))]
    public void GetDialect_WithSupportedConnectionType_ReturnsTargetDriverDialect(Type connectionType, Type dialectType)
    {
        var dialect = TargetConnectionDialectProvider.GetDialect(connectionType, null);
        Assert.NotNull(dialect);
        Assert.IsType(dialectType, dialect);
        Assert.Equal(connectionType, dialect.DriverConnectionType);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetDialect_WithUnsupportedConnectionType_ThrowsNotSupportedException()
    {
        var exception = Assert.Throws<NotSupportedException>(() =>
            TargetConnectionDialectProvider.GetDialect(typeof(AwsWrapperConnection), null));

        Assert.Contains("No dialect found for connection type", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetDialect_WithValidCustomDialect_ReturnsCustomDialect()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.CustomTargetConnectionDialect.Name, typeof(TestCustomDialect).AssemblyQualifiedName! },
        };

        var dialect = TargetConnectionDialectProvider.GetDialect(typeof(NpgsqlConnection), props);

        Assert.NotNull(dialect);
        Assert.IsType<TestCustomDialect>(dialect);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetDialect_WithInvalidCustomDialect_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.CustomTargetConnectionDialect.Name, typeof(TestCustomDialect).AssemblyQualifiedName! },
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            TargetConnectionDialectProvider.GetDialect(typeof(MySqlConnection), props));

        Assert.Contains("Failed to instantiate custom dialect type", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetDialect_WithInvalidCustomDialectTypeName_ThrowsInvalidOperationException()
    {
        var props = new Dictionary<string, string>
        {
            { PropertyDefinition.CustomTargetConnectionDialect.Name, "InvalidTypeName" },
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            TargetConnectionDialectProvider.GetDialect(typeof(NpgsqlConnection), props));

        Assert.Contains("Failed to instantiate custom dialect type", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetDialect_WithEmptyProperties_UsesDefaultDialect()
    {
        var props = new Dictionary<string, string>();
        var dialect = TargetConnectionDialectProvider.GetDialect(typeof(NpgsqlConnection), props);
        Assert.NotNull(dialect);
        Assert.IsType<PgTargetConnectionDialect>(dialect);
    }

    // Test custom dialect implementation
    public class TestCustomDialect : ITargetConnectionDialect
    {
        public Type DriverConnectionType => typeof(NpgsqlConnection);

        public bool IsDialect(Type connectionType) => connectionType == typeof(NpgsqlConnection);

        public string? GetSqlState(DbException exception) => string.Empty;

        public ISet<string> GetAllowedOnConnectionMethodNames() => new HashSet<string>();

        public string PrepareConnectionString(AwsWrapperDataProvider.Driver.HostInfo.HostSpec? hostSpec, Dictionary<string, string> props)
        {
            return "TestConnectionString";
        }
    }
}
