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
using AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.Tests;

public class AwsWrapperDbContextOptionsBuilderExtensionsTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void UseAwsWrapper_StoresRawWrapperConnectionString_OnExtension()
    {
        var pomeloConnectionString = "Server=localhost;Database=test;User ID=u;Password=p;";
        var wrapperConnectionString = "Server=localhost;Database=test;User ID=u;Password=p;Plugins=failover;AllowUserVariables=false;UseAffectedRows=true;";

        var builder = new DbContextOptionsBuilder();
        builder.UseAwsWrapper(
            wrapperConnectionString,
            wrapped => wrapped.UseMySql(pomeloConnectionString, new MySqlServerVersion(new Version(8, 0, 36))));

        var extension = builder.Options.Extensions.OfType<AwsWrapperOptionsExtension>().Single();
        Assert.Equal(wrapperConnectionString, extension.WrapperConnectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PomeloDialect_NormalizeConnectionString_EnforcesMandatoryMySqlOptions()
    {
        var input = "Server=localhost;Database=test;User ID=u;Password=p;Plugins=failover;AllowUserVariables=false;UseAffectedRows=true;";
        var normalized = PomeloEfMySqlRelationalConnectionDialect.Instance.NormalizeConnectionString(input);

        var connectionStringBuilder = new DbConnectionStringBuilder { ConnectionString = normalized };
        Assert.True(Convert.ToBoolean(connectionStringBuilder["AllowUserVariables"]));
        Assert.False(Convert.ToBoolean(connectionStringBuilder["UseAffectedRows"]));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void DetectEfMySqlProviderKind_WithPomeloUseMySql_ReturnsPomelo()
    {
        var pomeloConnectionString = "Server=localhost;Database=test;User ID=u;Password=p;";
        var wrapped = new DbContextOptionsBuilder()
            .UseMySql(pomeloConnectionString, new MySqlServerVersion(new Version(8, 0, 36)))
            .Options;

        var ext = wrapped.Extensions.First(x => x is not CoreOptionsExtension);
        Assert.Equal(EfMySqlProviderKind.Pomelo, RelationalConnectionDialectProvider.DetectEfMySqlProviderKind(ext));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetDialect_WithUnknownProviderKind_ThrowsInvalidOperationException()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RelationalConnectionDialectProvider.GetDialect(EfMySqlProviderKind.Unknown, wrappedExtensionForDiagnostics: null));
        Assert.Contains("relational connection", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("not supported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
