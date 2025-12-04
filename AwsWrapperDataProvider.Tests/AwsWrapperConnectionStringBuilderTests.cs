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

using System.Reflection;
using AwsWrapperDataProvider.Driver.Utils;
using Xunit;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperConnectionStringBuilderTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void AllPropertyDefinitionsAreImplemented()
    {
        var propertyDefinitions = typeof(PropertyDefinition)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(AwsWrapperProperty))
            .Select(f => (AwsWrapperProperty)f.GetValue(null)!)
            .ToList();

        var builderProperties = typeof(AwsWrapperConnectionStringBuilder)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var missingProperties = propertyDefinitions
            .Where(pd => !builderProperties.Contains(pd.Name))
            .Select(pd => pd.Name)
            .ToList();

        Assert.Empty(missingProperties);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionString_OnlyIncludesSetProperties()
    {
        var builder = new AwsWrapperConnectionStringBuilder
        {
            Host = "mydb.com",
            Port = 5432,
            Plugins = "failover,iam",
        };

        var connectionString = builder.ConnectionString;

        Assert.Contains("Host=mydb.com", connectionString);
        Assert.Contains("Port=5432", connectionString);
        Assert.Contains("Plugins=failover,iam", connectionString);
        Assert.DoesNotContain("FailoverTimeoutMs", connectionString);
        Assert.DoesNotContain("IamRegion", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ConnectionString_ParsesExistingConnectionString()
    {
        var builder = new AwsWrapperConnectionStringBuilder
        {
            ConnectionString = "Host=test.rds.amazonaws.com;Port=3306;User=admin;Password=secret;Plugins=failover",
        };

        Assert.Equal("test.rds.amazonaws.com", builder.Host);
        Assert.Equal(3306, builder.Port);
        Assert.Equal("admin", builder.User);
        Assert.Equal("secret", builder.Password);
        Assert.Equal("failover", builder.Plugins);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Properties_ReturnNullWhenNotSet()
    {
        var builder = new AwsWrapperConnectionStringBuilder();

        Assert.Null(builder.Host);
        Assert.Null(builder.Port);
        Assert.Null(builder.FailoverTimeoutMs);
        Assert.Null(builder.AutoSortPluginOrder);
        Assert.Null(builder.IamRegion);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetPropertyToNull_RemovesFromConnectionString()
    {
        var builder = new AwsWrapperConnectionStringBuilder
        {
            Host = "mydb.com",
            Port = 5432,
            Plugins = "failover",
        };

        builder.Plugins = null;

        var connectionString = builder.ConnectionString;
        Assert.Contains("Host=mydb.com", connectionString);
        Assert.DoesNotContain("Plugins", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void BoolProperties_WorkCorrectly()
    {
        var builder = new AwsWrapperConnectionStringBuilder
        {
            AutoSortPluginOrder = true,
            EnableConnectFailover = false,
            FailureDetectionEnabled = true,
        };

        Assert.True(builder.AutoSortPluginOrder);
        Assert.False(builder.EnableConnectFailover);
        Assert.True(builder.FailureDetectionEnabled);

        var connectionString = builder.ConnectionString;
        Assert.Contains("AutoSortPluginOrder=True", connectionString);
        Assert.Contains("EnableConnectFailover=False", connectionString);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IntProperties_WorkCorrectly()
    {
        var builder = new AwsWrapperConnectionStringBuilder
        {
            Port = 3306,
            FailoverTimeoutMs = 60000,
            IamExpiration = 900,
        };

        Assert.Equal(3306, builder.Port);
        Assert.Equal(60000, builder.FailoverTimeoutMs);
        Assert.Equal(900, builder.IamExpiration);

        var connectionString = builder.ConnectionString;
        Assert.Contains("Port=3306", connectionString);
        Assert.Contains("FailoverTimeoutMs=60000", connectionString);
        Assert.Contains("IamExpiration=900", connectionString);
    }
}
