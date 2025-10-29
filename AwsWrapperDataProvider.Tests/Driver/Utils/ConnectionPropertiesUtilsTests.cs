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

using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

public class ConnectionPropertiesUtilsTests
{
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("host=myhost.example.com;port=5432;database=mydb;username=myuser;password=mypassword", 5)]
    [InlineData("Host=myhost.example.com;Port=5432", 2)]
    [InlineData("Host=myhost.example.com", 1)]
    [InlineData("host=NOT-myhost.example.com;Host=myhost.example.com", 1)]
    public void ParseConnectionStringParameters_WithValidConnectionString_ReturnsDictionary(string connectionString, int expectedCount)
    {
        var result = ConnectionPropertiesUtils.ParseConnectionStringParameters(connectionString);

        Assert.NotNull(result);
        Assert.Equal(expectedCount, result.Count);
        Assert.Equal("myhost.example.com", result["Host"]);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("")]
    [InlineData(null)]
    public void ParseConnectionStringParameters_WithInvalidConnectionString_ThrowsArgumentNullException(string? connectionString)
    {
        Assert.Throws<ArgumentNullException>(() => ConnectionPropertiesUtils.ParseConnectionStringParameters(connectionString!));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("Host=myhost.example.com;InvalidPair;=;Port=5432;=NoKey;NoValue=", 3)]
    [InlineData("Host=myhost.example.com;=NoKey;NoValue=", 2)]
    public void ParseConnectionStringParameters_WithMalformedPairs_SkipsInvalidPairs(string connectionString, int expectedCount)
    {
        var result = ConnectionPropertiesUtils.ParseConnectionStringParameters(connectionString);

        Assert.NotNull(result);
        Assert.Equal(expectedCount, result.Count);
        Assert.Equal("myhost.example.com", result["Host"]);
        Assert.False(result.ContainsKey("InvalidPair"));
        Assert.False(result.ContainsKey(string.Empty));
        Assert.True(result.ContainsKey("NoValue"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ParseConnectionStringParameters_WithExtraWhitespace_TrimsValues()
    {
        string connectionString = " Host = myhost.example.com ; Port = 5432 ";

        var result = ConnectionPropertiesUtils.ParseConnectionStringParameters(connectionString);

        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("myhost.example.com", result["Host"]);
        Assert.Equal("5432", result["Port"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithRdsInstanceEndpoint_ReturnsHostSpec()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithRdsWriterClusterEndpoint_ReturnsHostSpec()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.cluster-123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.cluster-123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithRdsReaderClusterEndpoint_ReturnsHostSpecWithReaderRole()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.cluster-ro-123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.cluster-ro-123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb",
            HostRole.Reader,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithRdsCustomClusterEndpoint_ReturnsHostSpec()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.cluster-custom-123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.cluster-custom-123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithRdsProxyEndpoint_ReturnsHostSpec()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.proxy-123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.proxy-123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithServerInsteadOfHost_UsesServerValue()
    {
        var props = new Dictionary<string, string>
        {
            { "Server", "mydb.123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithMultipleRdsEndpoints_ReturnsMultipleHostSpecs()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb1.123456789012.us-east-1.rds.amazonaws.com,mydb2.123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost1 = new HostSpec(
            "mydb1.123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb1",
            HostRole.Writer,
            HostAvailability.Available);

        var expectedHost2 = new HostSpec(
            "mydb2.123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb2",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost1, expectedHost2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_SingleWriterConnectionString()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb1.123456789012.us-east-1.rds.amazonaws.com,mydb2.123456789012.us-east-1.rds.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost1 = new HostSpec(
            "mydb1.123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb1",
            HostRole.Writer,
            HostAvailability.Available);

        var expectedHost2 = new HostSpec(
            "mydb2.123456789012.us-east-1.rds.amazonaws.com",
            3306,
            "mydb2",
            HostRole.Reader,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, true, expectedHost1, expectedHost2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithHostAndPortInHostString_UsesPortFromHostString()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.123456789012.us-east-1.rds.amazonaws.com:3307" },
            { "Port", "3306" }, // This should be overridden by the port in the host string
        };

        var expectedHost = new HostSpec(
            "mydb.123456789012.us-east-1.rds.amazonaws.com",
            3307,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithChinaRegionRdsEndpoint_ReturnsHostSpec()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.123456789012.cn-north-1.rds.amazonaws.com.cn" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.123456789012.cn-north-1.rds.amazonaws.com.cn",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithGovCloudRdsEndpoint_ReturnsHostSpec()
    {
        var props = new Dictionary<string, string>
        {
            { "Host", "mydb.123456789012.rds.us-gov-west-1.amazonaws.com" },
            { "Port", "3306" },
        };

        var expectedHost = new HostSpec(
            "mydb.123456789012.rds.us-gov-west-1.amazonaws.com",
            3306,
            "mydb",
            HostRole.Writer,
            HostAvailability.Available);

        this.AssertHostsFromProperties(props, false, expectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHostsFromProperties_WithNoHostOrServer_ReturnsEmptyList()
    {
        var props = new Dictionary<string, string>
        {
            { "Port", "3306" },
            { "Database", "mydb" },
        };

        this.AssertHostsFromProperties(props, false);
    }

    private void AssertHostsFromProperties(
        Dictionary<string, string> props,
        bool singleWriterConnectionString,
        params HostSpec[] expectedHosts)
    {
        var hostSpecBuilder = new HostSpecBuilder();
        var result = ConnectionPropertiesUtils.GetHostsFromProperties(props, hostSpecBuilder, singleWriterConnectionString);

        Assert.NotNull(result);
        Assert.Equal(expectedHosts.Length, result.Count);

        for (int i = 0; i < expectedHosts.Length; i++)
        {
            Assert.Equal(expectedHosts[i], result[i]);
        }
    }
}
