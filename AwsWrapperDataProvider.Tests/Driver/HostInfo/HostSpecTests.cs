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

namespace AwsWrapperDataProvider.Tests.Driver.HostInfo;

public class HostSpecTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_ShouldInitializeProperties()
    {
        const string host = "test-host.example.com";
        const int port = 3306;
        const string hostId = "test-host-id";
        const HostRole role = HostRole.Writer;
        const HostAvailability availability = HostAvailability.Available;

        var hostSpec = new HostSpec(host, port, hostId, role, availability);

        Assert.Equal(host, hostSpec.Host);
        Assert.Equal(port, hostSpec.Port);
        Assert.Equal(hostId, hostSpec.HostId);
        Assert.Equal(role, hostSpec.Role);
        Assert.Equal(availability, hostSpec.RawAvailability);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsPortSpecified_WithNoPort_ShouldReturnFalse()
    {
        var hostSpec = new HostSpec("test-host.example.com", HostSpec.NoPort, "test-host-id", HostRole.Unknown, HostAvailability.Available);

        Assert.False(hostSpec.IsPortSpecified);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsPortSpecified_WithValidPort_ShouldReturnTrue()
    {
        var hostSpec = new HostSpec("test-host.example.com", 3306, "test-host-id", HostRole.Unknown, HostAvailability.Available);

        Assert.True(hostSpec.IsPortSpecified);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Equals_WithNull_ShouldReturnFalse()
    {
        var hostSpec = new HostSpec("test-host.example.com", 3306, "test-host-id", HostRole.Writer, HostAvailability.Available);

        Assert.False(hostSpec.Equals(null));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Equals_WithDifferentType_ShouldReturnFalse()
    {
        var hostSpec = new HostSpec("test-host.example.com", 3306, "test-host-id", HostRole.Writer, HostAvailability.Available);
        var differentType = new object();

        Assert.False(hostSpec.Equals(differentType));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Equals_WithIdenticalValues_ShouldReturnTrue()
    {
        var hostSpec1 = new HostSpec("test-host.example.com", 3306, "test-host-id-1", HostRole.Writer, HostAvailability.Available);
        var hostSpec2 = new HostSpec("test-host.example.com", 3306, "test-host-id-2", HostRole.Writer, HostAvailability.Available);

        Assert.True(hostSpec1.Equals(hostSpec2));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("host1.example.com", "host2.example.com", 3306, 3306, HostRole.Writer, HostRole.Writer, HostAvailability.Available, HostAvailability.Available, false)]
    [InlineData("host1.example.com", "host1.example.com", 3306, 3307, HostRole.Writer, HostRole.Writer, HostAvailability.Available, HostAvailability.Available, false)]
    [InlineData("host1.example.com", "host1.example.com", 3306, 3306, HostRole.Writer, HostRole.Reader, HostAvailability.Available, HostAvailability.Available, false)]
    [InlineData("host1.example.com", "host1.example.com", 3306, 3306, HostRole.Writer, HostRole.Writer, HostAvailability.Available, HostAvailability.Unavailable, false)]
    [InlineData("host1.example.com", "host1.example.com", 3306, 3306, HostRole.Writer, HostRole.Writer, HostAvailability.Available, HostAvailability.Available, true)]
    public void Equals_WithDifferentValues_ShouldReturnExpectedResult(
        string host1, string host2, int port1, int port2, HostRole role1, HostRole role2, HostAvailability availability1, HostAvailability availability2, bool expectedResult)
    {
        var hostSpec1 = new HostSpec(host1, port1, "id1", role1, availability1);
        var hostSpec2 = new HostSpec(host2, port2, "id2", role2, availability2);

        Assert.Equal(expectedResult, hostSpec1.Equals(hostSpec2));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHashCode_WithSameValues_ShouldReturnSameHashCode()
    {
        var hostSpec1 = new HostSpec("test-host.example.com", 3306, "id1", HostRole.Writer, HostAvailability.Available);
        var hostSpec2 = new HostSpec("test-host.example.com", 3306, "id2", HostRole.Writer, HostAvailability.Available);

        var hashCode1 = hostSpec1.GetHashCode();
        var hashCode2 = hostSpec2.GetHashCode();

        Assert.Equal(hashCode1, hashCode2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetHashCode_WithDifferentValues_ShouldReturnDifferentHashCodes()
    {
        var hostSpec1 = new HostSpec("test-host1.example.com", 3306, "id1", HostRole.Writer, HostAvailability.Available);
        var hostSpec2 = new HostSpec("test-host2.example.com", 3306, "id2", HostRole.Writer, HostAvailability.Available);

        var hashCode1 = hostSpec1.GetHashCode();
        var hashCode2 = hostSpec2.GetHashCode();

        Assert.NotEqual(hashCode1, hashCode2);
    }
}
