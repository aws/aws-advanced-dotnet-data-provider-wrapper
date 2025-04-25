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

public class HostSpecBuilderTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Build_WithValidHost_ShouldCreateHostSpec()
    {
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com");

        var hostSpec = builder.Build();

        Assert.Equal("test-host.example.com", hostSpec.Host);
        Assert.Equal(HostSpec.NoPort, hostSpec.Port);
        Assert.Equal(string.Empty, hostSpec.HostId);
        Assert.Equal(HostRole.Unknown, hostSpec.Role);
        Assert.Equal(HostAvailability.Available, hostSpec.RawAvailability);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Build_WithAllParameters_ShouldCreateHostSpecWithAllValues()
    {
        const string host = "test-host.example.com";
        const int port = 3306;
        const string hostId = "test-host-id";
        const HostRole role = HostRole.Writer;
        const HostAvailability availability = HostAvailability.Unavailable;

        var builder = new HostSpecBuilder()
            .WithHost(host)
            .WithPort(port)
            .WithHostId(hostId)
            .WithRole(role)
            .WithAvailability(availability);

        var hostSpec = builder.Build();

        Assert.Equal(host, hostSpec.Host);
        Assert.Equal(port, hostSpec.Port);
        Assert.Equal(hostId, hostSpec.HostId);
        Assert.Equal(role, hostSpec.Role);
        Assert.Equal(availability, hostSpec.RawAvailability);
        Assert.True(hostSpec.IsPortSpecified);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Build_WithNullHost_ShouldThrowArgumentException()
    {
        var builder = new HostSpecBuilder()
            .WithHost(null);

        var exception = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Equal("Host cannot be null or empty (Parameter '_host')", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Build_WithEmptyHost_ShouldThrowArgumentException()
    {
        var builder = new HostSpecBuilder()
            .WithHost(string.Empty);

        var exception = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Equal("Host cannot be null or empty (Parameter '_host')", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void WithPort_WithNullPort_ShouldSetNoPort()
    {
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithPort(null);

        var hostSpec = builder.Build();

        Assert.Equal(HostSpec.NoPort, hostSpec.Port);
        Assert.False(hostSpec.IsPortSpecified);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void WithRole_WithNullRole_ShouldSetUnknownRole()
    {
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithRole(null);

        var hostSpec = builder.Build();

        Assert.Equal(HostRole.Unknown, hostSpec.Role);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void WithAvailability_WithNullAvailability_ShouldKeepDefaultAvailability()
    {
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithAvailability(null);

        var hostSpec = builder.Build();

        Assert.Equal(HostAvailability.Available, hostSpec.RawAvailability);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void WithHostId_WithNullHostId_ShouldSetEmptyHostId()
    {
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithHostId(null);

        var hostSpec = builder.Build();

        Assert.Null(hostSpec.HostId);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FluentInterface_ShouldAllowChaining()
    {
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithPort(3306)
            .WithHostId("test-host-id")
            .WithRole(HostRole.Reader)
            .WithAvailability(HostAvailability.Available);

        Assert.NotNull(builder);
    }
}
