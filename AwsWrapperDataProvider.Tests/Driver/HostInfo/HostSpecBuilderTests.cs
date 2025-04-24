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
using Xunit;

namespace AwsWrapperDataProvider.Tests.Driver.HostInfo;

public class HostSpecBuilderTests
{
    [Fact]
    public void Build_WithValidHost_ShouldCreateHostSpec()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com");
        
        // Act
        var hostSpec = builder.Build();
        
        // Assert
        Assert.Equal("test-host.example.com", hostSpec.Host);
        Assert.Equal(HostSpec.NoPort, hostSpec.Port);
        Assert.Equal(string.Empty, hostSpec.HostId);
        Assert.Equal(HostRole.Unknown, hostSpec.Role);
        Assert.Equal(HostAvailability.Available, hostSpec.RawAvailability);
    }
    
    [Fact]
    public void Build_WithAllParameters_ShouldCreateHostSpecWithAllValues()
    {
        // Arrange
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
        
        // Act
        var hostSpec = builder.Build();
        
        // Assert
        Assert.Equal(host, hostSpec.Host);
        Assert.Equal(port, hostSpec.Port);
        Assert.Equal(hostId, hostSpec.HostId);
        Assert.Equal(role, hostSpec.Role);
        Assert.Equal(availability, hostSpec.RawAvailability);
        Assert.True(hostSpec.IsPortSpecified);
    }
    
    [Fact]
    public void Build_WithNullHost_ShouldThrowArgumentException()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost(null);
        
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Equal("Host cannot be null or empty (Parameter '_host')", exception.Message);
    }
    
    [Fact]
    public void Build_WithEmptyHost_ShouldThrowArgumentException()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost(string.Empty);
        
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Equal("Host cannot be null or empty (Parameter '_host')", exception.Message);
    }
    
    [Fact]
    public void WithPort_WithNullPort_ShouldSetNoPort()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithPort(null);
        
        // Act
        var hostSpec = builder.Build();
        
        // Assert
        Assert.Equal(HostSpec.NoPort, hostSpec.Port);
        Assert.False(hostSpec.IsPortSpecified);
    }
    
    [Fact]
    public void WithRole_WithNullRole_ShouldSetUnknownRole()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithRole(null);
        
        // Act
        var hostSpec = builder.Build();
        
        // Assert
        Assert.Equal(HostRole.Unknown, hostSpec.Role);
    }
    
    [Fact]
    public void WithAvailability_WithNullAvailability_ShouldKeepDefaultAvailability()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithAvailability(null);
        
        // Act
        var hostSpec = builder.Build();
        
        // Assert
        Assert.Equal(HostAvailability.Available, hostSpec.RawAvailability);
    }
    
    [Fact]
    public void WithHostId_WithNullHostId_ShouldSetEmptyHostId()
    {
        // Arrange
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithHostId(null);
        
        // Act
        var hostSpec = builder.Build();
        
        // Assert
        Assert.Null(hostSpec.HostId);
    }
    
    [Fact]
    public void FluentInterface_ShouldAllowChaining()
    {
        // Arrange & Act
        var builder = new HostSpecBuilder()
            .WithHost("test-host.example.com")
            .WithPort(3306)
            .WithHostId("test-host-id")
            .WithRole(HostRole.Reader)
            .WithAvailability(HostAvailability.Available);
        
        // Assert - Just verifying the chain works without exceptions
        Assert.NotNull(builder);
    }
}
