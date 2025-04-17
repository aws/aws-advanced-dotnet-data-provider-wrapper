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

namespace AwsWrapperDataProvider.driver.hostInfo;

/// <summary>
/// Builder class for creating HostSpec instances.
/// </summary>
public class HostSpecBuilder
{
    private string _host = string.Empty;
    private int _port = HostSpec.NoPort;
    private string _hostId = string.Empty;
    private HostRole _role = HostRole.Unknown;
    private HostAvailability _availability = HostAvailability.Available;
    
    public HostSpecBuilder WithHost(string host)
    {
        _host = host;
        return this;
    }
    
    public HostSpecBuilder WithPort(int port)
    {
        _port = port;
        return this;
    }
    
    public HostSpecBuilder WithHostId(string hostId)
    {
        _hostId = hostId;
        return this;
    }

    public HostSpecBuilder WithRole(HostRole role)
    {
        _role = role;
        return this;
    }
    
    public HostSpecBuilder WithAvailability(HostAvailability availability)
    {
        _availability = availability;
        return this;
    }
    
    /// <summary>
    /// Builds a new HostSpec instance with the configured parameters.
    /// </summary>
    /// <returns>A new HostSpec instance.</returns>
    /// <exception cref="ArgumentException">Thrown when required parameters are missing or invalid.</exception>
    public HostSpec Build()
    {
        if (string.IsNullOrEmpty(_host))
        {
            throw new ArgumentException("Host cannot be null or empty", nameof(_host));
        }
        
        if (string.IsNullOrEmpty(_hostId))
        {
            throw new ArgumentException("HostId cannot be null or empty", nameof(_hostId));
        }
        
        return new HostSpec(
            _host,
            _port,
            _hostId,
            _role,
            _availability);
    }
}