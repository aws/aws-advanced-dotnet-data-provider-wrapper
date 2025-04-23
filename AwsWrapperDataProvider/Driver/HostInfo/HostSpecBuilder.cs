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

namespace AwsWrapperDataProvider.Driver.HostInfo;

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
        this._host = host;
        return this;
    }

    public HostSpecBuilder WithPort(int port)
    {
        this._port = port;
        return this;
    }

    public HostSpecBuilder WithHostId(string hostId)
    {
        this._hostId = hostId;
        return this;
    }

    public HostSpecBuilder WithRole(HostRole role)
    {
        this._role = role;
        return this;
    }

    public HostSpecBuilder WithAvailability(HostAvailability availability)
    {
        this._availability = availability;
        return this;
    }

    /// <summary>
    /// Builds a new HostSpec instance with the configured parameters.
    /// </summary>
    /// <returns>A new HostSpec instance.</returns>
    /// <exception cref="ArgumentException">Thrown when required parameters are missing or invalid.</exception>
    public HostSpec Build()
    {
        if (string.IsNullOrEmpty(this._host))
        {
            throw new ArgumentException("Host cannot be null or empty", nameof(this._host));
        }

        if (string.IsNullOrEmpty(this._hostId))
        {
            throw new ArgumentException("HostId cannot be null or empty", nameof(this._hostId));
        }

        return new HostSpec(
            this._host,
            this._port,
            this._hostId,
            this._role,
            this._availability);
    }
}
