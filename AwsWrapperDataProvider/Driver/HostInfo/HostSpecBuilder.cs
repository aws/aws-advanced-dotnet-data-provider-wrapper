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
    private string? host;
    private int port = HostSpec.NoPort;
    private string? hostId;
    private HostRole role = HostRole.Writer;
    private HostAvailability availability = HostAvailability.Available;
    private long weight = HostSpec.DefaultWeight;
    private DateTime lastUpateTime;

    public HostSpecBuilder WithHost(string host)
    {
        this.host = host;
        return this;
    }

    public HostSpecBuilder WithPort(int port)
    {
        this.port = port;
        return this;
    }

    public HostSpecBuilder WithHostId(string hostId)
    {
        this.hostId = hostId;
        return this;
    }

    public HostSpecBuilder WithRole(HostRole role)
    {
        this.role = role;
        return this;
    }

    public HostSpecBuilder WithAvailability(HostAvailability availability)
    {
        this.availability = availability;
        return this;
    }

    public HostSpecBuilder WithWeight(long weight)
    {
        this.weight = weight;
        return this;
    }

    public HostSpecBuilder WithLastUpdateTime(DateTime lastUpdateTime)
    {
        this.lastUpateTime = lastUpdateTime;
        return this;
    }

    /// <summary>
    /// Builds a new HostSpec instance with the configured parameters.
    /// </summary>
    /// <returns>A new HostSpec instance.</returns>
    /// <exception cref="ArgumentException">Thrown when required parameters are missing or invalid.</exception>
    public HostSpec Build()
    {
        if (string.IsNullOrEmpty(this.host))
        {
            throw new ArgumentException("Host cannot be null or empty", nameof(this.host));
        }

        return new HostSpec(
            this.host,
            this.port,
            this.hostId,
            this.role,
            this.availability,
            this.weight,
            this.lastUpateTime);
    }
}
