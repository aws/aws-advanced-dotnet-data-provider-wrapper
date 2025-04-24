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
/// An object representing connection info for a given host. Modifiable fields are thread-safe to support sharing this
/// object with monitoring threads.
/// </summary>
public class HostSpec(
    string host,
    int port,
    string? hostId,
    HostRole hostRole,
    HostAvailability availability)
{
    public const int NoPort = -1;

    private readonly HostAvailability _availability = availability;

    public string? HostId { get; } = hostId;
    public string Host { get; } = host;
    public int Port { get; } = port;
    public bool IsPortSpecified => this.Port != NoPort;
    public HostRole Role { get; } = hostRole;
    public HostAvailability RawAvailability => this._availability;

    public override bool Equals(object? obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (obj is not HostSpec other)
        {
            return false;
        }

        return this.Host == other.Host &&
               this.Port == other.Port &&
               this.RawAvailability == other.RawAvailability &&
               this.Role == other.Role;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(this.Host, this.Port, this.RawAvailability, this.Role);
    }
}
