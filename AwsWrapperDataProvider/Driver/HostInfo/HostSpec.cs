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
public class HostSpec
{
    public const int NoPort = -1;
    public const long DefaultWeight = 100;

    /// <summary>Gets the host id. Could be a node name, node domain name, or some gibberish code.</summary>
    public string? HostId { get; }

    /// <summary>Gets the full domain name.</summary>
    public string Host { get; }
    public int Port { get; }
    public bool IsPortSpecified => this.Port != NoPort;
    public HostRole Role { get; }
    public HostAvailability RawAvailability { get; set; }

    public HostAvailability Availability
    {
        get
        {
            // TODO: check for host selection strategy when implemented.
            return this.RawAvailability;
        }
        set
        {
            this.RawAvailability = value;
        }
    }

    public long Weight { get; }
    public DateTime LastUpdateTime { get; }

    internal HostSpec(
        string host,
        int port,
        string? hostId,
        HostRole hostRole,
        HostAvailability availability)
        : this(host, port, hostId, hostRole, availability, DefaultWeight, DateTime.UtcNow)
    {
    }

    public HostSpec(
        string host,
        int port,
        HostRole hostRole,
        HostAvailability availability)
        : this(host, port, null, hostRole, availability, DefaultWeight, DateTime.UtcNow)
    {
    }

    public HostSpec(
        string host,
        int port,
        string? hostId,
        HostRole hostRole,
        HostAvailability availability,
        long weight,
        DateTime lastUpdateTime)
    {
        this.Host = host;
        this.Port = port;
        this.HostId = hostId;
        this.Role = hostRole;
        this.RawAvailability = availability;
        this.Weight = weight;
        this.LastUpdateTime = lastUpdateTime;
    }

    public HostSpec(HostSpec copyHost, HostRole role)
        : this(
              copyHost.Host,
              copyHost.Port,
              copyHost.HostId,
              role,
              copyHost.RawAvailability,
              copyHost.Weight,
              copyHost.LastUpdateTime)
    {
    }

    public string GetHostAndPort() => this.IsPortSpecified ? $"{this.Host}:{this.Port}" : this.Host;

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
               this.Role == other.Role &&
               this.Weight == other.Weight;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(this.Host, this.Port, this.RawAvailability, this.Role, this.Weight, this.LastUpdateTime);
    }

    public override string ToString()
    {
        return string.Format(
            "HostSpec@{0} [host={1}, port={2}, {3}, {4}, weight={5}, {6}, host id={7}]",
            this.GetHashCode().ToString("X"),
            this.Host,
            this.Port,
            this.Role,
            this.Availability,
            this.Weight,
            this.LastUpdateTime,
            this.HostId);
    }

    public HostSpec Clone()
    {
        return new(this.Host, this.Port, this.HostId, this.Role, this.RawAvailability, this.Weight, this.LastUpdateTime);
    }
}
