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

using System.Collections.Concurrent;

namespace AwsWrapperDataProvider.Driver.HostInfo;

/// <summary>
/// An object representing connection info for a given host. Modifiable fields are thread-safe to support sharing this
/// object with monitoring threads.
/// </summary>
public class HostSpec
{
    private readonly ConcurrentDictionary<string, byte> aliases = new();
    private readonly ConcurrentDictionary<string, byte> allAliases = new();

    public const int NoPort = -1;
    public const long DefaultWeight = 100;

    public string? HostId { get; }
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

        this.allAliases.TryAdd(this.AsAlias(), 0);
    }

    public string GetHostAndPort() => this.IsPortSpecified ? $"{this.Host}:{this.Port}" : this.Host;

    public string AsAlias()
    {
        return this.GetHostAndPort();
    }

    public ICollection<string> AsAliases()
    {
        return this.allAliases.Keys;
    }

    public ICollection<string> GetAliases()
    {
        return this.aliases.Keys;
    }

    public void AddAlias(params string[] aliases)
    {
        if (aliases == null || aliases.Length == 0)
        {
            return;
        }

        foreach (string alias in aliases)
        {
            if (!string.IsNullOrEmpty(alias))
            {
                this.aliases.TryAdd(alias, 0);
                this.allAliases.TryAdd(alias, 0);
            }
        }
    }

    public void RemoveAlias(params string[] aliases)
    {
        if (aliases == null || aliases.Length == 0)
        {
            return;
        }

        foreach (string alias in aliases)
        {
            if (!string.IsNullOrEmpty(alias))
            {
                this.aliases.TryRemove(alias, out _);
                this.allAliases.TryRemove(alias, out _);
            }
        }
    }

    public void ResetAliases()
    {
        this.aliases.Clear();
        this.allAliases.Clear();
        this.allAliases.TryAdd(this.AsAlias(), 0);
    }

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
            "HostSpec@{0} [host={1}, port={2}, {3}, {4}, weight={5}, {6}]",
            this.GetHashCode().ToString("X"),
            this.Host,
            this.Port,
            this.Role,
            this.Availability,
            this.Weight,
            this.LastUpdateTime);
    }
}
