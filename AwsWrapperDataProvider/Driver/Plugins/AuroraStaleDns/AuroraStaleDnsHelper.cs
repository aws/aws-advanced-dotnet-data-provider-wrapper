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

using System.Data.Common;
using System.Net;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraStaleDns;

/// <summary>
/// Helper class for detecting and handling stale DNS issues with Aurora cluster endpoints.
/// Aurora cluster endpoints can sometimes resolve to outdated IP addresses, especially during
/// failover scenarios. This helper detects such cases and ensures connections go to the correct instances.
/// </summary>
public class AuroraStaleDnsHelper
{
    private readonly IPluginService pluginService;
    private HostSpec? writerHostSpec = null;
    private string? writerHostAddress = null;

    private const int DnsRetries = 3;

    public AuroraStaleDnsHelper(IPluginService pluginService)
    {
        this.pluginService = pluginService;
    }

    public bool OpenVerifiedConnection(
        bool isInitialConnection,
        IHostListProviderService hostListProviderService,
        HostSpec hostSpec,
        Dictionary<string, string> props,
        ADONetDelegate openFunc)
    {
        // If this is not a writer cluster DNS, no verification needed
        if (!RdsUtils.IsWriterClusterDns(hostSpec.Host))
        {
            openFunc();
            return true;
        }

        openFunc();

        // Get the IP address that the cluster endpoint resolved to
        string? clusterInetAddress = GetHostIpAddress(hostSpec.Host);
        if (clusterInetAddress == null)
        {
            return true;
        }

        // Check the role of the connection we actually got
        HostRole connectionRole = this.pluginService.GetHostRole(this.pluginService.CurrentConnection);

        if (connectionRole == HostRole.Reader)
        {
            // If the connection URL is a writer cluster endpoint but we got a reader,
            // this indicates the topology is outdated. Force refresh to update it.
            this.pluginService.ForceRefreshHostList();
        }
        else
        {
            // Normal refresh for writer connections
            this.pluginService.RefreshHostList();
        }

        // Find the current writer in the topology
        if (this.writerHostSpec == null)
        {
            HostSpec? writerCandidate = this.GetWriter();
            if (writerCandidate != null && RdsUtils.IsRdsClusterDns(writerCandidate.Host))
            {
                return false;
            }

            this.writerHostSpec = writerCandidate;
        }

        if (this.writerHostSpec == null)
        {
            // No writer found in topology, return original connection
            return true;
        }

        // Get the IP address of the actual writer instance
        if (this.writerHostAddress == null)
        {
            this.writerHostAddress = GetHostIpAddress(this.writerHostSpec.Host);
        }

        if (this.writerHostAddress == null)
        {
            // Can't resolve writer IP, return original connection
            return true;
        }

        // Compare the cluster endpoint IP with the actual writer IP
        if (!this.writerHostAddress.Equals(clusterInetAddress))
        {
            // Stale DNS detected! The cluster endpoint resolves to a different IP than the actual writer

            // Verify the writer is in the allowed hosts list
            var allowedHosts = this.pluginService.GetHosts();
            if (!ContainsHostUrl(allowedHosts, this.writerHostSpec.Host))
            {
                throw new InvalidOperationException(
                    $"Current writer {this.writerHostSpec.Host} is not in the allowed hosts list. " +
                    $"Allowed hosts: {string.Join(", ", allowedHosts.Select(h => h.Host))}");
            }

            // Create a new connection to the correct writer instance
            this.pluginService.OpenConnection(this.writerHostSpec, props, isInitialConnection);

            // Update the initial connection host spec if this is the initial connection
            if (isInitialConnection)
            {
                hostListProviderService.InitialConnectionHostSpec = this.writerHostSpec;
            }
        }

        // No stale DNS detected, return the original connection
        return true;
    }

    /// <summary>
    /// Notifies the helper about changes in the node list, allowing it to reset cached information
    /// when the writer changes.
    /// </summary>
    /// <param name="changes">Dictionary of host URLs to their change options</param>
    public void NotifyNodeListChanged(Dictionary<string, NodeChangeOptions> changes)
    {
        if (this.writerHostSpec == null)
        {
            return;
        }

        foreach (var entry in changes)
        {
            if (entry.Key.Equals(this.writerHostSpec.Host) &&
                entry.Value.HasFlag(NodeChangeOptions.PromotedToReader))
            {
                // The current writer was demoted to reader, reset our cached information
                this.writerHostSpec = null;
                this.writerHostAddress = null;
                break;
            }
        }
    }

    /// <summary>
    /// Resets the cached writer information. This can be called when topology changes are detected.
    /// </summary>
    public void Reset()
    {
        this.writerHostSpec = null;
        this.writerHostAddress = null;
    }

    /// <summary>
    /// Gets the IP address for a given hostname with retry logic.
    /// </summary>
    /// <param name="hostname">The hostname to resolve</param>
    /// <returns>The IP address as a string, or null if resolution fails</returns>
    private static string? GetHostIpAddress(string hostname)
    {
        for (int attempt = 0; attempt < DnsRetries; attempt++)
        {
            try
            {
                IPAddress[] addresses = Dns.GetHostAddresses(hostname);
                if (addresses.Length > 0)
                {
                    // Return the first IPv4 address, or the first address if no IPv4 is found
                    IPAddress? ipv4Address = addresses.FirstOrDefault(addr => addr.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
                    return (ipv4Address ?? addresses[0]).ToString();
                }
            }
            catch (Exception)
            {
                // If this is not the last attempt, continue to retry
                if (attempt == DnsRetries - 1)
                {
                    return null;
                }

                // Brief delay before retry
                Thread.Sleep(100);
            }
        }

        return null;
    }

    /// <summary>
    /// Finds the writer host in the current topology.
    /// </summary>
    /// <returns>The writer host specification, or null if no writer is found</returns>
    private HostSpec? GetWriter()
    {
        return this.pluginService.AllHosts.FirstOrDefault(host => host.Role == HostRole.Writer);
    }

    /// <summary>
    /// Checks if the given host URL is contained in the list of host specifications.
    /// </summary>
    /// <param name="hosts">List of host specifications.</param>
    /// <param name="hostUrl">The host URL to search for.</param>
    /// <returns>True if the host URL is found in the list.</returns>
    private static bool ContainsHostUrl(IList<HostSpec> hosts, string hostUrl)
    {
        return hosts.Any(host => host.Host.Equals(hostUrl, StringComparison.OrdinalIgnoreCase));
    }
}
