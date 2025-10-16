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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraStaleDns;

/// <summary>
/// Helper class for detecting and handling stale DNS issues with Aurora cluster endpoints.
/// Aurora cluster endpoints can sometimes resolve to outdated IP addresses, especially during
/// failover scenarios. This helper detects such cases and ensures connections go to the correct instances.
/// </summary>
public class AuroraStaleDnsHelper
{
    private const int DnsRetries = 3;

    private static readonly ILogger<AuroraStaleDnsHelper> Logger = LoggerUtils.GetLogger<AuroraStaleDnsHelper>();

    private readonly IPluginService pluginService;
    private HostSpec? writerHostSpec;
    private string? writerHostAddress;

    public AuroraStaleDnsHelper(IPluginService pluginService)
    {
        this.pluginService = pluginService;
    }

    public async Task<DbConnection> OpenVerifiedConnection(
        bool isInitialConnection,
        IHostListProviderService hostListProviderService,
        HostSpec hostSpec,
        Dictionary<string, string> props,
        ADONetDelegate<DbConnection> openFunc)
    {
        // If this is not a writer cluster DNS, no verification needed
        if (!RdsUtils.IsWriterClusterDns(hostSpec.Host))
        {
            return await openFunc();
        }

        DbConnection connection = await openFunc();

        // Get the IP address that the cluster endpoint resolved to
        string? clusterInetAddress = GetHostIpAddress(hostSpec.Host);
        Logger.LogTrace(
            "Cluster endpoint {host} resolved to IP address {ipAddress}",
            hostSpec.Host,
            clusterInetAddress ?? "null");
        if (clusterInetAddress == null)
        {
            return connection;
        }

        // Check the role of the connection we actually got
        HostRole connectionRole = this.pluginService.GetHostRole(connection);
        Logger.LogTrace("Current connection role: {role}", connectionRole);

        this.pluginService.ForceRefreshHostList(connection);
        Logger.LogTrace(LoggerUtils.LogTopology(this.pluginService.AllHosts, null));

        this.writerHostSpec = this.GetWriter();
        Logger.LogTrace("Writer host spec: {hostSpec}", this.writerHostSpec);

        if (this.writerHostSpec == null)
        {
            // No writer found in topology, return original connection
            return connection;
        }

        this.writerHostAddress = GetHostIpAddress(this.writerHostSpec.Host);
        Logger.LogTrace("Writer host address: {address}", this.writerHostAddress);

        if (this.writerHostAddress == null)
        {
            // Can't resolve writer IP, return original connection
            return connection;
        }

        // Compare the cluster endpoint IP with the actual writer IP
        if (this.writerHostAddress != clusterInetAddress)
        {
            // Stale DNS detected! The cluster endpoint resolves to a different IP than the actual writer
            Logger.LogTrace("Stale DNS data detected. Opening a connection to {host}", this.writerHostSpec);

            // Verify the writer is in the allowed hosts list
            var allowedHosts = this.pluginService.GetHosts();
            if (!ContainsHostUrl(allowedHosts, this.writerHostSpec.Host))
            {
                throw new InvalidOperationException(
                    $"Current writer {this.writerHostSpec.Host} is not in the allowed hosts list. " +
                    $"Allowed hosts: {string.Join(", ", allowedHosts.Select(h => h.Host))}");
            }

            // Create a new connection to the correct writer instance
            DbConnection writerConnection = await this.pluginService.OpenConnection(this.writerHostSpec, props, null, true);

            // Update the initial connection host spec if this is the initial connection
            if (isInitialConnection)
            {
                hostListProviderService.InitialConnectionHostSpec = this.writerHostSpec;
            }

            connection.Dispose();
            return writerConnection;
        }

        // No stale DNS detected, return the original connection
        return connection;
    }

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
                // Brief delay before retry
                Thread.Sleep(100);
            }
        }

        return null;
    }

    private static bool ContainsHostUrl(IList<HostSpec> hosts, string hostUrl)
    {
        return hosts.Any(host => host.Host.Equals(hostUrl, StringComparison.OrdinalIgnoreCase));
    }

    private HostSpec? GetWriter()
    {
        return this.pluginService.AllHosts.FirstOrDefault(host => host.Role == HostRole.Writer);
    }

    private IList<HostSpec>? GetReaders()
    {
        return [.. this.pluginService.AllHosts.Where(host => host.Role != HostRole.Writer)];
    }
}
