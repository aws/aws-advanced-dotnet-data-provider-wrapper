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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Utils;

public static class ConnectionPropertiesUtils
{
    private const string HostSeperator = ",";
    private const string HostPortSeperator = ":";

    private static readonly ILogger<AwsWrapperProperty> Logger = LoggerUtils.GetLogger<AwsWrapperProperty>();

    public static Dictionary<string, string> ParseConnectionStringParameters(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        var props = connectionString
            .Split(";", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Split("=", StringSplitOptions.TrimEntries))
            .Where(pairs => pairs.Length == 2 && !string.IsNullOrEmpty(pairs[0]))
            .GroupBy(pairs => pairs[0], StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.Last()[1], StringComparer.OrdinalIgnoreCase);

        return props;
    }

    public static IList<HostSpec> GetHostsFromProperties(Dictionary<string, string> props, HostSpecBuilder hostSpecBuilder, bool singleWriterConnectionString)
    {
        List<HostSpec> hosts = [];
        string hostsString = PropertyDefinition.Host.GetString(props)
                      ?? PropertyDefinition.Server.GetString(props)
                      ?? string.Empty;
        IList<string> hostStringList = hostsString.Split(HostSeperator, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        int port = PropertyDefinition.Port.GetInt(props) ?? HostSpec.NoPort;

        for (int i = 0; i < hostStringList.Count; i++)
        {
            IList<string> hostPortPair = hostStringList[i].Split(
                HostPortSeperator,
                2,
                StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            string hostString = hostPortPair[0];
            port = hostPortPair.Count == 2 ? int.Parse(hostPortPair[1]) : port;
            HostRole hostRole;
            if (singleWriterConnectionString)
            {
                hostRole = i > 0 ? HostRole.Reader : HostRole.Writer;
            }
            else
            {
                RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(hostString);
                hostRole = RdsUrlType.RdsReaderCluster.Equals(rdsUrlType) ? HostRole.Reader : HostRole.Writer;
            }

            string? hostId = RdsUtils.GetRdsInstanceId(hostString);

            HostSpec host = hostSpecBuilder
                .WithHost(hostString)
                .WithHostId(hostId)
                .WithPort(port)
                .WithRole(hostRole)
                .Build();

            hosts.Add(host);
        }

        return hosts;
    }

    public static HostSpec ParseHostPortPair(string url, HostSpecBuilder hostSpecBuilder)
    {
        IList<string> hostPortPair = url.Split(
                HostPortSeperator,
                2,
                StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(hostPortPair[0]);
        HostRole hostRole = RdsUrlType.RdsReaderCluster.Equals(rdsUrlType) ? HostRole.Reader : HostRole.Writer;
        string? hostId = RdsUtils.GetRdsInstanceId(hostPortPair[0]);
        if (hostPortPair.Count > 1)
        {
            if (int.TryParse(hostPortPair[1], out int port))
            {
                return hostSpecBuilder
                    .WithHost(hostPortPair[0])
                    .WithPort(port)
                    .WithHostId(hostId)
                    .WithRole(hostRole)
                    .Build();
            }
        }

        return hostSpecBuilder
            .WithHost(hostPortPair[0])
            .WithPort(HostSpec.NoPort)
            .WithHostId(hostId)
            .WithRole(hostRole)
            .Build();
    }
}
