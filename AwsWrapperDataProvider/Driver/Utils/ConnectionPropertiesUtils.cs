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

namespace AwsWrapperDataProvider.Driver.Utils;

public static class ConnectionPropertiesUtils
{
    private static readonly string HostSeperator = ",";
    private static readonly string HostPortSeperator = ":";

    public static Dictionary<string, string> ParseConnectionStringParameters(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        return connectionString
            .Split(";", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Split("=", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
            .Select(x => new { Key = x.Length > 0 ? x[0] : null, Value = x.Length > 1 ? x[1] : null })
            .Where(x => x.Key != null && x.Value != null)
            .ToDictionary(k => k.Key ?? string.Empty, v => v.Value ?? string.Empty);
    }

    public static IList<HostSpec> GetHostsFromProperties(Dictionary<string, string> props, HostSpecBuilder hostSpecBuilder)
    {
        IList<HostSpec> hosts = new List<HostSpec>();
        string hostsString = PropertyDefinition.Host.GetString(props)
                      ?? PropertyDefinition.Server.GetString(props)
                      ?? string.Empty;
        IList<string> hostStringList =
            hostsString.Split(HostSeperator, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        int port = PropertyDefinition.Port.GetInt(props) ?? HostSpec.NoPort;

        foreach (string hostPortString in hostStringList)
        {
            IList<string> hostPortPair = hostPortString.Split(HostPortSeperator,
                2,
                StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            string hostString = hostPortPair[0];
            port = hostPortPair.Count == 2 ? int.Parse(hostPortPair[1]) : port;

            RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(hostString);
            HostRole hostRole = RdsUrlType.RdsReaderCluster.Equals(rdsUrlType) ? HostRole.Reader : HostRole.Writer;

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
}
