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
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection;

public class BlueGreenInterimStatus
{
    public BlueGreenPhaseType? BlueGreenPhase { get; set; }

    public string Version { get; set; }

    public int Port { get; set; }

    public IList<HostSpec> StartTopology { get; set; }

    public IList<HostSpec> CurrentTopology { get; set; }

    public Dictionary<string, string?> StartIpAddressesByHostMap { get; set; }

    public Dictionary<string, string?> CurrentIpAddressesByHostMap { get; set; }

    public HashSet<string> HostNames { get; set; }

    public bool AllStartTopologyIpChanged { get; set; }

    public bool AllStartTopologyEndpointsRemoved { get; set; }

    public bool AllTopologyChanged { get; set; }

    public BlueGreenInterimStatus(
        BlueGreenPhaseType? blueGreenPhase,
        string version,
        int port,
        IList<HostSpec> startTopology,
        IList<HostSpec> currentTopology,
        Dictionary<string, string?> startIpAddressesByHostMap,
        Dictionary<string, string?> currentIpAddressesByHostMap,
        HashSet<string> hostNames,
        bool allStartTopologyIpChanged,
        bool allStartTopologyEndpointsRemoved,
        bool allTopologyChanged)
    {
        this.BlueGreenPhase = blueGreenPhase;
        this.Version = version;
        this.Port = port;
        this.StartTopology = startTopology;
        this.CurrentTopology = currentTopology;
        this.StartIpAddressesByHostMap = startIpAddressesByHostMap;
        this.CurrentIpAddressesByHostMap = currentIpAddressesByHostMap;
        this.HostNames = hostNames;
        this.AllStartTopologyIpChanged = allStartTopologyIpChanged;
        this.AllStartTopologyEndpointsRemoved = allStartTopologyEndpointsRemoved;
        this.AllTopologyChanged = allTopologyChanged;
    }

    public override string ToString()
    {
        var currentIpMap = string.Join("\n   ", this.CurrentIpAddressesByHostMap.Select(x => $"{x.Key} -> {x.Value}"));
        var startIpMap = string.Join("\n   ", this.StartIpAddressesByHostMap.Select(x => $"{x.Key} -> {x.Value}"));
        var allHostNamesStr = string.Join("\n   ", this.HostNames);
        var startTopologyStr = LoggerUtils.LogTopology(this.StartTopology, typeof(BlueGreenInterimStatus).ToString());
        var currentTopologyStr = LoggerUtils.LogTopology(this.CurrentTopology, typeof(BlueGreenInterimStatus).ToString());

        return $"{base.ToString()} [\n" +
               $" phase {this.BlueGreenPhase.ToString()}, \n" +
               $" version '{this.Version}', \n" +
               $" port {this.Port}, \n" +
               $" hostNames:\n" +
               $"   {(string.IsNullOrEmpty(allHostNamesStr) ? "-" : allHostNamesStr)} \n" +
               $" Start {(string.IsNullOrEmpty(startTopologyStr) ? "-" : startTopologyStr)} \n" +
               $" start IP map:\n" +
               $"   {(string.IsNullOrEmpty(startIpMap) ? "-" : startIpMap)} \n" +
               $" Current {(string.IsNullOrEmpty(currentTopologyStr) ? "-" : currentTopologyStr)} \n" +
               $" current IP map:\n" +
               $"   {(string.IsNullOrEmpty(currentIpMap) ? "-" : currentIpMap)} \n" +
               $" allStartTopologyIpChanged: {this.AllStartTopologyIpChanged} \n" +
               $" allStartTopologyEndpointsRemoved: {this.AllStartTopologyEndpointsRemoved} \n" +
               $" allTopologyChanged: {this.AllTopologyChanged} \n" +
               "]";
    }

    public int GetCustomHashCode()
    {
        int result = this.GetValueHash(1, this.BlueGreenPhase.ToString());
        result = this.GetValueHash(result, this.Version);
        result = this.GetValueHash(result, this.Port.ToString());
        result = this.GetValueHash(result, this.AllStartTopologyIpChanged.ToString());
        result = this.GetValueHash(result, this.AllStartTopologyEndpointsRemoved.ToString());
        result = this.GetValueHash(result, this.AllTopologyChanged.ToString());
        result = this.GetValueHash(result, string.Join(",", this.HostNames.OrderBy(x => x)));
        result = this.GetValueHash(result, string.Join(",", this.StartTopology.Select(x => x.Host + x.Role).OrderBy(x => x)));
        result = this.GetValueHash(result, string.Join(",", this.CurrentTopology.Select(x => x.Host + x.Role).OrderBy(x => x)));
        result = this.GetValueHash(result, string.Join(",", this.StartIpAddressesByHostMap.Select(x => x.Key + x.Value).OrderBy(x => x)));
        result = this.GetValueHash(result, string.Join(",", this.CurrentIpAddressesByHostMap.Select(x => x.Key + x.Value).OrderBy(x => x)));
        return result;
    }

    protected int GetValueHash(int currentHash, string val)
    {
        return currentHash * 31 + val.GetHashCode();
    }
}
