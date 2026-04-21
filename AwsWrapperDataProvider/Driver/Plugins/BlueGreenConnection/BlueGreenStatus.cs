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
using System.Collections.ObjectModel;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection.Routing;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;

public class BlueGreenStatus
{
    private string BgdId { get; }

    public BlueGreenPhaseType CurrentPhase { get; }

    public ReadOnlyCollection<IConnectRouting> ConnectRouting { get; }

    public ReadOnlyCollection<IExecuteRouting> ExecuteRouting { get; }

    public ConcurrentDictionary<string, BlueGreenRoleType> RoleByHost { get; } = new();

    public ConcurrentDictionary<string, (HostSpec Blue, HostSpec? Green)> CorrespondingNodes { get; }

    public BlueGreenStatus(string bgdId, BlueGreenPhaseType phase)
        : this(
            bgdId,
            phase,
            [
            ],
            [
            ],
            new Dictionary<string, BlueGreenRoleType>(),
            new Dictionary<string, (HostSpec Blue, HostSpec? Green)>())
    {
    }

    public BlueGreenStatus(
        string bgdId,
        BlueGreenPhaseType phase,
        List<IConnectRouting> connectRouting,
        List<IExecuteRouting> executeRouting,
        IDictionary<string, BlueGreenRoleType> roleByHost,
        IDictionary<string, (HostSpec Blue, HostSpec? Green)> correspondingNodes)
    {
        this.BgdId = bgdId;
        this.CurrentPhase = phase;
        this.ConnectRouting = new ReadOnlyCollection<IConnectRouting>(new List<IConnectRouting>(connectRouting));
        this.ExecuteRouting = new ReadOnlyCollection<IExecuteRouting>(new List<IExecuteRouting>(executeRouting));
        this.RoleByHost = new ConcurrentDictionary<string, BlueGreenRoleType>(roleByHost);
        this.CorrespondingNodes = new ConcurrentDictionary<string, (HostSpec Blue, HostSpec? Green)>(correspondingNodes);
    }

    public BlueGreenRoleType? GetRole(HostSpec? hostSpec)
    {
        if (hostSpec != null && this.RoleByHost.TryGetValue(hostSpec.Host.ToLowerInvariant(), out var role))
        {
            return role;
        }

        return null;
    }

    public override string ToString()
    {
        var roleByHostMap = string.Join("\n   ", this.RoleByHost.Select(x => $"{x.Key} -> {x.Value}"));
        var connectRoutingStr = string.Join("\n   ", this.ConnectRouting.Select(x => x.ToString()));
        var executeRoutingStr = string.Join("\n   ", this.ExecuteRouting.Select(x => x.ToString()));

        return $"{base.ToString()} [\n" +
               $" bgdId: '{this.BgdId}', \n" +
               $" phase: {this.CurrentPhase}, \n" +
               $" Connect routing: \n" +
               $"   {(string.IsNullOrEmpty(connectRoutingStr) ? "-" : connectRoutingStr)} \n" +
               $" Execute routing: \n" +
               $"   {(string.IsNullOrEmpty(executeRoutingStr) ? "-" : executeRoutingStr)} \n" +
               $" roleByHost: \n" +
               $"   {(string.IsNullOrEmpty(roleByHostMap) ? "-" : roleByHostMap)} \n" +
               "]";
    }
}
