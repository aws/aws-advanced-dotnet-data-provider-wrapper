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

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;

public class StatusInfo
{
    public string Version { get; }

    public string Endpoint { get; }

    public int Port { get; }

    public BlueGreenRoleType Role { get; }

    public BlueGreenPhaseType Phase { get; }

    public StatusInfo(string version, string endpoint, int port, BlueGreenPhaseType phase, BlueGreenRoleType role)
    {
        this.Version = version;
        this.Endpoint = endpoint;
        this.Port = port;
        this.Phase = phase;
        this.Role = role;
    }

    public override string ToString()
    {
        return $"StatusInfo [version={this.Version}, endpoint={this.Endpoint}, port={this.Port}, phase={this.Phase}]";
    }
}
