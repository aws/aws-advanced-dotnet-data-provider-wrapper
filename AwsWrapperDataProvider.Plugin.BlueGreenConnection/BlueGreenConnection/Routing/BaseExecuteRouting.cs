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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.Routing;

public abstract class BaseExecuteRouting : BaseRouting, IExecuteRouting
{
    protected readonly string? HostAndPort;
    protected BlueGreenRoleType? role;

    protected BaseExecuteRouting(string? hostAndPort, BlueGreenRoleType? role)
    {
        this.HostAndPort = hostAndPort?.ToLowerInvariant();
        this.role = role;
    }

    public bool IsMatch(HostSpec hostSpec, BlueGreenRoleType hostRole)
    {
        return (this.HostAndPort == null || this.HostAndPort.Equals(hostSpec?.GetHostAndPort()?.ToLowerInvariant()))
               && (this.role == null || this.role.Equals(hostRole));
    }

    public abstract Task<T?> Apply<T>(
        IConnectionPlugin plugin,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> jdbcMethodFunc,
        object[] jdbcMethodArgs,
        IPluginService pluginService,
        Dictionary<string, string> props);

    public override string ToString()
    {
        return $"{base.ToString()} [{this.HostAndPort ?? "<null>"}, {this.role.ToString()}]";
    }
}
