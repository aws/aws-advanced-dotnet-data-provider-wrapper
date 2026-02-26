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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Routing;

public class SubstituteConnectRouting : BaseConnectRouting
{
    private static readonly ILogger<SubstituteConnectRouting> Logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<SubstituteConnectRouting>();

    protected readonly HostSpec SubstituteHostSpec;
    protected readonly List<HostSpec>? IamHosts;
    protected readonly IamSuccessfulConnectFunc? IamSuccessfulConnectNotify;

    public SubstituteConnectRouting(string? hostAndPort, BlueGreenRoleType? role, HostSpec substituteHostSpec,
        List<HostSpec>? iamHosts, IamSuccessfulConnectFunc? iamSuccessfulConnectNotify)
        : base(hostAndPort, role)
    {
        this.SubstituteHostSpec = substituteHostSpec;
        this.IamHosts = iamHosts;
        this.IamSuccessfulConnectNotify = iamSuccessfulConnectNotify;
    }

    public override async Task<DbConnection> Apply(
        IConnectionPlugin plugin,
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool useForceConnect,
        ADONetDelegate<DbConnection> connectFunc,
        IPluginService pluginService)
    {
        if (!RdsUtils.IsIp(this.SubstituteHostSpec.Host))
        {
            return useForceConnect
                ? await pluginService.ForceOpenConnection(this.SubstituteHostSpec, props, plugin, false)
                : await pluginService.OpenConnection(this.SubstituteHostSpec, props, plugin, false);
        }

        bool iamInUse = pluginService.IsPluginInUse("IamAuthPlugin");

        if (!iamInUse)
        {
            return useForceConnect
                ? await pluginService.ForceOpenConnection(this.SubstituteHostSpec, props, plugin, false)
                : await pluginService.OpenConnection(this.SubstituteHostSpec, props, plugin, false);
        }

        if (this.IamHosts == null || this.IamHosts.Count == 0)
        {
            throw new InvalidOperationException(Resources.SubstituteConnectRouting_Apply_RequireIamHost);
        }

        foreach (var iamHost in this.IamHosts)
        {
            var reroutedHostSpec = pluginService.HostSpecBuilder
                .CopyFrom(this.SubstituteHostSpec)
                .WithHostId(iamHost.HostId)
                .WithAvailability(HostAvailability.Available)
                .Build();
            reroutedHostSpec.AddAlias(iamHost.Host);

            var rerouteProperties = new Dictionary<string, string>(props);
            rerouteProperties[PropertyDefinition.IamHost.Name] = iamHost.Host;
            if (iamHost.IsPortSpecified)
            {
                rerouteProperties[PropertyDefinition.IamDefaultPort.Name] = iamHost.Port.ToString();
            }

            try
            {
                var conn = useForceConnect
                    ? await pluginService.ForceOpenConnection(reroutedHostSpec, rerouteProperties, null, false)
                    : await pluginService.OpenConnection(reroutedHostSpec, rerouteProperties, null, false);

                if (this.IamSuccessfulConnectNotify != null)
                {
                    try
                    {
                        this.IamSuccessfulConnectNotify(iamHost.Host);
                    }
                    catch
                    {
                        // do nothing
                    }
                }

                return conn;
            }
            catch (Exception ex)
            {
                if (!pluginService.IsLoginException(ex))
                {
                    throw;
                }

                // try with another IAM host
            }
        }

        throw new InvalidOperationException(
            string.Format(Resources.SubstituteConnectRouting_Apply_InProgressCantOpenConnection, this.SubstituteHostSpec.GetHostAndPort()));
    }

    public override string ToString()
    {
        return $"{this.GetType().Name}@{this.GetHashCode():X} [{this.HostAndPort ?? "<null>"}, {this.Role?.ToString() ?? "<null>"}, substitute: {SubstituteHostSpec?.GetHostAndPort() ?? "<null>"}, iamHosts: {(this.IamHosts == null ? "<null>" : string.Join(", ", this.IamHosts.Select(h => h.GetHostAndPort())))}]";
    }

    public delegate void IamSuccessfulConnectFunc(string iamHost);
}
