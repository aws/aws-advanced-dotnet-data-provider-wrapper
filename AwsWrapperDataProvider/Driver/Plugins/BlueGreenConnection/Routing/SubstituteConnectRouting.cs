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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection.Routing;

public class SubstituteConnectRouting : BaseConnectRouting
{
    private static readonly ILogger<SubstituteConnectRouting> Logger = LoggerUtils.GetLogger<SubstituteConnectRouting>();

    private readonly HostSpec substituteHostSpec;
    private readonly List<HostSpec>? iamHosts;
    private readonly IamSuccessfulConnectFunc? iamSuccessfulConnectNotify;

    public SubstituteConnectRouting(
        string? hostAndPort,
        BlueGreenRoleType? role,
        HostSpec substituteHostSpec,
        List<HostSpec>? iamHosts,
        IamSuccessfulConnectFunc? iamSuccessfulConnectNotify)
        : base(hostAndPort, role)
    {
        this.substituteHostSpec = substituteHostSpec;
        this.iamHosts = iamHosts;
        this.iamSuccessfulConnectNotify = iamSuccessfulConnectNotify;
    }

    public override async Task<DbConnection?> Apply(
        IConnectionPlugin plugin,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool useForceConnect,
        ADONetDelegate<DbConnection> connectFunc,
        IPluginService pluginService)
    {
        Logger.LogTrace(Resources.BlueGreenConnection_SubstituteConnectRouting_Apply, this.substituteHostSpec);

        if (!RdsUtils.IsIp(this.substituteHostSpec.Host))
        {
            return useForceConnect
                ? await pluginService.ForceOpenConnection(this.substituteHostSpec, props, plugin, false)
                : await pluginService.OpenConnection(this.substituteHostSpec, props, plugin, false);
        }

        bool iamInUse = pluginService.IsPluginInUse(PluginCodes.Iam);

        if (!iamInUse)
        {
            return useForceConnect
                ? await pluginService.ForceOpenConnection(this.substituteHostSpec, props, plugin, false)
                : await pluginService.OpenConnection(this.substituteHostSpec, props, plugin, false);
        }

        if (this.iamHosts == null || this.iamHosts.Count == 0)
        {
            throw new InvalidOperationException(Resources.SubstituteConnectRouting_Apply_RequireIamHost);
        }

        foreach (var iamHost in this.iamHosts)
        {
            var reroutedHostSpec = pluginService.HostSpecBuilder
                .CopyFrom(this.substituteHostSpec)
                .WithHostId(iamHost.HostId)
                .WithAvailability(HostAvailability.Available)
                .Build();
            reroutedHostSpec.AddAlias(iamHost.Host);

            var rerouteProperties = new Dictionary<string, string>(props) { [PropertyDefinition.IamHost.Name] = iamHost.Host };
            if (iamHost.IsPortSpecified)
            {
                rerouteProperties[PropertyDefinition.IamDefaultPort.Name] = iamHost.Port.ToString();
            }

            try
            {
                var conn = useForceConnect
                    ? await pluginService.ForceOpenConnection(reroutedHostSpec, rerouteProperties, null, false)
                    : await pluginService.OpenConnection(reroutedHostSpec, rerouteProperties, null, false);

                if (this.iamSuccessfulConnectNotify == null)
                {
                    return conn;
                }

                try
                {
                    this.iamSuccessfulConnectNotify(iamHost.Host);
                }
                catch
                {
                    Logger.LogWarning(Resources.SubstituteConnectRouting_Apply_IamSuccessfulConnectNotifyFailed, iamHost.Host);
                }

                return conn;
            }
            catch (Exception ex)
            {
                if (!pluginService.IsLoginException(ex))
                {
                    throw;
                }
            }
        }

        throw new InvalidOperationException(
            string.Format(Resources.SubstituteConnectRouting_Apply_InProgressCantOpenConnection, this.substituteHostSpec.GetHostAndPort()));
    }

    public override string ToString()
    {
        return $"{this.GetType().Name}@{this.GetHashCode():X} " +
               $"[{this.hostAndPort ?? "<null>"}, " +
               $"{this.role?.ToString() ?? "<null>"}, " +
               $"substitute: {this.substituteHostSpec?.GetHostAndPort() ?? "<null>"}, " +
               $"iamHosts: {(this.iamHosts == null ? "<null>" : string.Join(", ", this.iamHosts.Select(h => h.GetHostAndPort())))}]";
    }

    public delegate void IamSuccessfulConnectFunc(string iamHost);
}
