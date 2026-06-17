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

namespace AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;

/// <summary>
/// Global Database (GDB) Read/Write Splitting Plugin.
/// Extends <see cref="ReadWriteSplittingPlugin"/> to add the notion of a home region and to optionally
/// restrict writer/reader connections to that region.
/// </summary>
public class GdbReadWriteSplittingPlugin : ReadWriteSplittingPlugin
{
    private static readonly ILogger<GdbReadWriteSplittingPlugin> Logger = LoggerUtils.GetLogger<GdbReadWriteSplittingPlugin>();

    protected readonly bool restrictWriterToHomeRegion;
    protected readonly bool restrictReaderToHomeRegion;
    protected readonly bool enableGwf;

    protected bool isInit;
    protected string? homeRegion;

    public GdbReadWriteSplittingPlugin(IPluginService pluginService, Dictionary<string, string> props)
        : base(pluginService, props)
    {
        this.restrictWriterToHomeRegion = PropertyDefinition.GdbRwRestrictWriterToHomeRegion.GetBoolean(props);
        this.restrictReaderToHomeRegion = PropertyDefinition.GdbRwRestrictReaderToHomeRegion.GetBoolean(props);
        this.enableGwf = PropertyDefinition.GdbEnableGlobalWriteForwarding.GetBoolean(props);
    }

    public override Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        this.InitSettings(hostSpec, props);
        return base.OpenConnection(hostSpec, props, isInitialConnection, methodFunc, async);
    }

    protected void InitSettings(HostSpec? initHostSpec, Dictionary<string, string> props)
    {
        if (this.isInit)
        {
            return;
        }

        this.isInit = true;

        this.homeRegion = PropertyDefinition.GdbRwHomeRegion.GetString(props);
        if (string.IsNullOrEmpty(this.homeRegion) && initHostSpec != null)
        {
            var rdsUrlType = RdsUtils.IdentifyRdsType(initHostSpec.Host);
            if (rdsUrlType.HasRegion)
            {
                this.homeRegion = RdsUtils.GetRdsRegion(initHostSpec.Host);
            }
        }

        if (string.IsNullOrEmpty(this.homeRegion))
        {
            throw new ReadWriteSplittingDbException(
                string.Format(Resources.GdbReadWriteSplittingPlugin_MissingHomeRegion, initHostSpec?.Host));
        }

        Logger.LogDebug(
            Resources.GdbReadWriteSplittingPlugin_ParameterValue,
            PropertyDefinition.GdbRwHomeRegion.Name,
            this.homeRegion);
    }

    protected override async Task InitializeWriterConnection(HostSpec writerHost)
    {
        if (this.restrictWriterToHomeRegion
            && !string.Equals(
                this.homeRegion,
                RdsUtils.GetRdsRegion(writerHost.Host),
                StringComparison.OrdinalIgnoreCase))
        {
            if (this.enableGwf)
            {
                Logger.LogDebug(
                    Resources.GdbReadWriteSplittingPlugin_EnabledGwf,
                    RdsUtils.GetRdsRegion(writerHost.Host));
                return;
            }

            throw new ReadWriteSplittingDbException(
                string.Format(
                    Resources.GdbReadWriteSplittingPlugin_CantConnectWriterOutOfHomeRegion,
                    writerHost.Host,
                    this.homeRegion));
        }

        await base.InitializeWriterConnection(writerHost);
    }

    protected override IList<HostSpec> GetReaderHostCandidates()
    {
        if (this.restrictReaderToHomeRegion)
        {
            var hostsInRegion = this.pluginService.GetHosts()
                .Where(x => string.Equals(
                    RdsUtils.GetRdsRegion(x.Host),
                    this.homeRegion,
                    StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (hostsInRegion.Count == 0)
            {
                throw new ReadWriteSplittingDbException(
                    string.Format(
                        Resources.GdbReadWriteSplittingPlugin_NoAvailableReadersInHomeRegion,
                        this.homeRegion));
            }

            return hostsInRegion;
        }

        return base.GetReaderHostCandidates();
    }
}
