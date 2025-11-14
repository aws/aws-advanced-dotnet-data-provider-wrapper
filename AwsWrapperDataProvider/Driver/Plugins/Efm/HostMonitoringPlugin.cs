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
using System.Runtime.Versioning;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitoringPlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<HostMonitoringPlugin> Logger = LoggerUtils.GetLogger<HostMonitoringPlugin>();

    public static readonly int DefaultFailureDetectionTime = 30000;
    public static readonly int DefaultFailureDetectionInterval = 5000;
    public static readonly int DefaultFailureDetectionCount = 3;

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private HostSpec? monitoringHostSpec;
    private IHostMonitorService? monitorService;

    protected readonly bool isEnabled;

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>()
    {
        // Network-bound methods that might fail and trigger failover
        "DbConnection.Open",
        "DbConnection.OpenAsync",
        "DbConnection.BeginDbTransaction",
        "DbConnection.BeginDbTransactionAsync",

        "DbCommand.ExecuteNonQuery",
        "DbCommand.ExecuteNonQueryAsync",
        "DbCommand.ExecuteReader",
        "DbCommand.ExecuteReaderAsync",
        "DbCommand.ExecuteScalar",
        "DbCommand.ExecuteScalarAsync",

        "DbDataReader.Read",
        "DbDataReader.ReadAsync",
        "DbDataReader.NextResult",
        "DbDataReader.NextResultAsync",

        "DbTransaction.Commit",
        "DbTransaction.CommitAsync",
        "DbTransaction.Rollback",
        "DbTransaction.RollbackAsync",

        // Special methods
        "DbConnection.ClearWarnings",
        "initHostProvider",
    };

    public HostMonitoringPlugin(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.isEnabled = PropertyDefinition.FailureDetectionEnabled.GetBoolean(props);
    }

    public override async Task<T> Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        if (!this.isEnabled || !this.SubscribedMethods.Contains(methodName))
        {
            return await methodFunc();
        }

        int failureDetectionTimeMillis = PropertyDefinition.FailureDetectionTime.GetInt(this.props) ?? DefaultFailureDetectionTime;
        int failureDetectionIntervalMillis = PropertyDefinition.FailureDetectionInterval.GetInt(this.props) ?? DefaultFailureDetectionInterval;
        int failureDetectionCount = PropertyDefinition.FailureDetectionCount.GetInt(this.props) ?? DefaultFailureDetectionCount;

        this.InitMonitorService();

        T result = default!;
        HostMonitorConnectionContext? monitorContext = null;

        try
        {
            Logger.LogTrace(Resources.EfmHostMonitor_ActivatedMonitoring);

            HostSpec monitoringHostSpec = await this.GetMonitoringHostSpec();

            monitorContext = this.monitorService!.StartMonitoring(
                this.pluginService.CurrentConnection!,
                monitoringHostSpec,
                this.props,
                failureDetectionTimeMillis,
                failureDetectionIntervalMillis,
                failureDetectionCount);

            result = await methodFunc();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, Resources.Error_ExceptionDuringMethodExecution, methodName);
            throw;
        }
        finally
        {
            if (monitorContext != null && this.monitorService != null && this.pluginService.CurrentConnection != null)
            {
                Logger.LogTrace(Resources.EfmHostMonitoringPlugin_Execute_DeactivatingMonitoring);
                this.monitorService.StopMonitoring(monitorContext, this.pluginService.CurrentConnection);
            }

            Logger.LogTrace(Resources.EfmHostMonitor_DeactivatedMonitoring);
        }

        return result;
    }

    public override async Task<DbConnection> OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        return await this.ConnectInternal(hostSpec, methodFunc);
    }

    public override async Task<DbConnection> ForceOpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        return await this.ConnectInternal(hostSpec, methodFunc);
    }

    private async Task<DbConnection> ConnectInternal(HostSpec? hostSpec, ADONetDelegate<DbConnection> methodFunc)
    {
        DbConnection conn = await methodFunc();

        if (hostSpec != null)
        {
            RdsUrlType type = RdsUtils.IdentifyRdsType(hostSpec.Host);

            if (type.IsRdsCluster)
            {
                hostSpec.ResetAliases();
                await this.pluginService.FillAliasesAsync(conn, hostSpec);
            }
        }

        return conn;
    }

    private void InitMonitorService()
    {
        this.monitorService ??= new HostMonitorService(this.pluginService, this.props);
    }

    public async Task<HostSpec> GetMonitoringHostSpec()
    {
        if (this.monitoringHostSpec == null)
        {
            this.monitoringHostSpec = this.pluginService.CurrentHostSpec!;
            RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(this.monitoringHostSpec.Host);

            try
            {
                if (rdsUrlType.IsRdsCluster)
                {
                    Logger.LogTrace(Resources.EfmHostMonitoringPlugin_GetMonitoringHostSpec_ClusterEndpointIdentification);
                    this.monitoringHostSpec = await this.pluginService.IdentifyConnectionAsync(this.pluginService.CurrentConnection!, this.pluginService.CurrentTransaction);
                    if (this.monitoringHostSpec == null)
                    {
                        throw new Exception(Resources.Error_UnableToIdentifyConnectionAndGatherMonitoringHostSpec);
                    }

                    await this.pluginService.FillAliasesAsync(this.pluginService.CurrentConnection!, this.monitoringHostSpec, this.pluginService.CurrentTransaction);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(string.Format(Resources.EfmHostMonitor_ErrorIdentifyingConnection, ex.Message));
                throw new Exception(Resources.Error_CouldntIdentifyConnection, ex);
            }
        }

        return this.monitoringHostSpec;
    }
}
