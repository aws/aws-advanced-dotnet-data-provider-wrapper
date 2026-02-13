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
using Amazon;
using Amazon.RDS;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.CustomEndpoint.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;

/// <summary>
/// A plugin that analyzes custom endpoints for custom endpoint information and custom endpoint changes, such as adding
/// or removing an instance in the custom endpoint.
/// </summary>
public class CustomEndpointPlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<CustomEndpointPlugin> Logger = LoggerUtils.GetLogger<CustomEndpointPlugin>();

    protected static readonly MemoryCache Monitors = new(new MemoryCacheOptions { SizeLimit = 100 });

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>
    {
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

        "DbBatch.ExecuteNonQuery",
        "DbBatch.ExecuteNonQueryAsync",
        "DbBatch.ExecuteReader",
        "DbBatch.ExecuteReaderAsync",
        "DbBatch.ExecuteScalar",
        "DbBatch.ExecuteScalarAsync",

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
    };

    /// <summary>
    /// Closes all active custom endpoint monitors.
    /// </summary>
    public static void CloseMonitors()
    {
        Logger.LogInformation(Resources.CustomEndpointPlugin_CloseMonitors);

        // The clear call automatically calls Dispose() on all monitors via eviction callbacks.
        Monitors.Clear();
    }

    protected readonly IPluginService pluginService;
    protected readonly Dictionary<string, string> props;
    protected readonly Func<RegionEndpoint, AmazonRDSClient> rdsClientFunc;

    protected readonly bool shouldWaitForInfo;
    protected readonly int waitOnCachedInfoDurationMs;
    protected readonly int idleMonitorExpirationMs;
    protected HostSpec? customEndpointHostSpec;
    protected string? customEndpointId;
    protected RegionEndpoint? region;

    public CustomEndpointPlugin(IPluginService pluginService, Dictionary<string, string> props) : this(
        pluginService,
        props,
        region => new AmazonRDSClient(region))
    {
    }

    public CustomEndpointPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props,
        Func<RegionEndpoint, AmazonRDSClient> rdsClientFunc)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.rdsClientFunc = rdsClientFunc;

        this.shouldWaitForInfo = PropertyDefinition.WaitForCustomEndpointInfo.GetBoolean(this.props);
        this.waitOnCachedInfoDurationMs = PropertyDefinition.WaitForCustomEndpointInfoTimeoutMs.GetInt(this.props) ?? 5000;
        this.idleMonitorExpirationMs = PropertyDefinition.CustomEndpointMonitorIdleExpirationMs.GetInt(this.props) ?? 900000;
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        if (hostSpec == null || !RdsUtils.IsRdsCustomClusterDns(hostSpec.Host))
        {
            return await methodFunc();
        }

        this.customEndpointHostSpec = hostSpec;

        Logger.LogTrace(Resources.CustomEndpointPlugin_ConnectionRequestToCustomEndpoint, hostSpec.Host);

        this.customEndpointId = RdsUtils.GetRdsClusterId(this.customEndpointHostSpec.Host);
        if (string.IsNullOrEmpty(this.customEndpointId))
        {
            throw new InvalidOperationException(
                string.Format(Resources.CustomEndpointPlugin_ErrorParsingEndpointIdentifier, this.customEndpointHostSpec.Host));
        }

        string? regionString = RegionUtils.GetRegion(this.customEndpointHostSpec.Host, props, PropertyDefinition.CustomEndpointRegion);
        if (string.IsNullOrEmpty(regionString) || !RegionUtils.IsValidRegion(regionString))
        {
            throw new InvalidOperationException(
                string.Format(Resources.CustomEndpointPlugin_UnableToDetermineRegion, PropertyDefinition.CustomEndpointRegion.Name));
        }

        this.region = RegionEndpoint.GetBySystemName(regionString);

        ICustomEndpointMonitor monitor = this.CreateMonitorIfAbsent(props);

        if (this.shouldWaitForInfo)
        {
            // If needed, wait a short time for custom endpoint info to be discovered.
            await this.WaitForCustomEndpointInfoAsync(monitor);
        }

        return await methodFunc();
    }

    /// <summary>
    /// Creates a monitor for the custom endpoint if it does not already exist.
    /// </summary>
    /// <param name="props">The connection properties.</param>
    /// <returns>ICustomEndpointMonitor.</returns>
    protected virtual ICustomEndpointMonitor CreateMonitorIfAbsent(Dictionary<string, string> props)
    {
        if (this.customEndpointHostSpec == null || this.region == null || string.IsNullOrEmpty(this.customEndpointId))
        {
            throw new InvalidOperationException("Custom endpoint information is not initialized.");
        }

        string cacheKey = this.customEndpointHostSpec.Host;
        ICustomEndpointMonitor? existingMonitor = Monitors.Get<Lazy<ICustomEndpointMonitor>>(cacheKey)?.Value;
        if (existingMonitor != null)
        {
            return existingMonitor;
        }

        TimeSpan refreshRate = TimeSpan.FromMilliseconds(
            PropertyDefinition.CustomEndpointInfoRefreshRateMs.GetInt(props) ?? 30000);
        int refreshRateBackoffFactor = PropertyDefinition.CustomEndpointInfoRefreshRateBackoffFactor.GetInt(props) ?? 2;
        TimeSpan maxRefreshRate = TimeSpan.FromMilliseconds(
            PropertyDefinition.CustomEndpointInfoMaxRefreshRateMs.GetInt(props) ?? 300000);

        var newMonitor = Monitors.GetOrCreate(cacheKey, entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(this.idleMonitorExpirationMs);
            entry.Size = 1;
            entry.RegisterPostEvictionCallback(OnMonitorEvicted);

            return new Lazy<CustomEndpointMonitor>(() =>
                new CustomEndpointMonitor(
                    this.pluginService,
                    this.customEndpointHostSpec,
                    this.customEndpointId,
                    this.region,
                    refreshRate,
                    refreshRateBackoffFactor,
                    maxRefreshRate,
                    this.rdsClientFunc));
        });

        return newMonitor!.Value;
    }

    internal static void OnMonitorEvicted(object key, object? value, EvictionReason reason, object? state)
    {
        ICustomEndpointMonitor? toDispose = null;
        if (value is Lazy<CustomEndpointMonitor> lazyCustom && lazyCustom.IsValueCreated)
        {
            toDispose = lazyCustom.Value;
        }
        else if (value is Lazy<ICustomEndpointMonitor> lazyIf && lazyIf.IsValueCreated)
        {
            toDispose = lazyIf.Value;
        }
        else if (value is ICustomEndpointMonitor monitor)
        {
            toDispose = monitor;
        }

        if (toDispose != null)
        {
            try
            {
                Logger.LogTrace(Resources.CustomEndpointPlugin_OnMonitorEvicted_Disposing, key, reason);
                toDispose.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(Resources.Error_DisposingCustomEndpointMonitor, ex.Message);
            }
        }
    }

    /// <summary>
    /// If custom endpoint info does not exist for the current custom endpoint, waits a short time for the info to be
    /// made available by the custom endpoint monitor. This is necessary so that other plugins can rely on accurate custom
    /// endpoint info. Since custom endpoint monitors and information are shared, we should not have to wait often.
    /// </summary>
    /// <param name="monitor">A ICustomEndpointMonitor monitor.</param>
    /// <exception cref="InvalidOperationException">If there's an error getting custom endpoint, or if it takes longer time than anticipated.</exception>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    protected async Task WaitForCustomEndpointInfoAsync(ICustomEndpointMonitor monitor)
    {
        bool hasCustomEndpointInfo = monitor.HasCustomEndpointInfo();

        if (!hasCustomEndpointInfo)
        {
            monitor.RequestCustomEndpointInfoUpdate();

            Logger.LogTrace(Resources.CustomEndpointPlugin_WaitingForCustomEndpointInfo,
                this.customEndpointHostSpec?.Host,
                this.waitOnCachedInfoDurationMs);

            DateTime waitForEndpointInfoTimeout = DateTime.UtcNow.AddMilliseconds(this.waitOnCachedInfoDurationMs);

            try
            {
                while (!hasCustomEndpointInfo && DateTime.UtcNow < waitForEndpointInfoTimeout)
                {
                    await Task.Delay(100);
                    hasCustomEndpointInfo = monitor.HasCustomEndpointInfo();
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    string.Format(Resources.CustomEndpointPlugin_InterruptedThread, this.customEndpointHostSpec?.Host), ex);
            }

            if (!hasCustomEndpointInfo)
            {
                throw new InvalidOperationException(
                    string.Format(Resources.CustomEndpointPlugin_TimedOutWaitingForCustomEndpointInfo,
                        this.waitOnCachedInfoDurationMs,
                        this.customEndpointHostSpec?.Host));
            }
        }
    }

    /// <summary>
    /// Executes the given method via a pipeline of plugins. If a custom endpoint is being used, a monitor for that custom
    /// endpoint will be created if it does not already exist.
    /// </summary>
    /// <param name="methodInvokedOn">The object that the methodFunc is being invoked on.</param>
    /// <param name="methodName">The name of the method being invoked.</param>
    /// <param name="methodFunc">The execute pipeline to call to invoke the method.</param>
    /// <param name="methodArgs">The arguments to the method being invoked.</param>
    /// <typeparam name="T">The type of the result returned by the method.</typeparam>
    /// <returns>The result of the method invocation.</returns>
    public override async Task<T> Execute<T>(
        object methodInvokedOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        if (this.customEndpointHostSpec == null)
        {
            return await methodFunc();
        }

        try
        {
            ICustomEndpointMonitor monitor = this.CreateMonitorIfAbsent(this.props);
            if (this.shouldWaitForInfo)
            {
                // If needed, wait a short time for custom endpoint info to be discovered.
                await this.WaitForCustomEndpointInfoAsync(monitor);
            }
        }
        catch (Exception e)
        {
            Logger.LogWarning(e, Resources.CustomEndpointPlugin_ErrorCreatingMonitor);

            // Continue execution even if monitor creation fails
        }

        return await methodFunc();
    }
}
