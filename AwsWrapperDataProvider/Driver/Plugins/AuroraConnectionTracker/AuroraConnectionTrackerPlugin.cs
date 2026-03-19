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
using AwsWrapperDataProvider.Driver.Plugins.Failover.Exceptions;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraConnectionTracker;

/// <summary>
/// Tracks all opened connections keyed by RDS instance endpoint. When a cluster failover
/// occurs and the writer node changes, this plugin closes all tracked connections to the
/// old writer, preventing applications from using stale connections that now point to a reader.
/// </summary>
public class AuroraConnectionTrackerPlugin : AbstractConnectionPlugin
{
    private const string MethodClose = "DbConnection.Close";
    private const string MethodCloseAsync = "DbConnection.CloseAsync";
    private const string MethodDispose = "DbConnection.Dispose";
    private static readonly TimeSpan TopologyChangesExpectedTime = TimeSpan.FromMinutes(3);

    private static readonly ILogger<AuroraConnectionTrackerPlugin> Logger =
        LoggerUtils.GetLogger<AuroraConnectionTrackerPlugin>();

    // Static shared state for refresh deadline across all plugin instances.
    // 0 means no refresh needed. Uses Interlocked for thread-safe updates.
    private static long s_hostListRefreshEndTimeTicks = 0;

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly IConnectionTracker tracker;
    private HostSpec? currentWriter;

    // Forward-compatibility placeholder: in reference implementation, this is set to true by the
    // NotifyNodeListChanged pipeline. This wrapper does not yet have this pipeline.
    private bool needUpdateCurrentWriter;

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>
    {
        // Connection close/dispose methods (for tracking removal)
        MethodClose,
        MethodCloseAsync,
        MethodDispose,

        // Network-bound DbCommand methods
        "DbCommand.ExecuteNonQuery",
        "DbCommand.ExecuteNonQueryAsync",
        "DbCommand.ExecuteScalar",
        "DbCommand.ExecuteScalarAsync",
        "DbCommand.ExecuteReader",
        "DbCommand.ExecuteReaderAsync",

        // Network-bound DbTransaction methods
        "DbTransaction.Commit",
        "DbTransaction.CommitAsync",
        "DbTransaction.Rollback",
        "DbTransaction.RollbackAsync",

        // Network-bound DbDataReader methods
        "DbDataReader.Read",
        "DbDataReader.ReadAsync",
        "DbDataReader.NextResult",
        "DbDataReader.NextResultAsync",

        // Network-bound DbBatch methods
        "DbBatch.ExecuteNonQuery",
        "DbBatch.ExecuteNonQueryAsync",
        "DbBatch.ExecuteReader",
        "DbBatch.ExecuteReaderAsync",
        "DbBatch.ExecuteScalar",
        "DbBatch.ExecuteScalarAsync",

        // Connection open/transaction methods
        "DbConnection.Open",
        "DbConnection.OpenAsync",
        "DbConnection.BeginDbTransaction",
        "DbConnection.BeginDbTransactionAsync",
    };

    public AuroraConnectionTrackerPlugin(IPluginService pluginService, Dictionary<string, string> props)
        : this(pluginService, props, new OpenedConnectionTracker(pluginService))
    {
    }

    internal AuroraConnectionTrackerPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props,
        IConnectionTracker tracker)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.tracker = tracker;
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        var conn = await methodFunc();

        if (conn == null || hostSpec == null)
        {
            return conn;
        }

        var rdsUrlType = RdsUtils.IdentifyRdsType(hostSpec.Host);
        if (rdsUrlType.IsRdsCluster || rdsUrlType == RdsUrlType.Other || rdsUrlType == RdsUrlType.IpAddress)
        {
            hostSpec.ResetAliases();
            await this.pluginService.FillAliasesAsync(conn, hostSpec);
        }

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, conn);

        return conn;
    }

    public override async Task<T> Execute<T>(
        object methodInvokedOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        var currentHostSpec = this.pluginService.CurrentHostSpec;
        this.RememberWriter();

        if (methodName is MethodClose or MethodCloseAsync or MethodDispose)
        {
            var result = await methodFunc();
            if (currentHostSpec != null)
            {
                this.tracker.RemoveConnectionTracking(currentHostSpec, this.pluginService.CurrentConnection);
            }

            return result;
        }

        long localRefreshEndTicks = Interlocked.Read(ref s_hostListRefreshEndTimeTicks);
        bool needRefreshHostList = false;
        if (localRefreshEndTicks > 0)
        {
            if (localRefreshEndTicks > DateTime.UtcNow.Ticks)
            {
                // The time specified in s_hostListRefreshEndTimeTicks isn't yet reached
                // Need to continue to refresh host list
                needRefreshHostList = true;
            }
            else
            {
                // The time specified in s_hostListRefreshEndTimeTicks is reached, and we can stop further refreshes
                // of host list
                Interlocked.CompareExchange(ref s_hostListRefreshEndTimeTicks, 0, localRefreshEndTicks);
            }
        }

        if (this.needUpdateCurrentWriter || needRefreshHostList)
        {
            await this.CheckWriterChangedAsync(needRefreshHostList);
        }

        try
        {
            return await methodFunc();
        }
        catch (FailoverException ex)
        {
            // Set the 3-minute refresh window.
            Interlocked.Exchange(
                ref s_hostListRefreshEndTimeTicks,
                DateTime.UtcNow.Ticks + TopologyChangesExpectedTime.Ticks);

            await this.CheckWriterChangedAsync(true);
            throw;
        }
    }

    private void RememberWriter()
    {
        if (this.currentWriter == null || this.needUpdateCurrentWriter)
        {
            this.currentWriter = WrapperUtils.GetWriter(this.pluginService.AllHosts);
            this.needUpdateCurrentWriter = false;
        }
    }

    private async Task CheckWriterChangedAsync(bool needRefreshHostList)
    {
        if (needRefreshHostList)
        {
            try
            {
                await this.pluginService.RefreshHostListAsync();
            }
            catch (Exception)
            {
                // Do nothing
            }
        }

        var writerAfterRefresh = WrapperUtils.GetWriter(this.pluginService.AllHosts);
        if (writerAfterRefresh == null)
        {
            return;
        }

        if (this.currentWriter == null)
        {
            this.currentWriter = writerAfterRefresh;
            this.needUpdateCurrentWriter = false;
        }
        else if (this.currentWriter.GetHostAndPort() != writerAfterRefresh.GetHostAndPort())
        {
            // The writer's changed, invalidate all connections to the old writer
            this.tracker.InvalidateAllConnections(this.currentWriter);
            this.tracker.LogOpenedConnections();
            this.currentWriter = writerAfterRefresh;
            this.needUpdateCurrentWriter = false;
            Interlocked.Exchange(ref s_hostListRefreshEndTimeTicks, 0);
        }
    }
}
