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
    private static long hostListRefreshEndTimeTicks = 0;

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly OpenedConnectionTracker tracker;
    private HostSpec? currentWriter;

    // Forward-compatibility placeholder: in JDBC and Go, this is set to true by the
    // NotifyNodeListChanged pipeline when a PROMOTED_TO_WRITER event fires. Since the
    // dotnet wrapper does not yet have this pipeline, nothing currently sets this flag.
    // Writer change detection relies on the CheckWriterChangedAsync path instead.
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
        OpenedConnectionTracker tracker)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.tracker = tracker;
    }
}
