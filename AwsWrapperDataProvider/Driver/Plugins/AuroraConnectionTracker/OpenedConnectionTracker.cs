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
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraConnectionTracker;

/// <summary>
/// Tracks opened database connections keyed by RDS instance endpoint.
/// The backing tracking map is static and shared across all plugin instances,
/// so that all wrapper connections contribute to and read from the same global
/// view of tracked connections.
/// </summary>
public class OpenedConnectionTracker
{
    private static readonly ILogger<OpenedConnectionTracker> Logger =
        LoggerUtils.GetLogger<OpenedConnectionTracker>();

    /// <summary>
    /// Static tracking map shared across all instances. Keys are RDS instance
    /// endpoints in host:port format. Values are thread-safe queues of weak
    /// references to DbConnection objects opened to that instance.
    /// </summary>
    internal static readonly ConcurrentDictionary<string, ConcurrentQueue<WeakReference<DbConnection>>> OpenedConnections = new();

    /// <summary>
    /// Singleton background pruning task. Initialized once on first access via Lazy.
    /// Runs every 30 seconds to remove garbage-collected connection references.
    /// </summary>
    private static readonly Lazy<Task> PruneTask = new(() => Task.Run(PruneLoop));

    private readonly IPluginService pluginService;

    public OpenedConnectionTracker(IPluginService pluginService)
    {
        this.pluginService = pluginService;

        // Touch the lazy to ensure the background pruning task is started.
        _ = PruneTask.Value;
    }

    /// <summary>
    /// Registers a connection in the tracking map keyed by its RDS instance endpoint.
    /// </summary>
    public void PopulateOpenedConnectionQueue(HostSpec hostSpec, DbConnection connection)
    {
        // Check if the connection was established using an instance endpoint
        if (RdsUtils.IsRdsInstance(hostSpec.Host))
        {
            TrackConnection(hostSpec.GetHostAndPort(), connection);
            this.LogOpenedConnections();
            return;
        }

        // Find the instance endpoint from aliases
        string? instanceEndpoint = hostSpec.AsAliases()
            .Where(alias => RdsUtils.IsRdsInstance(RdsUtils.RemovePort(alias)))
            .MaxBy(alias => alias, StringComparer.OrdinalIgnoreCase);

        if (instanceEndpoint != null)
        {
            TrackConnection(instanceEndpoint, connection);
            this.LogOpenedConnections();
            return;
        }

        // No RDS instance endpoint found — skip tracking
        Logger.LogDebug(
            Resources.OpenedConnectionTracker_PopulateOpenedConnectionQueue_NoRdsInstanceEndpoint,
            hostSpec.Host);
    }

    private static void TrackConnection(string instanceEndpoint, DbConnection connection)
    {
        var queue = OpenedConnections.GetOrAdd(
            instanceEndpoint,
            _ => new ConcurrentQueue<WeakReference<DbConnection>>());
        queue.Enqueue(new WeakReference<DbConnection>(connection));
    }

    /// <summary>
    /// Clears all entries from the static tracking map.
    /// </summary>
    public static void ClearCache()
    {
        OpenedConnections.Clear();
    }

    private static async Task PruneLoop()
    {
        while (true)
        {
            try
            {
                PruneNullConnections();
                await Task.Delay(TimeSpan.FromSeconds(30));
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, Resources.OpenedConnectionTracker_PruneLoop_Error);
            }
        }
    }

    /// <summary>
    /// Removes garbage-collected connection references from all queues in the tracking map.
    /// </summary>
    public static void PruneNullConnections()
    {
        foreach (var kvp in OpenedConnections)
        {
            var queue = kvp.Value;
            var liveEntries = new ConcurrentQueue<WeakReference<DbConnection>>();

            while (queue.TryDequeue(out var weakRef))
            {
                if (weakRef.TryGetTarget(out _))
                {
                    liveEntries.Enqueue(weakRef);
                }
            }

            // Replace the old drained queue with only live references,
            // or remove the key entirely if no live references remain.
            if (!liveEntries.IsEmpty)
            {
                OpenedConnections[kvp.Key] = liveEntries;
            }
            else
            {
                OpenedConnections.TryRemove(kvp.Key, out _);
            }
        }
    }

    private void LogOpenedConnections()
    {
        if (!Logger.IsEnabled(LogLevel.Debug))
        {
            return;
        }

        foreach (var kvp in OpenedConnections)
        {
            if (!kvp.Value.IsEmpty)
            {
                int count = kvp.Value.Count;
                Logger.LogDebug(Resources.OpenedConnectionTracker_LogOpenedConnections_Tracking, count, kvp.Key);
            }
        }
    }
}
