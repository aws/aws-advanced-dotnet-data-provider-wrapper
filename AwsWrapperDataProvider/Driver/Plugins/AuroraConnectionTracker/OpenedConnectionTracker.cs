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
public class OpenedConnectionTracker : IConnectionTracker
{
    private static readonly ILogger<OpenedConnectionTracker> Logger =
        LoggerUtils.GetLogger<OpenedConnectionTracker>();

    /// <summary>
    /// Static tracking map shared across all instances. Keys are RDS instance
    /// endpoints in host:port format. Values are thread-safe queues of weak
    /// references to DbConnection objects opened to that instance.
    /// </summary>
    internal static readonly ConcurrentDictionary<string, RWQueue<WeakReference<DbConnection>>> OpenedConnections = new();

    private static readonly object PruneLock = new();
    private static CancellationTokenSource? pruneCts;
    private static Task? pruneTask;

    private readonly IPluginService pluginService;

    public OpenedConnectionTracker(IPluginService pluginService)
    {
        this.pluginService = pluginService;
        EnsurePruneLoopRunning();
    }

    private static void EnsurePruneLoopRunning()
    {
        lock (PruneLock)
        {
            if (pruneTask is null || pruneTask.IsCompleted)
            {
                pruneCts = new CancellationTokenSource();
                pruneTask = Task.Run(() => PruneLoop(pruneCts.Token));
            }
        }
    }

    /// <summary>
    /// Registers a connection in the tracking map keyed by its RDS instance endpoint.
    /// </summary>
    /// <param name="hostSpec">The host specification identifying the database instance.</param>
    /// <param name="connection">The database connection to track.</param>
    public void PopulateOpenedConnectionQueue(HostSpec hostSpec, DbConnection connection)
    {
        EnsurePruneLoopRunning();

        // Check if the connection was established using an instance endpoint
        if (RdsUtils.IsRdsInstance(hostSpec.Host))
        {
            TrackConnection(hostSpec.AsAlias(), connection);
            this.LogOpenedConnections();
            return;
        }

        var aliases = hostSpec.AsAliases();

        // Find the instance endpoint from aliases
        string? instanceEndpoint = aliases
            .Where(alias => RdsUtils.IsRdsInstance(RdsUtils.RemovePort(alias)))
            .MaxBy(alias => alias, StringComparer.OrdinalIgnoreCase);

        if (instanceEndpoint != null)
        {
            TrackConnection(instanceEndpoint, connection);
            this.LogOpenedConnections();
            return;
        }

        // No RDS instance host found. It might be a custom domain name. Track by all aliases.
        Logger.LogDebug(
            Resources.OpenedConnectionTracker_PopulateOpenedConnectionQueue_TrackingByAllAliases,
            hostSpec.Host);

        foreach (string alias in aliases)
        {
            TrackConnection(alias, connection);
        }

        this.LogOpenedConnections();
    }

    private static void TrackConnection(string instanceEndpoint, DbConnection connection)
    {
        var queue = OpenedConnections.GetOrAdd(
            instanceEndpoint,
            _ => new RWQueue<WeakReference<DbConnection>>());
        queue.Enqueue(new WeakReference<DbConnection>(connection));
    }

    /// <summary>
    /// Removes a specific connection from the tracking map for the given host.
    /// </summary>
    /// <param name="hostSpec">The host specification identifying the database instance.</param>
    /// <param name="connection">The database connection to remove from tracking.</param>
    public void RemoveConnectionTracking(HostSpec hostSpec, DbConnection? connection)
    {
        string? host = RdsUtils.IsRdsInstance(hostSpec.Host)
            ? hostSpec.AsAlias()
            : hostSpec.GetAliases()
                .FirstOrDefault(alias => RdsUtils.IsRdsInstance(RdsUtils.RemovePort(alias)));

        if (string.IsNullOrEmpty(host))
        {
            // Connections tracked under custom domain aliases are not individually
            // removed here as iterating all alias keys is expensive. They will be
            // cleaned up by the background pruning loop once garbage-collected.
            return;
        }

        if (!OpenedConnections.TryGetValue(host, out var queue))
        {
            return;
        }

        queue.RemoveIf(weakRef => !weakRef.TryGetTarget(out var conn) || ReferenceEquals(conn, connection));
    }

    /// <summary>
    /// Invalidates all tracked connections for the given host.
    /// </summary>
    /// <param name="hostSpec">The host specification identifying the database instance.</param>
    public void InvalidateAllConnections(HostSpec hostSpec)
    {
        var keys = new List<string> { hostSpec.AsAlias() };
        keys.AddRange(hostSpec.GetAliases());
        this.InvalidateAllConnections(keys.ToArray());
    }

    private static void InvalidateConnections(RWQueue<WeakReference<DbConnection>> queue)
    {
        while (true)
        {
            var (weakRef, success) = queue.Dequeue();
            if (!success)
            {
                break;
            }

            if (!weakRef!.TryGetTarget(out var conn))
            {
                continue;
            }

            try
            {
                conn.Close();
            }
            catch (Exception)
            {
                // Swallow — current connection is stale anyway.
            }
        }
    }

    /// <summary>
    /// Invalidates all tracked connections for the given keys.
    /// </summary>
    private void InvalidateAllConnections(params string[] keys)
    {
        foreach (string key in keys)
        {
            try
            {
                if (!OpenedConnections.TryGetValue(key, out var queue))
                {
                    continue;
                }

                LogConnectionQueue(key, queue);
                InvalidateConnections(queue);
            }
            catch (Exception)
            {
                // ignore and continue
            }
        }
    }


    /// <summary>
    /// Cancels the background pruning task and clears all tracked connections.
    /// The pruning loop will restart automatically when a new tracker instance is created or when a connection is tracked.
    /// </summary>
    public static void ReleaseResources()
    {
        lock (PruneLock)
        {
            pruneCts?.Cancel();
            pruneCts?.Dispose();
            pruneCts = null;
            pruneTask = null;
        }

        OpenedConnections.Clear();
    }

    private static async Task PruneLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                PruneNullConnections();
                await Task.Delay(TimeSpan.FromSeconds(30), token);
            }
            catch (OperationCanceledException)
            {
                break;
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
            kvp.Value.RemoveIf(weakRef => !weakRef.TryGetTarget(out _));
        }
    }

    public void LogOpenedConnections()
    {
        foreach (var kvp in OpenedConnections)
        {
            if (!kvp.Value.IsEmpty)
            {
                int count = kvp.Value.Count;
                Logger.LogDebug(Resources.OpenedConnectionTracker_LogOpenedConnections_Tracking, count, kvp.Key);
            }
        }
    }

    private static void LogConnectionQueue(string host, RWQueue<WeakReference<DbConnection>> queue)
    {
        if (queue.IsEmpty)
        {
            return;
        }

        var builder = new System.Text.StringBuilder();
        builder.Append(host).Append("\n[");
        queue.ForEach(weakRef =>
        {
            weakRef.TryGetTarget(out var conn);
            builder.Append("\n\t").Append(conn);
        });

        builder.Append("\n]");
        Logger.LogDebug(Resources.OpenedConnectionTracker_LogConnectionQueue_InvalidatingConnections, builder.ToString());
    }
}
