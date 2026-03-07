# Requirements Document

## Introduction

The Aurora Connection Tracker Plugin for the dotnet wrapper (`aws-advanced-dotnet-data-provider-wrapper`) tracks all opened database connections keyed by their RDS instance endpoint. When a cluster failover occurs and the writer node changes, the plugin closes all tracked connections that were pointing to the old writer. This prevents applications from accidentally using stale idle connections that now point to a reader, which would cause confusing errors like "cannot execute UPDATE in a read-only transaction". Instead, applications receive a clean "connection closed" error.

The plugin consists of two components:
- `AuroraConnectionTrackerPlugin` — an `IConnectionPlugin` that intercepts `OpenConnection`, `Execute`, and close methods to detect writer changes and trigger invalidation.
- `OpenedConnectionTracker` — a separate class encapsulating the thread-safe tracking data structure (a `ConcurrentDictionary` of instance endpoints to queues of weak references to `DbConnection`) and the invalidation/pruning logic.

Because the dotnet wrapper does not yet have the `NotifyNodeListChanged` pipeline, the plugin detects writer changes on the `Execute` path: it remembers the current writer, and after catching a `FailoverException`, it refreshes the host list and checks whether the writer has changed.

## Glossary

- **Plugin**: A class implementing `IConnectionPlugin` (via `AbstractConnectionPlugin`) that intercepts connection lifecycle and execution methods in the wrapper pipeline.
- **Plugin_Service**: The `IPluginService` interface (implemented by the `PluginService` class) providing access to the current connection, host list, host refresh, alias resolution, and other wrapper services.
- **Connection_Tracker_Plugin**: The `AuroraConnectionTrackerPlugin` class that intercepts `OpenConnection` and `Execute` calls to track connections and detect writer changes.
- **Opened_Connection_Tracker**: The `OpenedConnectionTracker` class that maintains the thread-safe data structure mapping RDS instance endpoints to queues of weak references to `DbConnection` objects, and provides methods to populate, invalidate, remove, and prune tracked connections.
- **Host_Spec**: The `HostSpec` class representing a database host with its hostname, port, role (Writer/Reader), availability, and aliases.
- **Writer**: The `HostSpec` in the current host list whose `Role` is `HostRole.Writer`.
- **Failover_Exception**: The `FailoverException` base class (and its subclasses `FailoverSuccessException`, `FailoverFailedException`, `TransactionStateUnknownException`) thrown when the failover plugin detects and handles a cluster failover event.
- **RDS_Instance_Endpoint**: A hostname matching the RDS instance DNS pattern (identified by `RdsUtils.IsRdsInstance()`), e.g., `mydb.123456789012.us-east-1.rds.amazonaws.com`.
- **Weak_Reference**: A `WeakReference<DbConnection>` that allows the garbage collector to reclaim the connection object while the tracker still holds a reference to it.
- **Tracking_Map**: The static `ConcurrentDictionary<string, ConcurrentQueue<WeakReference<DbConnection>>>` mapping RDS_Instance_Endpoint strings to queues of Weak_References.
- **Plugin_Chain_Builder**: The `ConnectionPluginChainBuilder` class that maps plugin codes to factories and assigns execution weights.
- **Plugin_Code**: The string `"auroraConnectionTracker"` registered in `PluginCodes.AuroraConnectionTracker`.

## Requirements

### Requirement 1: Plugin Registration and Wiring

**User Story:** As a developer using the dotnet wrapper, I want the Aurora Connection Tracker Plugin to be available and loadable by plugin code, so that I can enable connection tracking by adding `"auroraConnectionTracker"` to my plugin configuration.

#### Acceptance Criteria

1. THE Plugin_Chain_Builder SHALL map the Plugin_Code `"auroraConnectionTracker"` to an `AuroraConnectionTrackerPluginFactory` instance.
2. WHEN the Plugin_Chain_Builder instantiates the Connection_Tracker_Plugin, THE `AuroraConnectionTrackerPluginFactory` SHALL create an `AuroraConnectionTrackerPlugin` with an `OpenedConnectionTracker` whose backing Tracking_Map is static (shared across all plugin instances), so that all wrapper connections contribute to and read from the same global view of tracked connections.
3. THE Connection_Tracker_Plugin SHALL have an execution weight of 400 in the Plugin_Chain_Builder ordering.

### Requirement 2: Method Subscription

**User Story:** As a developer, I want the plugin to intercept the correct set of methods, so that it can track connections on open, detect writer changes on execute, and clean up on close.

#### Acceptance Criteria

1. THE Connection_Tracker_Plugin SHALL subscribe to the `"DbConnection.Close"` method.
2. THE Connection_Tracker_Plugin SHALL subscribe to the `"DbConnection.CloseAsync"` method.
3. THE Connection_Tracker_Plugin SHALL subscribe to the `"DbConnection.Dispose"` method.
4. THE Connection_Tracker_Plugin SHALL subscribe to all `"DbCommand.*"` execution methods (`"DbCommand.ExecuteNonQuery"`, `"DbCommand.ExecuteNonQueryAsync"`, `"DbCommand.ExecuteScalar"`, `"DbCommand.ExecuteScalarAsync"`, `"DbCommand.ExecuteReader"`, `"DbCommand.ExecuteReaderAsync"`).
5. THE Connection_Tracker_Plugin SHALL subscribe to all `"DbTransaction.*"` methods (`"DbTransaction.Commit"`, `"DbTransaction.CommitAsync"`, `"DbTransaction.Rollback"`, `"DbTransaction.RollbackAsync"`, etc.).

### Requirement 3: Track Connections on Open

**User Story:** As a developer, I want every new connection to be tracked by its RDS instance endpoint, so that the plugin knows which connections to invalidate when a failover occurs.

#### Acceptance Criteria

1. WHEN `OpenConnection` returns a non-null `DbConnection`, THE Connection_Tracker_Plugin SHALL call `FillAliasesAsync` on the Plugin_Service if the host's `RdsUrlType` is `RdsCluster`, `Other`, or `IpAddress`, after resetting the Host_Spec's aliases.
2. WHEN `OpenConnection` returns a non-null `DbConnection`, THE Connection_Tracker_Plugin SHALL pass the Host_Spec and connection to the Opened_Connection_Tracker's `PopulateOpenedConnectionQueue` method.
3. WHEN the Host_Spec's hostname is an RDS_Instance_Endpoint, THE Opened_Connection_Tracker SHALL track the connection under the Host_Spec's `host:port` key.
4. WHEN the Host_Spec's hostname is not an RDS_Instance_Endpoint, THE Opened_Connection_Tracker SHALL find the first alias that is an RDS_Instance_Endpoint and track the connection under that alias key.
5. IF no RDS_Instance_Endpoint is found in the Host_Spec's hostname or aliases, THEN THE Opened_Connection_Tracker SHALL log a debug message and skip tracking for that connection.
6. THE Opened_Connection_Tracker SHALL store each tracked connection as a `WeakReference<DbConnection>` in a `ConcurrentQueue` within the Tracking_Map.

### Requirement 4: Detect Writer Changes on Execute

**User Story:** As a developer, I want the plugin to detect when the writer node has changed after a failover, so that stale connections to the old writer are invalidated promptly.

#### Acceptance Criteria

1. WHEN `Execute` is called, THE Connection_Tracker_Plugin SHALL remember the current Writer by calling `WrapperUtils.GetWriter()` with the Plugin_Service's `AllHosts` list, if the current Writer is null or flagged for update.
2. WHEN `Execute` catches a Failover_Exception, THE Connection_Tracker_Plugin SHALL call `RefreshHostListAsync` on the Plugin_Service and then check whether the Writer has changed.
3. WHEN the Writer has changed (the Writer's `GetHostAndPort()` before failover differs from the Writer's `GetHostAndPort()` after failover), THE Connection_Tracker_Plugin SHALL call `InvalidateAllConnections` on the Opened_Connection_Tracker with the old Writer's Host_Spec.
4. WHEN the Writer has changed, THE Connection_Tracker_Plugin SHALL update its remembered Writer to the new Writer.
5. WHEN `Execute` catches a Failover_Exception, THE Connection_Tracker_Plugin SHALL re-throw the exception after performing the writer-change check.
6. WHEN the method name is `"DbConnection.Close"`, `"DbConnection.CloseAsync"`, or `"DbConnection.Dispose"`, THE Connection_Tracker_Plugin SHALL skip the writer-change check logic and proceed directly to the delegate call.

### Requirement 5: Remove Tracking on Close

**User Story:** As a developer, I want connections to be untracked when they are explicitly closed or disposed, so that the tracker does not hold references to connections that are no longer in use.

#### Acceptance Criteria

1. WHEN `Execute` completes successfully for method `"DbConnection.Close"`, `"DbConnection.CloseAsync"`, or `"DbConnection.Dispose"`, THE Connection_Tracker_Plugin SHALL call `RemoveConnectionTracking` on the Opened_Connection_Tracker with the current Host_Spec and current connection.
2. THE Opened_Connection_Tracker's `RemoveConnectionTracking` method SHALL remove the matching `WeakReference<DbConnection>` from the Tracking_Map queue for the host's RDS_Instance_Endpoint key.

### Requirement 6: Invalidate Stale Connections

**User Story:** As a developer, I want all tracked connections to the old writer to be closed when a writer change is detected, so that my application does not accidentally use stale connections pointing to a reader.

#### Acceptance Criteria

1. WHEN `InvalidateAllConnections` is called with a Host_Spec, THE Opened_Connection_Tracker SHALL look up the Tracking_Map using the Host_Spec's `AsAlias()` and all entries from `GetAliases()`.
2. WHEN a matching queue is found in the Tracking_Map, THE Opened_Connection_Tracker SHALL dequeue each Weak_Reference, resolve the `DbConnection`, and call `Close()` on each non-null, non-garbage-collected connection.
3. IF calling `Close()` on a connection throws an exception, THEN THE Opened_Connection_Tracker SHALL swallow the exception and continue processing the remaining connections in the queue.
4. THE Opened_Connection_Tracker SHALL only attempt to invalidate connections keyed under RDS_Instance_Endpoint aliases (filtering aliases through `RdsUtils.IsRdsInstance()`).

### Requirement 7: Background Pruning of Stale References

**User Story:** As a developer, I want garbage-collected or closed connection references to be periodically cleaned up from the tracking map, so that memory is not wasted on stale weak references.

#### Acceptance Criteria

1. THE Opened_Connection_Tracker SHALL start a background task that runs `PruneNullConnections` every 30 seconds.
2. THE `PruneNullConnections` method SHALL iterate over all entries in the Tracking_Map and remove any Weak_Reference whose target `DbConnection` has been garbage-collected (i.e., `TryGetTarget` returns false).
3. THE background pruning task SHALL be initialized once (singleton pattern) regardless of how many Opened_Connection_Tracker instances are created.
4. THE Opened_Connection_Tracker SHALL expose a `ClearCache` method that clears all entries from the static Tracking_Map.

### Requirement 8: Thread Safety

**User Story:** As a developer, I want the connection tracking to be thread-safe, so that concurrent connection opens, closes, and failovers do not cause data corruption or race conditions.

#### Acceptance Criteria

1. THE Opened_Connection_Tracker SHALL use a `ConcurrentDictionary<string, ConcurrentQueue<WeakReference<DbConnection>>>` for the Tracking_Map to support concurrent reads and writes from multiple threads.
2. THE Opened_Connection_Tracker SHALL use `GetOrAdd` on the `ConcurrentDictionary` when adding a new queue for a previously unseen endpoint key, to avoid race conditions.

### Requirement 9: Topology Refresh After Failover

**User Story:** As a developer, I want the plugin to continue refreshing the host list for a period after a failover, so that topology changes that happen gradually are detected.

#### Acceptance Criteria

1. WHEN `Execute` catches a Failover_Exception, THE Connection_Tracker_Plugin SHALL record a host-list-refresh deadline of 3 minutes from the current time.
2. WHILE the host-list-refresh deadline has not been reached, THE Connection_Tracker_Plugin SHALL call `RefreshHostListAsync` on the Plugin_Service before each non-close `Execute` call and check for writer changes.
3. WHEN the host-list-refresh deadline is reached, THE Connection_Tracker_Plugin SHALL stop the periodic host list refresh.
4. WHEN a writer change is confirmed, THE Connection_Tracker_Plugin SHALL reset the host-list-refresh deadline to zero (stop further refreshes).

### Requirement 10: Logging and Diagnostics

**User Story:** As a developer, I want the plugin to log connection tracking activity, so that I can diagnose connection management issues.

#### Acceptance Criteria

1. WHEN connections are tracked or invalidated, THE Opened_Connection_Tracker SHALL log the tracked connections at Debug level using the wrapper's `ILogger` infrastructure.
2. WHEN invalidating connections for a host, THE Opened_Connection_Tracker SHALL log the host and the number of connections being invalidated before closing them.
