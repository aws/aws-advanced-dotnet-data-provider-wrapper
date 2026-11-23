# Connection Lifetime Plugin

The connection lifetime plugin limits how long the AWS Advanced .NET Data Provider Wrapper keeps using one physical database connection. When an expired connection is about to make a network-bound call, the plugin either closes the connection or reconnects to the host originally supplied in the connection string.

## Loading the Connection Lifetime Plugin

The connection lifetime plugin is not loaded by default. To load the plugin, include the plugin code `connectionLifetime` in the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) connection parameter and set the required `ConnectionLifetimeMs` parameter.

```dotnet
var connectionString = "Host=my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com;" +
    "Database=myapp;Username=admin;Password=pwd;" +
    "Plugins=connectionLifetime,initialConnection,failover,efm;" +
    "ConnectionLifetimeMs=300000";

using var connection = new AwsWrapperConnection(connectionString);
```

With the default `AutoSortPluginOrder` behavior, the plugin is placed before routing, failover, monitoring and authentication plugins, so it checks the lifetime before they run. Plugins without a fixed position, such as `executionTime` and `connectTime`, keep their place relative to the plugin listed before them.

## Connection Lifetime Parameters

| Parameter                  | Value   | Required | Description                                                                                                         | Default Value |
|----------------------------|---------|----------|---------------------------------------------------------------------------------------------------------------------|---------------|
| `ConnectionLifetimeMs`     | Integer | Yes      | Maximum physical connection lifetime in milliseconds. The value must be at least `1`.                               | None          |
| `ConnectionLifetimeAction` | String  | No       | Action taken after expiration. Accepted values are `reconnect` and `close`, ignoring letter case and surrounding spaces. | `reconnect`   |

A missing or invalid `ConnectionLifetimeMs`, or a `ConnectionLifetimeAction` other than `reconnect` or `close`, causes an `ArgumentException` when the connection is created.

## Expiration Checks

The lifetime starts when the wrapper opens a physical connection. A physical connection that another plugin installs without opening it through the plugin chain starts its lifetime the first time the plugin sees it. A connection is expired when its elapsed lifetime is greater than or equal to `ConnectionLifetimeMs`, so a connection with a lifetime of 1000 ms is expired at exactly 1000 ms.

The plugin checks the lifetime immediately before network-bound calls: `DbCommand` and `DbBatch` execution methods, `BeginTransaction`, and `DbTransaction.Commit` and `Rollback`, including their asynchronous forms. Data reader calls are deferred as described in [Transactions and Open Data Readers](#transactions-and-open-data-readers). It does not run a background timer. An expired connection therefore stays open until the next network-bound call triggers the configured action.

Lifecycle and local calls such as `Close`, `Dispose`, `ChangeDatabase`, `GetSchema` and `CreateCommand` never trigger an expiration action.

A call on a wrapper connection that the application has closed never triggers an expiration action either. The plugin does not reopen a connection the application closed; the target driver reports the closed state as usual.

The plugin applies only to the connection used by the application. Internal connections opened by other plugins, such as monitoring or topology connections, have their own lifecycle and are not tracked.

The configured action runs before the pending call. The pending call is not executed when the plugin closes or reconnects the connection.

## Close Action

With `ConnectionLifetimeAction=close`, the plugin closes the current physical connection and throws `ConnectionLifetimeException` with `SqlState` `08003` to report that the connection reached its maximum lifetime and is closed.

Every later network-bound call on the wrapper connection throws the same exception until the application calls `Close` and then `Open` or `OpenAsync` again. Opening the wrapper connection again creates a new physical connection with a new lifetime.

## Reconnect Action

With the default `ConnectionLifetimeAction=reconnect`, the plugin opens a replacement physical connection to the host and port given in the connection string. It does not use a host selected later by routing, read/write splitting or failover. The wrapper installs the replacement in the existing `AwsWrapperConnection` and resets the lifetime only after installation succeeds.

After a successful reconnect, the plugin throws `ConnectionLifetimeSuccessException` with `SqlState` `08S02`. The wrapper connection now holds a usable physical connection, but the pending call was not executed. The application must handle the exception, restore any session state it set on the previous connection, and retry the operation when appropriate.

Commands and batches created from the wrapper connection follow the replacement connection. Dispose every data reader before the next command runs: a reader that is still open when the plugin reconnects blocks its command from moving to the replacement, and the reconnect then fails as a partial installation.

If opening or installing the replacement fails, the plugin throws `ConnectionLifetimeFailedException` with `SqlState` `08001`, and the reconnect failure is available as the `InnerException`. A failed reconnect does not reset the lifetime, so the next network-bound call tries to reconnect again. If the replacement was installed only partially, the plugin closes both connections and later calls fail with `ConnectionLifetimeException` as with the close action.

If another plugin switches the physical connection, for example after failover, the plugin tracks the new connection separately. A host change on the same physical connection does not reset its lifetime.

## Transactions and Open Data Readers

Neither action runs while a transaction started through `BeginTransaction` is active. If the lifetime expires during a transaction, network-bound calls continue on the current physical connection, so the application can still commit or roll back. Deferral does not extend or reset the lifetime.

After the transaction ends, the first network-bound call that is not a data reader call runs the configured action before that call and throws the corresponding exception. The pending call is not executed.

The plugin tracks only transactions started through `BeginTransaction`. A transaction started with SQL, such as `BEGIN` or `START TRANSACTION`, is not detected, so an expiration action can close or replace the connection in the middle of it.

`DbDataReader.Read` and `NextResult` also never trigger an action, including their asynchronous forms. An open data reader belongs to the physical connection it was opened on, so the application can finish reading its rows. The action runs on the next command, batch or transaction call.

## Connection Lifetime Errors

| Errors                             | SqlState | Is the connection valid? | Can the connection be reused?   | Does the session need to be reconfigured? | Does the last query need to be re-executed? |
|------------------------------------|----------|--------------------------|---------------------------------|-------------------------------------------|---------------------------------------------|
| ConnectionLifetimeSuccessException | `08S02`  | Yes                      | Yes                             | Yes                                       | Yes                                         |
| ConnectionLifetimeFailedException  | `08001`  | No                       | Yes, after `Close` and `Open`   | Yes                                       | Yes                                         |
| ConnectionLifetimeException        | `08003`  | No                       | Yes, after `Close` and `Open`   | Yes                                       | Yes                                         |

`ConnectionLifetimeSuccessException` and `ConnectionLifetimeFailedException` both derive from `ConnectionLifetimeException`, so catch the more specific types first.

## Target Driver Connection Pooling

The plugin measures the lifetime of the target driver connection object that the wrapper opens. When the target driver pools connections, which is the default for `Npgsql` and `MySqlConnector`, closing that object returns the underlying server session to the driver's pool instead of ending it, and opening a replacement may reuse a pooled session that was created earlier.

To retire server sessions after a fixed age, combine this plugin with the target driver's own `Connection Lifetime` connection string option, or disable target driver pooling with `Pooling=false`. The target driver's option is separate from `ConnectionLifetimeMs` and is measured in seconds.
