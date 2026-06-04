# Global Database (GDB) Read/Write Splitting Plugin

The GDB Read/Write Splitting Plugin extends the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) with settings that improve support for Aurora Global Databases.

The plugin adds the notion of a **home region** and lets you constrain new connections to that region. This is useful to avoid opening connections in remote AWS regions where the added latency cannot be tolerated.

Unless otherwise stated, all recommendations, configurations, and behavior described for the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) apply to the GDB Read/Write Splitting Plugin.

## Loading the GDB Read/Write Splitting Plugin

The GDB Read/Write Splitting Plugin is not loaded by default. To load it, include the plugin code `gdbReadWriteSplitting` in the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) connection parameter.

If you load the GDB Read/Write Splitting Plugin alongside the failover and host monitoring plugins, it **must be listed before** those plugins in the plugin chain so that failover exceptions are processed correctly. You can rely on the default `AutoSortPluginOrder` behavior to order plugins, or specify the order explicitly.

```dotnet
var connectionString =
    "Host=my-global-db.global-xyz.global.rds.amazonaws.com;" +
    "Database=myapp;Username=admin;Password=pwd;" +
    "Plugins=gdbReadWriteSplitting,failover,efm;" +
    "GdbRwHomeRegion=us-east-1;";
```

To use the GDB Read/Write Splitting Plugin without the failover plugin, include only the plugins you need:

```dotnet
var connectionString =
    "Host=my-global-db.global-xyz.global.rds.amazonaws.com;" +
    "Database=myapp;Username=admin;Password=pwd;" +
    "Plugins=gdbReadWriteSplitting;" +
    "GdbRwHomeRegion=us-east-1;";
```

> [!WARNING]
> Do not use the `readWriteSplitting` and `gdbReadWriteSplitting` plugins at the same time for the same connection.

## Using the plugin against non-GDB clusters

The GDB Read/Write Splitting Plugin can be used against single-region Aurora and RDS clusters. However, because those cluster types are single-region, setting a home region has little effect: the home region is auto-detected from the endpoint and all instances live in that region, so the region restrictions are trivially satisfied.

## Configuration parameters

| Parameter                          |  Value  |                                                              Required                                                              | Description                                                                                                                                                                                                                                                                                                                                                            | Default Value |
|------------------------------------|:-------:|:---------------------------------------------------------------------------------------------------------------------------------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------:|
| `RWSplittingReaderHostSelectorStrategy` | String  |                                                                No                                                                 | The strategy used to select a reader host when switching to a reader. See the [Reader Selection Strategies](../ReaderSelectionStrategies.md) table for allowed values.                                                                                                                                                                                              | `Random`      |
| `GdbRwHomeRegion`                  | String  | If connecting using an IP address, a custom domain URL, a Global Database endpoint, or another endpoint with no region: Yes<br><br>Otherwise: No | Defines the home region.<br><br>Examples: `us-west-2`, `us-east-1`.<br><br>If omitted, the value is parsed from the connection endpoint. For regional cluster endpoints and instance endpoints, it is set to the region of the provided endpoint. If the endpoint has no region (for example, a Global Database endpoint or an IP address), this parameter is mandatory. | `null` (parsed from the endpoint when the endpoint has a region) |
| `GdbRwRestrictWriterToHomeRegion`  | Boolean |                                                                No                                                                 | If `true`, prevents connecting to a writer node outside the home region. An exception is raised when such a connection is requested (unless `GdbEnableGlobalWriteForwarding` is `true`).                                                                                                                                                                              | `true`        |
| `GdbRwRestrictReaderToHomeRegion`  | Boolean |                                                                No                                                                 | If `true`, restricts reader candidates to the home region. If no readers in the home region are available, an exception is raised.                                                                                                                                                                                                                                   | `true`        |
| `GdbEnableGlobalWriteForwarding`   | Boolean |                                                                No                                                                 | If `true`, allows connections in a secondary region to forward write queries to the primary global region. Useful when your home region is a secondary global region. Requires [Global Write Forwarding](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-write-forwarding.html) to be enabled on the Global Database.            | `false`       |

## How region restrictions work

- **Writer connections** (`GdbRwRestrictWriterToHomeRegion=true`): when a writer connection is requested and the writer is not in the home region, the plugin raises a `ReadWriteSplittingDbException`. If `GdbEnableGlobalWriteForwarding=true`, the plugin allows the connection instead and logs that global write forwarding is enabled.
- **Reader connections** (`GdbRwRestrictReaderToHomeRegion=true`): the plugin filters reader candidates to hosts in the home region before applying the reader selection strategy. If no readers are available in the home region, it raises a `ReadWriteSplittingDbException`.

When both restrictions are `false`, the plugin behaves like the standard [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md), selecting readers and writers from any region.

## How read/write switching works

Read/write switching works exactly as described in the [Read/Write Splitting Plugin documentation](./UsingTheReadWriteSplittingPlugin.md#how-read-write-switching-works): the plugin inspects the SQL text of each command and switches the underlying connection when it sees a statement that sets the session to read-only or read-write.

**PostgreSQL:**

```sql
SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY   -- switch to reader
SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE  -- switch to writer
```

**MySQL:**

```sql
SET SESSION TRANSACTION READ ONLY   -- switch to reader
SET SESSION TRANSACTION READ WRITE  -- switch to writer
```

## Limitations

The same limitation described for the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md#limitations) applies: a `DbCommand` or `DbDataReader` is bound to the underlying connection at the time it is created. Create new `DbCommand` and `DbDataReader` instances after switching between reader and writer; do not reuse them across a switch.

## Example

See the [PGReadWriteSplitting.cs](../../examples/AwsWrapperDataProviderExample/PGReadWriteSplitting.cs) example for the read/write splitting pattern. To adapt it for a Global Database, replace `readWriteSplitting` with `gdbReadWriteSplitting` in the `Plugins` parameter and set `GdbRwHomeRegion` (and the restriction parameters) as needed.

## Related documentation

- [Aurora Global Databases](../GlobalDatabases.md)
- [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md)
- [GDB Failover Plugin](./UsingTheGdbFailoverPlugin.md)
- [Reader Selection Strategies](../ReaderSelectionStrategies.md)
