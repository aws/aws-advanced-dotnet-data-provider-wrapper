# Read/Write Splitting Plugin

The read/write splitting plugin adds functionality to switch between writer and reader instances by executing SQL statements that set the session transaction mode. When you execute a statement that sets the session to read-only, the plugin connects to a reader instance according to a [reader selection strategy](../ReaderSelectionStrategies.md) and directs subsequent commands to that instance. Executing a statement that sets the session to read-write switches the connection back to the writer. The plugin switches the underlying physical connection so that read traffic can be distributed across reader instances while write traffic goes to the writer.

## Loading the Read/Write Splitting Plugin

The read/write splitting plugin is not loaded by default. To load the plugin, include the plugin code `readWriteSplitting` in the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) connection parameter.

If you use the read/write splitting plugin together with the failover and host monitoring plugins, the read/write splitting plugin must be listed before these plugins in the plugin chain so that failover exceptions are processed correctly. You can rely on the default `AutoSortPluginOrder` behavior to order plugins, or specify the order explicitly. For example:

```dotnet
var connectionString = "Host=my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com;" +
    "Database=myapp;Username=admin;Password=pwd;" +
    "Plugins=failover,efm,readWriteSplitting";

using var connection = new AwsWrapperConnection(connectionString);
```

To use the read/write splitting plugin without the failover plugin, include only the plugins you need:

```dotnet
var connectionString = "Host=my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com;" +
    "Database=myapp;Username=admin;Password=pwd;" +
    "Plugins=readWriteSplitting";
```

When using read/write splitting against Aurora clusters, you must also include either the failover plugin or another mechanism (such as the initial connection strategy plugin) so that the driver can obtain the cluster topology. See [Supplying the connection string](#supplying-the-connection-string) below.

## Supplying the connection string

When using the read/write splitting plugin with Aurora clusters, you do not need to supply multiple instance URLs in the connection string. Supply the URL for the initial instance (or cluster endpoint) you connect to. You must include either the failover plugin or the Aurora host list plugin in your plugin chain so that the driver can query Aurora for its topology. See [Loading the Read/Write Splitting Plugin](#loading-the-readwrite-splitting-plugin) for how to configure the plugin list.

## Using the Read/Write Splitting Plugin against non-Aurora clusters

The read/write splitting plugin is not currently supported for non-Aurora clusters.

## How read/write switching works

The plugin switches between writer and reader based on the **SQL text** of the command being executed. Before running a command, the plugin checks whether the command sets the session to read-only or read-write. If it does, the plugin switches the underlying connection as needed, then executes the command.

### Triggering a switch to a reader

Execute one of the following statements (depending on your database engine) so that subsequent commands use a reader connection:

**PostgreSQL:**

```sql
SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY
```

**MySQL:**

```sql
SET SESSION TRANSACTION READ ONLY
```

After this, the logical connection is backed by a physical connection to a reader instance chosen according to the configured [reader selection strategy](#reader-selection).

### Triggering a switch back to the writer

Execute one of the following statements to direct subsequent commands to the writer:

**PostgreSQL:**

```sql
SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE
```

**MySQL:**

```sql
SET SESSION TRANSACTION READ WRITE
```

Example pattern:

```dotnet
using var connection = new AwsWrapperConnection(connectionString);
await connection.OpenAsync();

// Use writer (default after open)
await ExecuteNonQuery(connection, "INSERT INTO my_table VALUES (1, 'data')");

// Switch to reader for a read
await ExecuteNonQuery(connection, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"); // PostgreSQL
var count = await ExecuteScalar(connection, "SELECT COUNT(*) FROM my_table");

// Switch back to writer
await ExecuteNonQuery(connection, "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE");
await ExecuteNonQuery(connection, "UPDATE my_table SET value = 'updated' WHERE id = 1");
```

Whenever the plugin sees a command that sets the session to read-only, it ensures the current physical connection is a reader (connecting to one if necessary). Whenever it sees a command that sets the session to read-write, it switches back to the writer connection.

## Read/Write Splitting Plugin Parameters

| Parameter                         | Value  | Required | Description                                                                                                                                                                                                 | Default Value |
|-----------------------------------|--------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `RWSplittingReaderHostSelectorStrategy` | String | No       | The strategy used to select a reader host when switching to a reader. See [Reader Selection](#reader-selection) and the [reader selection strategies](../ReaderSelectionStrategies.md) table for allowed values. | `Random`      |

## Reader selection

To use a reader selection strategy other than the default, set the `RWSplittingReaderHostSelectorStrategy` connection parameter to one of the strategies described in the [Reader Selection Strategies](../ReaderSelectionStrategies.md) document. For example, to use round-robin selection:

```dotnet
var connectionString = "Host=my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com;" +
    "Database=myapp;Username=admin;Password=pwd;" +
    "Plugins=failover,efm,readWriteSplitting;" +
    "RWSplittingReaderHostSelectorStrategy=RoundRobin";
```

## Cached reader connection

The first time a command sets the session to read-only, the plugin opens a new physical connection to a reader and uses it for that logical connection. That reader connection is then cached for the lifetime of the `AwsWrapperConnection`. Subsequent commands that set the session to read-only on the same connection object reuse the same reader connection. Commands that set the session to read-write reuse the existing writer connection. This avoids repeatedly opening new connections when you alternate between read and write within the same connection.

## Limitations

### Statements and result sets bound to the current connection

When a `DbCommand` or `DbDataReader` is created, it is bound to the underlying database connection at that time. There is no standard ADO.NET way to change the connection used by an existing command or reader. Therefore, if the read/write splitting plugin switches the underlying connection (e.g., after executing a read-only or read-write session statement), any commands or readers that were created before the switch continue to use the previous connection. To avoid incorrect behavior, create new `DbCommand` and `DbDataReader` instances after switching between reader and writer. Do not reuse commands or readers across such switches.

### Transaction behavior

- You cannot switch from a reader back to the writer while inside a transaction that was started on the reader. The plugin will throw `ReadWriteSplittingDbException` if you execute a statement that sets the session to read-write while the current connection is a reader and a transaction is active. Commit or roll back the transaction first, then switch to the writer.
- If you start a transaction on the writer and then switch to read-only, read-only operations run on a reader; switching back to read-write while that transaction is still open is not allowed from the reader. The plugin enforces this to keep transaction semantics correct.

## Example

[PGReadWriteSplitting.cs](../../examples/AwsWrapperDataProviderExample/PGReadWriteSplitting.cs) demonstrates how to enable and use the Read/Write Splitting plugin with the AWS Advanced .NET Data Provider Wrapper. The example connects to an Aurora PostgreSQL cluster, performs a write on the writer, switches to a reader for a read, then switches back to the writer.