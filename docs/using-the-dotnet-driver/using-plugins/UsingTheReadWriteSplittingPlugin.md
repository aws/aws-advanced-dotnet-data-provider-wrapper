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

## Session state is not carried across a switch

Each switch opens a connection to the target instance and closes the previous one. The underlying driver may return the closed connection to its own pool and hand the same socket back later, but the session is reset in the process, so **session state does not survive a switch**: temporary tables, `SET` variables, session-level advisory locks and server-side prepared statements are all gone once you switch away, and they do not come back when you switch to that role again.

Keep anything session-scoped on one side of a switch, or re-establish it after switching.

## Limitations

### What survives a connection switch

| | Survives a switch? |
|---|---|
| `AwsWrapperConnection` | Yes — it is the same object throughout; only the underlying connection changes. |
| `DbCommand`, `DbBatch` | Yes. The wrapper tracks the commands and batches created from an `AwsWrapperConnection` and re-points them at the new connection, so a command created before a switch executes against the connection current at execution time. |
| `DbDataReader` | **No.** A reader is bound to the connection it was created on, and that connection is closed as part of the switch. |
| An attached `DbTransaction` | **No.** Transactions are not re-pointed. A command that still carries a transaction from the previous connection will fail or, worse, run outside the transaction the application believes it is in. |

As a matter of style, commands and readers are best kept short-lived — create them where you use them rather than holding them across a switch. That keeps you clear of all of the above.

### Do not switch while a reader is open

Executing a statement on a connection that already has an open `DbDataReader` is not valid in the first place: neither Npgsql nor MySqlConnector supports multiple concurrent result sets on one connection, and both reject it. Because the read-only and read-write session statements are themselves commands, this applies to them too.

If you do it anyway, the plugin raises an `InvalidOperationException` reporting an open data reader, and the switch is left partly applied — the previous connection is not closed, and commands created after the reader are still pointing at it. Finish and dispose readers before switching.

## Example

[PGReadWriteSplitting.cs](../../examples/AwsWrapperDataProviderExample/PGReadWriteSplitting.cs) demonstrates how to enable and use the Read/Write Splitting plugin with the AWS Advanced .NET Data Provider Wrapper. The example connects to an Aurora PostgreSQL cluster, performs a write on the writer, switches to a reader for a read, then switches back to the writer.