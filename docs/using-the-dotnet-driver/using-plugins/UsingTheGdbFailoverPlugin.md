# Global Database (GDB) Failover Plugin

The AWS Advanced .NET Data Provider Wrapper uses the GDB Failover Plugin to provide minimal downtime in the event of a DB instance failure in an Aurora Global Database. The plugin extends the [Failover Plugin](./UsingTheFailoverPlugin.md), and unless explicitly stated otherwise, the information and recommendations for the [Failover Plugin](./UsingTheFailoverPlugin.md) also apply to the GDB Failover Plugin.

## Differences between the GDB Failover Plugin and the Failover Plugin

The GDB Failover Plugin introduces the notion of a **home region** and extends the configuration to allow different failover logic for **in-home** and **out-of-home** cases.

You define a home region with an AWS region name. This introduces two cases:

- **in-home**: the primary (writer) region of the Global Database is the same as the specified home region.
- **out-of-home**: the Global Database has switched over to another region, and the primary region is not the same as the specified home region.

You can specify different failover logic for each case. The following examples show how in-home and out-of-home cases affect failover.

**Example 1**

When an application needs a writer connection, it makes sense to follow the writer (`strict-writer`). However, some applications may choose not to follow a writer node when cross-region failover occurs (see [Configuration Example 3](#configuration-example-3) below).

- **in-home**: when in-region failover occurs, the wrapper reconnects to a new writer node and continues serving the application with a write connection.
- **out-of-home**: the application stops performing writes, prioritizing reduced connection latency over being connected to a writer node in another region.

**Example 2**

When an application needs a reader connection, prioritize readers in the home region to reduce connection latency (see [Configuration Example 2](#configuration-example-2) below).

- **in-home**: the primary region of the Global Database is the same as the home region, so connect to any reader.
- **out-of-home**: connect to readers in the specified home region, even though those readers are no longer part of the primary region of the Global Database.

The [Failover Plugin](./UsingTheFailoverPlugin.md) does not support the home region concept and cannot be used for the cases above.

## Using the GDB Failover Plugin

The GDB Failover Plugin is not loaded by default. To enable it, add the plugin code `gdbFailover` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) connection parameter. After you load the plugin, the failover feature is enabled.

Please refer to the [failover configuration guide](../FailoverConfigurationGuide.md) for tips to keep in mind when using a failover plugin.

> [!WARNING]
> Do not use the `failover` and `gdbFailover` plugins at the same time for the same connection.

### GDB Failover Plugin Configuration Parameters

In addition to the parameters that you can configure for the underlying driver, you can pass the following connection parameters to the AWS Advanced .NET Data Provider Wrapper to specify additional failover behavior.

| Parameter                           |  Value  |                                                                  Required                                                                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Default Value                                                                                                                          |
|-------------------------------------|:-------:|:-------------------------------------------------------------------------------------------------------------------------------------------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `FailoverHomeRegion`                | String  | If connecting using an IP address, a custom domain URL, a Global Database endpoint, or another endpoint with no region: Yes<br><br>Otherwise: No | Defines the home region.<br><br>Examples: `us-west-2`, `us-east-1`.<br><br>If omitted, the value is parsed from the connection endpoint. For regional cluster endpoints and instance endpoints, it is set to the region of the provided endpoint. If the endpoint has no region (for example, a Global Database endpoint or an IP address), this parameter is mandatory.                                                                                       | Parsed from the connection endpoint when the endpoint has a region.                                                                    |
| `ActiveHomeFailoverMode`            | String  |                                                                     No                                                                      | Defines the failover mode when the GDB primary region **is** the home region. See [Failover modes](#failover-modes) for possible values.                                                                                                                                                                                                                                                                                                                                                                                                                                              | For an Aurora writer cluster endpoint or a Global Database endpoint: `strict-writer`. Otherwise: `home-reader-or-writer`.               |
| `InactiveHomeFailoverMode`          | String  |                                                                     No                                                                      | Defines the failover mode when the GDB primary region **is not** the home region. See [Failover modes](#failover-modes) for possible values.                                                                                                                                                                                                                                                                                                                                                                                                                                          | For an Aurora writer cluster endpoint or a Global Database endpoint: `strict-writer`. Otherwise: `home-reader-or-writer`.               |
| `GlobalClusterInstanceHostPatterns` | String  |                                               For Global Databases: Yes<br><br>Otherwise: No                                                | A comma-separated list of per-region instance host patterns. Required for Aurora Global Databases. Each pattern uses a `?` placeholder for the instance name. Standard RDS endpoints parse the region from the domain (`?.XYZ1.us-east-2.rds.amazonaws.com`); custom domains use a `[region]` prefix (`[us-east-2]?.customHost`). See [Aurora Global Databases](../GlobalDatabases.md#global-cluster-instance-host-patterns).                                                                                                                                                            |                                                                                                                                        |
| `ClusterInstanceHostPattern`        | String  |                              If connecting using an IP address or custom domain URL: Yes<br><br>Otherwise: No                               | Specifies the cluster instance DNS pattern used to build a complete instance endpoint when connecting via an IP address or custom domain. A `?` character is a placeholder for the DB instance identifier. See the [Failover Plugin host pattern documentation](./UsingTheFailoverPlugin.md#host-pattern).                                                                                                                                                                                                                                                                              | Acquired automatically from the connection endpoint for standard RDS clusters.                                                         |
| `ClusterTopologyRefreshRateMs`      | Integer |                                                                     No                                                                      | Cluster topology refresh rate in milliseconds when the cluster is not in failover.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `30000`                                                                                                                                |
| `ClusterTopologyHighRefreshRateMs`  | Integer |                                                                     No                                                                      | Interval in milliseconds between topology updates after a new writer is detected following a failover event. The monitor uses this higher rate for a short time after detecting a new writer.                                                                                                                                                                                                                                                                                                                                                                                          | `100`                                                                                                                                  |
| `FailoverTimeoutMs`                 | Integer |                                                                     No                                                                      | Maximum allowed time in milliseconds to reconnect to a new writer or reader instance after failover is initiated.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `300000`                                                                                                                               |
| `FailoverReaderHostSelectorStrategy`| String  |                                                                     No                                                                      | Strategy used to select a reader node during failover. See the [Reader Selection Strategies](../ReaderSelectionStrategies.md) table for allowed values.                                                                                                                                                                                                                                                                                                                                                                                                                               | `Random`                                                                                                                               |
| `ClusterId`                         | String  |                          If connecting to multiple database clusters within a single application: Yes<br><br>Otherwise: No                  | A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. When supporting multiple clusters, all connection strings for the same cluster must use identical values, and connection strings for different clusters must use distinct values.                                                                                                                                                                                                                                                                                            | `1`                                                                                                                                    |
| `TelemetryFailoverAdditionalTopTrace`| Boolean |                                                                     No                                                                      | Allows the wrapper to produce an additional telemetry span associated with failover, which helps telemetry analysis in AWS CloudWatch. Only applies when telemetry is enabled; see [Telemetry](../Telemetry.md).                                                                                                                                                                                                                                                                                                                                                                       | `false`                                                                                                                                |
| `SkipFailoverOnInterruptedThread`   | Boolean |                                                                     No                                                                      | Enable to skip failover if the current thread is interrupted. This may leave the connection in an invalid state, so the connection should be disposed.                                                                                                                                                                                                                                                                                                                                                                                                                                | `false`                                                                                                                                |

## Failover modes

The `ActiveHomeFailoverMode` and `InactiveHomeFailoverMode` parameters accept the following kebab-case values. During failover, the wrapper determines the writer's region from the refreshed topology, compares it to the home region, and applies `ActiveHomeFailoverMode` (in-home) or `InactiveHomeFailoverMode` (out-of-home).

| Value                          | Behavior                                                                                                                              |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `strict-writer`                | Follows the writer node and connects to the new writer in any region when it changes.                                                |
| `strict-home-reader`           | Connects only to reader nodes in the home region. If no such reader is available, raises a failover-failed error.                     |
| `strict-out-of-home-reader`    | Connects only to reader nodes outside the home region.                                                                               |
| `strict-any-reader`            | Connects to any reader node in any region.                                                                                           |
| `home-reader-or-writer`        | Connects to a reader in the home region first, falling back to a writer (in any region) if no home-region reader is available.        |
| `out-of-home-reader-or-writer` | Connects to a reader outside the home region first, falling back to a writer if no out-of-home reader is available.                   |
| `any-reader-or-writer`         | Connects to any available host (reader or writer) in any region.                                                                     |

## Failover errors

The GDB Failover Plugin raises the same failover exceptions as the [Failover Plugin](./UsingTheFailoverPlugin.md#failover-errors): `FailoverSuccessException`, `FailoverFailedException`, and `TransactionStateUnknownException`. Handle them the same way. See the [Failover Plugin documentation](./UsingTheFailoverPlugin.md#failover-errors) for details and recommended handling, including the warnings about proper usage of the `AwsWrapperConnection` object.

## Failover configuration examples

### Configuration Example 1

**Goal:** Provide a writer connection. The application is deployed in `us-west-1` and connects to a Global Database with `us-east-1`, `us-east-2`, and `us-west-1` regions.

**Solution:**

```
FailoverHomeRegion=us-west-1;
ActiveHomeFailoverMode=strict-writer;
InactiveHomeFailoverMode=strict-writer;
GlobalClusterInstanceHostPatterns=?.XYZ1.us-east-1.rds.amazonaws.com,?.XYZ2.us-east-2.rds.amazonaws.com,?.XYZ3.us-west-1.rds.amazonaws.com;
Plugins=initialConnection,gdbFailover,efm;
```

Use the Global Database endpoint in your connection string. Replace `XYZ1`, `XYZ2`, `XYZ3` with the values for your database.

### Configuration Example 2

**Goal:** Provide a reader connection in `us-west-1`. The application is deployed in `us-west-1` and connects to a Global Database with `us-east-1`, `us-east-2`, and `us-west-1` regions. If the GDB primary region switches over, prioritize reader connections in `us-west-1`.

**Solution:**

```
FailoverHomeRegion=us-west-1;
ActiveHomeFailoverMode=strict-home-reader;
InactiveHomeFailoverMode=strict-home-reader;
GlobalClusterInstanceHostPatterns=?.XYZ1.us-east-1.rds.amazonaws.com,?.XYZ2.us-east-2.rds.amazonaws.com,?.XYZ3.us-west-1.rds.amazonaws.com;
Plugins=initialConnection,gdbFailover,efm;
```

Use the cluster reader endpoint in `us-west-1` (`<cluster-name>.cluster-ro-XYZ3.us-west-1.rds.amazonaws.com`).

### Configuration Example 3

**Goal:** Provide a writer connection while the primary region is closest to the application. When the primary region switches over and network latency becomes unacceptable, the application deployment in `us-west-1` becomes inactive and lets deployments in other regions process transactions. The Global Database has `us-east-1`, `us-east-2`, and `us-west-1` regions.

**Solution:** Configure the deployment in `us-west-1` as follows. Other deployments need similar configuration with different home regions.

```
FailoverHomeRegion=us-west-1;
ActiveHomeFailoverMode=strict-writer;
InactiveHomeFailoverMode=strict-any-reader;
GlobalClusterInstanceHostPatterns=?.XYZ1.us-east-1.rds.amazonaws.com,?.XYZ2.us-east-2.rds.amazonaws.com,?.XYZ3.us-west-1.rds.amazonaws.com;
Plugins=initialConnection,gdbFailover,efm;
```

Use the cluster writer endpoint in `us-west-1` (`<cluster-name>.cluster-XYZ3.us-west-1.rds.amazonaws.com`).

**Explanation:** While the primary region is `us-west-1`, the application needs writable connections. Using the cluster writer endpoint, the wrapper opens connections to a writer node. If in-region failover occurs, it reconnects to a new writer in `us-west-1`. Because the writer is still in the home region, this is treated as in-home and uses `ActiveHomeFailoverMode=strict-writer`, which keeps latency low. When a cross-region failover event occurs and the new writer is, say, in `us-east-1`, the wrapper treats this as out-of-home and uses `InactiveHomeFailoverMode=strict-any-reader`, serving a reader connection from any available region.

> [!WARNING]
> These examples cover failover settings only. A complete configuration may require settings for other plugins. For instance, `initialConnection` may require additional parameters to be set.

## Example

The [PG Failover](../../examples/AwsWrapperDataProviderExample/PGFailover.cs) and [MySQL Failover](../../examples/AwsWrapperDataProviderExample/MySqlFailover.cs) examples use the `failover` plugin, but they can also be used with the `gdbFailover` plugin by adjusting the configuration parameters according to the tables above.

## Related documentation

- [Aurora Global Databases](../GlobalDatabases.md)
- [Failover Plugin](./UsingTheFailoverPlugin.md)
- [Failover Configuration Guide](../FailoverConfigurationGuide.md)
- [Aurora Initial Connection Strategy Plugin](./UsingTheAuroraInitialConnectionStrategyPlugin.md)
- [Reader Selection Strategies](../ReaderSelectionStrategies.md)
