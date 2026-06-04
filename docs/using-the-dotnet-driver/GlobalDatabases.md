# Aurora Global Databases

The AWS Advanced .NET Data Provider Wrapper provides support for [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/), including both in-region and cross-region failover, and region-constrained read/write splitting.

## Overview

An Aurora Global Database is a single Aurora database that spans multiple AWS regions. It replicates data across regions with minimal impact on database performance, enabling low-latency global reads and disaster recovery. A Global Database has a single primary (writer) region and one or more secondary (reader) regions.

The AWS Advanced .NET Data Provider Wrapper supports:

- Multi-region topology awareness (instances are discovered across every region of the Global Database).
- Global writer cluster endpoint recognition (`*.global-*.global.rds.amazonaws.com`).
- Home-region-aware failover through the [GDB Failover Plugin](./using-plugins/UsingTheGdbFailoverPlugin.md).
- Region-constrained read/write splitting through the [GDB Read/Write Splitting Plugin](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md).
- Stale DNS handling for global endpoints through the [Aurora Initial Connection Strategy Plugin](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md).

## Required configuration

The `GlobalClusterInstanceHostPatterns` parameter is **required** for Aurora Global Databases. It provides a comma-separated list of per-region instance host patterns so the wrapper can construct instance endpoints for each region during topology discovery. See [Global cluster instance host patterns](#global-cluster-instance-host-patterns) below.

### Writer connections

Use the global cluster endpoint in your connection string:

```
<global-db-name>.global-<XYZ>.global.rds.amazonaws.com
```

| Parameter                           | Value                                                                                                   | Notes                                                          |
|-------------------------------------|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| `ClusterId`                         | `1`                                                                                                     | Use the same value for all connections to the same cluster.    |
| `Plugins`                           | `initialConnection,failover,efm` or<br/>`initialConnection,gdbFailover,efm`                             | Without connection pooling.                                    |
|                                     | `auroraConnectionTracker,initialConnection,failover,efm` or<br/>`auroraConnectionTracker,initialConnection,gdbFailover,efm` | With connection pooling.                  |
| `GlobalClusterInstanceHostPatterns` | `?.XYZ1.us-east-2.rds.amazonaws.com,?.XYZ2.us-west-2.rds.amazonaws.com`                                  | Required. One pattern per region.                              |

### Reader connections

Use the cluster reader endpoint in your connection string:

```
<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com
```

| Parameter                           | Value                                                                                                   | Notes                                    |
|-------------------------------------|---------------------------------------------------------------------------------------------------------|------------------------------------------|
| `ClusterId`                         | `1`                                                                                                     | Use the same value as writer connections.|
| `Plugins`                           | `initialConnection,failover,efm` or<br/>`initialConnection,gdbFailover,efm`                             | Without connection pooling.              |
|                                     | `auroraConnectionTracker,initialConnection,failover,efm` or<br/>`auroraConnectionTracker,initialConnection,gdbFailover,efm` | With connection pooling.|
| `GlobalClusterInstanceHostPatterns` | Same as writer configuration.                                                                           |                                          |
| `FailoverMode`                      | `StrictReader` or `ReaderOrWriter`                                                                      | Depending on your requirements.          |

> [!NOTE]
> The `gdbFailover` plugin extends the `failover` plugin with home-region awareness. Use `gdbFailover` when you need different failover behavior for in-home and out-of-home scenarios; otherwise `failover` is sufficient. Do not enable `failover` and `gdbFailover` at the same time for the same connection.

## Example configuration

### PostgreSQL writer connection

```dotnet
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.Npgsql;
using Npgsql;

NpgsqlDialectLoader.Load();

const string connectionString =
    "Host=my-global-db.global-xyz.global.rds.amazonaws.com;" +
    "Database=mydb;Username=username;Password=password;" +
    "ClusterId=1;" +
    "Plugins=initialConnection,gdbFailover,efm;" +
    "GlobalClusterInstanceHostPatterns=?.abc123.us-east-1.rds.amazonaws.com,?.def456.us-west-2.rds.amazonaws.com;" +
    "FailoverHomeRegion=us-east-1;";

await using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);
await connection.OpenAsync();
```

### MySQL reader connection

```dotnet
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.MySqlClient;
using MySql.Data.MySqlClient;

MySqlClientDialectLoader.Load();

const string connectionString =
    "Server=my-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com;" +
    "Database=mydb;User Id=username;Password=password;" +
    "ClusterId=1;" +
    "Plugins=initialConnection,gdbFailover,efm;" +
    "GlobalClusterInstanceHostPatterns=?.abc123.us-east-1.rds.amazonaws.com,?.def456.us-west-2.rds.amazonaws.com;" +
    "FailoverHomeRegion=us-east-1;" +
    "ActiveHomeFailoverMode=strict-home-reader;" +
    "InactiveHomeFailoverMode=strict-home-reader;";

await using var connection = new AwsWrapperConnection<MySqlConnection>(connectionString);
await connection.OpenAsync();
```

## Important considerations

### Unique instance names across regions

> [!WARNING]
> The Global Database plugins do not support duplicate instance names across regions. Ensure that all instance names are unique across all regions of the Global Database.

### Global cluster instance host patterns

The `GlobalClusterInstanceHostPatterns` parameter is required for Aurora Global Databases. It should contain:

- A comma-separated list of host patterns, one per region of the Global Database.
- A `?` placeholder that is replaced with each instance name when constructing instance endpoints.
- Different cluster identifiers for each region (for example, `XYZ1`, `XYZ2`).

Two formats are supported:

- **Standard RDS instance endpoints**, where the region is parsed from the endpoint domain:
  `?.XYZ1.us-east-2.rds.amazonaws.com,?.XYZ2.us-west-2.rds.amazonaws.com`
- **Custom domains**, where the region is provided as a square-bracket prefix:
  `[us-east-2]?.customHost,[us-west-2]?.anotherCustomHost`. A port can also be included:
  `[us-east-2]?.customHost:8888,[us-west-2]?.anotherCustomHost:9999`.

If a topology row reports an `AWS_REGION` that has no corresponding entry in this list, the wrapper raises an error indicating the missing region template.

### Plugin selection

- **Connection pooling**: include the `auroraConnectionTracker` plugin when using connection pooling.
- **Home region**: use `gdbFailover` (instead of `failover`) when you need different failover behavior for in-home and out-of-home scenarios.

### Failover behavior

- **In-region failover**: automatic failover within the same region.
- **Cross-region failover**: failover to an instance in a different region according to the configured failover mode.
- **DNS handling**: the `initialConnection` plugin helps mitigate stale DNS issues for global endpoints by verifying the connected host's role and reconnecting to the actual writer when needed.

## Related documentation

- [GDB Failover Plugin](./using-plugins/UsingTheGdbFailoverPlugin.md)
- [GDB Read/Write Splitting Plugin](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md)
- [Failover Plugin](./using-plugins/UsingTheFailoverPlugin.md)
- [Read/Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)
- [Aurora Initial Connection Strategy Plugin](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md)
- [Reader Selection Strategies](./ReaderSelectionStrategies.md)
- [Custom Dialects](./CustomDialects.md)
