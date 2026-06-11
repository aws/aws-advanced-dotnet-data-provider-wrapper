# Database Dialects

## What are database dialects?
The AWS Advanced .NET Data Provider Wrapper is a wrapper that requires an underlying community .NET data provider (such as [Npgsql](https://www.nuget.org/packages/Npgsql/), [MySqlConnector](https://www.nuget.org/packages/MySqlConnector/), or [MySql.Data](https://www.nuget.org/packages/mysql.data/)), and it is meant to be compatible with any of them. Database dialects help the AWS Advanced .NET Data Provider Wrapper determine what kind of underlying database is being used. To function correctly, the wrapper requires details unique to specific databases such as the default port number, the query used to determine the current host, or how to evaluate whether a host is a writer or a reader. These details can be defined and provided to the wrapper by using database dialects.

By default, the database dialect is determined automatically based on the connection details (for example, the type of the underlying connection and the shape of the host endpoint). If the dialect cannot be fully resolved before connecting, the wrapper refines its choice after establishing the first connection by querying the server.

## Configuration Parameters
| Name            | Required             | Description                                                                                                     | Example                                                                                                                                                                            |
|-----------------|----------------------|-----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `CustomDialect` | No (see notes below) | A custom database dialect. The value should be the `AssemblyQualifiedName` of a class that implements `IDialect`. | `MyNamespace.MyCustomDialect, MyAssembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null` |

> [!NOTE]
>
> The `CustomDialect` parameter is not required. When it is not provided by the user, the AWS Advanced .NET Data Provider Wrapper will attempt to determine which of the existing dialects to use based on other connection details. However, if the dialect is known by the user, setting `CustomDialect` avoids the time it takes to resolve the dialect.

### List of Available Dialects
The following database dialects are built into the wrapper. The wrapper selects dialects by their .NET type, so the table below lists the dialect class together with the database it supports.

| Dialect Class                     | Database                                                                                                                                            |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `AuroraMySqlDialect`              | [Aurora MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_GettingStartedAurora.html)                                         |
| `GlobalAuroraMySqlDialect`        | [Aurora Global Database MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-getting-started.html)            |
| `RdsMultiAzDbClusterMySqlDialect` | [Amazon RDS MySQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)       |
| `RdsMySqlDialect`                 | Amazon RDS MySQL                                                                                                                                    |
| `MySqlDialect`                    | MySQL                                                                                                                                               |
| `AuroraPgDialect`                 | [Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_GettingStartedAurora.html)                                    |
| `GlobalAuroraPgDialect`           | [Aurora Global Database PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-getting-started.html)       |
| `RdsMultiAzDbClusterPgDialect`    | [Amazon RDS PostgreSQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)  |
| `RdsPgDialect`                    | Amazon RDS PostgreSQL                                                                                                                               |
| `PgDialect`                       | PostgreSQL                                                                                                                                          |
| `UnknownDialect`                  | Unknown. Although this dialect exists, do not set it explicitly as it will result in errors.                                                        |

## Custom Dialects
If you are interested in using the AWS Advanced .NET Data Provider Wrapper but your desired database type is not currently supported, it is possible to create a custom dialect.

To create a custom dialect, implement the [`IDialect`](../../AwsWrapperDataProvider/Driver/Dialects/IDialect.cs) interface. See the following classes for examples:

- [`PgDialect`](../../AwsWrapperDataProvider/Driver/Dialects/PgDialect.cs)
  - This is a generic dialect that should work with any PostgreSQL database.
- [`AuroraPgDialect`](../../AwsWrapperDataProvider/Driver/Dialects/AuroraPgDialect.cs)
  - This dialect is an extension of `PgDialect` that adds Aurora PostgreSQL specific behavior.

Once the custom dialect class has been created, tell the AWS Advanced .NET Data Provider Wrapper to use it by setting the `CustomDialect` connection property to the `AssemblyQualifiedName` of the custom dialect class. See below for an example:

```dotnet
var builder = new AwsWrapperConnectionStringBuilder
{
    // ... other connection properties ...
    CustomDialect = typeof(MyCustomDialect).AssemblyQualifiedName,
};
```

When the `CustomDialect` property is set, the wrapper uses the provided dialect and will not attempt to resolve a dialect from the connection details.
