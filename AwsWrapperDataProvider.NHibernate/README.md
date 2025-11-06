# AWS Advanced .NET Data Provider Wrapper - NHibernate Integration

## Overview

This project provides NHibernate integration for the AWS Advanced .NET Data Provider Wrapper, enabling NHibernate applications to take full advantage of AWS Aurora features such as automatic failover, enhanced failure monitoring, and cluster topology awareness.

## Usage

### Basic Configuration

Configure NHibernate to use the AWS Wrapper Driver with your underlying database driver:

```csharp
using AwsWrapperDataProvider.NHibernate;
using NHibernate.Cfg;
using NHibernate.Driver.MySqlConnector;

var connectionString = "Server=your-aurora-cluster.cluster-xyz.us-east-1.rds.amazonaws.com;" +
                       "Database=mydb;" +
                       "User Id=myuser;" +
                       "Password=mypassword;" +
                       "Plugins=failover,initialConnection;";

var cfg = new Configuration()
    .AddAssembly(Assembly.GetExecutingAssembly())
    .AddProperties(new Dictionary<string, string>
    {
        { "connection.connection_string", connectionString },
        { "dialect", "NHibernate.Dialect.MySQLDialect" },
    });

// Configure to use AWS Wrapper Driver with MySqlConnector
cfg.DataBaseIntegration(c => c.UseAwsWrapperDriver<MySqlConnectorDriver>());

var sessionFactory = cfg.BuildSessionFactory();
```

### Connection String Parameters

For detailed information about all available connection string parameters, plugins, and configuration options, see the [main documentation](../docs/)

### Exception Handling

> [!NOTE]
> **Important**: NHibernate wraps underlying exceptions in `HibernateException`. When handling failover scenarios, you must catch `HibernateException` and check the `InnerException` for AWS wrapper-specific exceptions like `FailoverSuccessException` or `TransactionStateUnknownException`.

```csharp
try
{
    using var session = sessionFactory.OpenSession();
    using var transaction = session.BeginTransaction();
    
    // Your NHibernate operations
    session.Save(entity);
    await transaction.CommitAsync();
}
catch (HibernateException ex) when (ex.InnerException is FailoverSuccessException)
{
    // Failover completed successfully - the operation may need to be retried
    Console.WriteLine($"Failover completed successfully: {ex.InnerException.Message}");
}
catch (HibernateException ex) when (ex.InnerException is TransactionStateUnknownException)
{
    // Transaction state is unknown after failover
    Console.WriteLine($"Transaction state unknown: {ex.InnerException.Message}");
}
catch (HibernateException ex)
{
    // Other NHibernate exceptions
    Console.WriteLine($"NHibernate error: {ex.Message}");
}
```

## Supported Database Drivers

The NHibernate integration supports wrapping the following underlying drivers:

- **MySqlConnector**: `UseAwsWrapperDriver<MySqlConnectorDriver>()`
- **Npgsql (PostgreSQL)**: `UseAwsWrapperDriver<NpgsqlDriver>()`

## Example

See the complete [NHibernateExample](../docs/examples/NHibernateExample/) project for a working demonstration that shows:

- How to configure NHibernate with the AWS Wrapper Driver
- Proper exception handling for failover scenarios
- Continuous database operations with automatic failover handling

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper visit the [main documentation](../docs/) directory.
