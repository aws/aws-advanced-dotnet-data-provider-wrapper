# AWS Advanced .NET Data Provider Wrapper - Entity Framework Core MySQL Integration

## Overview

This project provides Entity Framework Core integration for MySQL databases using the AWS Advanced .NET Data Provider Wrapper, enabling EF Core applications to take advantage of AWS Aurora MySQL features like automatic failover.

## Dependencies

This project depends on:
- **MySql.Data**: MySQL Connector/NET for database connectivity
- **Microsoft.EntityFrameworkCore**: Entity Framework Core framework

## Usage

Configure your DbContext to use the AWS Wrapper:

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseAwsWrapper(
        connectionString,
        wrappedOptions => wrappedOptions.UseMySQL(connectionString)));
```

## Example

See the [MySqlEntityFrameworkExample](../docs/examples/MySqlEntityFrameworkExample/) project for a complete working demonstration.

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
