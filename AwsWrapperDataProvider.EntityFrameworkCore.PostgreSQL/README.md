# AWS Advanced .NET Data Provider Wrapper - Entity Framework Core PostgreSQL Integration

## Overview

This project provides Entity Framework Core integration for PostgreSQL databases using the AWS Advanced .NET Data Provider Wrapper, enabling EF Core applications to take advantage of AWS Aurora PostgreSQL features like automatic failover.

## Dependencies

This project depends on:
- **[Npgsql.EntityFrameworkCore.PostgreSQL](https://www.nuget.org/packages/Npgsql.EntityFrameworkCore.PostgreSQL/)**: Npgsql EF Core provider for PostgreSQL
- **[Microsoft.EntityFrameworkCore](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore/)**: Entity Framework Core framework

## Usage

Configure your DbContext to use the AWS Wrapper:

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseAwsWrapperNpgsql(
        connectionString,
        wrappedOptions => wrappedOptions.UseNpgsql(connectionString)));
```

## Example

See the [PgEntityFrameworkExample](../docs/examples/PgEntityFrameworkExample/) project for a complete working demonstration.

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
