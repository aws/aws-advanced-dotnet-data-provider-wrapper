# AWS Advanced .NET Data Provider Wrapper - Npgsql Dialect

## Overview

This project provides a database dialect implementation for Npgsql, enabling the AWS Advanced .NET Data Provider Wrapper to work with PostgreSQL applications and ORMs using Npgsql.

## Dependencies

This project depends on:
- **Npgsql**: High-performance PostgreSQL driver for .NET

## Usage

The dialect must be explicitly loaded before using Npgsql with the AWS Wrapper:

```csharp
using AwsWrapperDataProvider.Npgsql;

// Register the Npgsql dialect
NpgsqlDialectLoader.Load();

// Now you can use Npgsql with the AWS Wrapper
var connection = new AwsWrapperConnection(connectionString);
```

The dialect enables support for:
- Aurora PostgreSQL clusters
- RDS PostgreSQL instances
- Self-managed PostgreSQL databases

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
