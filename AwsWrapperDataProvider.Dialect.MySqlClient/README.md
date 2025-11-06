# AWS Advanced .NET Data Provider Wrapper - MySql.Data Dialect

## Overview

This project provides a database dialect implementation for MySql.Data (MySQL Connector/NET), enabling the AWS Advanced .NET Data Provider Wrapper to work with MySql.Data-based applications and ORMs.

## Dependencies

This project depends on:
- **[MySql.Data](https://www.nuget.org/packages/MySql.Data/)**: Official MySQL Connector/NET from Oracle

## Usage

The dialect must be explicitly loaded before using MySql.Data with the AWS Advanced .NET Data Provider Wrapper:

```csharp
using AwsWrapperDataProvider.MySqlClient;

// Register the MySql.Data dialect
MySqlClientDialectLoader.Load();

// Now you can use MySql.Data with the AWS Advanced .NET Data Provider Wrapper
var connection = new AwsWrapperConnection(connectionString);
```

The dialect enables support for:
- Aurora MySQL clusters
- RDS MySQL instances
- Self-managed MySQL databases

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/). For dialect-specific information, see [Custom Dialects](../docs/using-the-dotnet-driver/CustomDialects.md).
