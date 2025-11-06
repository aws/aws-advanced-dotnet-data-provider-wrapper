# AWS Advanced .NET Data Provider Wrapper - MySqlConnector Dialect

## Overview

This project provides a database dialect implementation for MySqlConnector, enabling the AWS Advanced .NET Data Provider Wrapper to work with MySqlConnector-based applications and ORMs.

## Dependencies

This project depends on:
- **MySqlConnector**: High-performance MySQL connector for .NET

## Usage

The dialect must be explicitly loaded before using MySqlConnector with the AWS Wrapper:

```csharp
using AwsWrapperDataProvider.MySqlConnector;

// Register the MySqlConnector dialect
MySqlConnectorDialectLoader.Load();

// Now you can use MySqlConnector with the AWS Wrapper
var connection = new AwsWrapperConnection(connectionString);
```

The dialect enables support for:
- Aurora MySQL clusters
- RDS MySQL instances
- Self-managed MySQL databases

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
