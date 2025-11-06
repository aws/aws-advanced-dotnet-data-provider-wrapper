# AWS Advanced .NET Data Provider Wrapper - MySqlConnector Dialect

## Overview

This project provides a database dialect implementation for MySqlConnector, enabling the AWS Advanced .NET Data Provider Wrapper to work with MySqlConnector-based applications and ORMs.

## Dependencies

This project depends on:
- **[MySqlConnector](https://www.nuget.org/packages/MySqlConnector/)**: High-performance MySQL connector for .NET

## Usage

The dialect must be explicitly loaded before using MySqlConnector with the AWS Advanced .NET Data Provider Wrapper:

```csharp
using AwsWrapperDataProvider.MySqlConnector;

// Register the MySqlConnector dialect
MySqlConnectorDialectLoader.Load();

// Now you can use MySqlConnector with the AWS Wrapper
var connection = new AwsWrapperConnection(connectionString);
```

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/). For dialect-specific information, see [Custom Dialects](../docs/using-the-dotnet-driver/CustomDialects.md).
