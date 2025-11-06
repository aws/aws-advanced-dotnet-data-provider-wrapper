# AWS Advanced .NET Data Provider Wrapper - IAM Authentication Plugin

## Overview

This plugin provides AWS IAM database authentication support for the AWS Advanced .NET Data Provider Wrapper, enabling applications to authenticate to RDS and Aurora databases using IAM credentials instead of traditional username/password authentication.

## Dependencies

This project depends on:
- **[AWSSDK.RDS](https://www.nuget.org/packages/AWSSDK.RDS/)**: AWS SDK for RDS to generate authentication tokens

## Usage

Register the IAM plugin before using it:

```csharp
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.Iam;

// Register the IAM plugin
ConnectionPluginChainBuilder.RegisterPluginFactory<IamAuthPluginFactory>(PluginCodes.Iam);

// Use in connection string
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "User Id=db-user;" +
                       "Plugins=iam;";
```

## Documentation

For comprehensive information about IAM database authentication and the AWS Advanced .NET Data Provider Wrapper, visit the [Using the IAM Authentication Plugin](../docs/using-the-dotnet-driver/using-plugins/UsingTheIamAuthenticationPlugin.md) guide.
