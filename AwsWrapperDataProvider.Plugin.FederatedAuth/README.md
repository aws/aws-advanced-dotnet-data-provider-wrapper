# AWS Advanced .NET Data Provider Wrapper - Federated Authentication Plugin

## Overview

This plugin provides AWS federated authentication support for the AWS Advanced .NET Data Provider Wrapper, enabling applications to authenticate to RDS and Aurora databases using federated identity providers through AWS Security Token Service (STS).

## Dependencies

This project depends on:
- **[AWSSDK.RDS](https://www.nuget.org/packages/AWSSDK.RDS/)**: AWS SDK for RDS to generate authentication tokens
- **[AWSSDK.SecurityToken](https://www.nuget.org/packages/AWSSDK.SecurityToken/)**: AWS SDK for STS to handle federated authentication

## Usage

Register the Federated Authentication plugin before using it:

```csharp
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.FederatedAuth;

// Register the Federated Auth plugin
ConnectionPluginChainBuilder.RegisterPluginFactory<FederatedAuthPluginFactory>(PluginCodes.FederatedAuth);

// Use in connection string
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "User Id=db-user;" +
                       "Plugins=federatedAuth;";
```

## Documentation

For comprehensive information about federated authentication and the AWS Advanced .NET Data Provider Wrapper, visit the [Using the Federated Authentication Plugin](../docs/using-the-dotnet-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md) guide.
