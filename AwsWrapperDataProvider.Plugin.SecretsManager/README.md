# AWS Advanced .NET Data Provider Wrapper - Secrets Manager Plugin

## Overview

This plugin provides AWS Secrets Manager integration for the AWS Advanced .NET Data Provider Wrapper, enabling applications to retrieve database credentials securely from AWS Secrets Manager instead of hardcoding them in connection strings.

## Dependencies

This project depends on:
- **[AWSSDK.SecretsManager](https://www.nuget.org/packages/AWSSDK.SecretsManager/)**: AWS SDK for Secrets Manager to retrieve secrets

## Usage

Register the Secrets Manager plugin before using it:

```csharp
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.SecretsManager;

// Register the Secrets Manager plugin
ConnectionPluginChainBuilder.RegisterPluginFactory<SecretsManagerAuthPluginFactory>(PluginCodes.SecretsManager);

// Use in connection string
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "SecretArn=arn:aws:secretsmanager:region:account:secret:secret-name;" +
                       "Plugins=secretsManager;";
```

## Documentation

For comprehensive information about AWS Secrets Manager integration and the AWS Advanced .NET Data Provider Wrapper, visit the [Using the AWS Secrets Manager Plugin](../docs/using-the-dotnet-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md) guide.
