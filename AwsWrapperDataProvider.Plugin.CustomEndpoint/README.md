# AWS Advanced .NET Data Provider Wrapper - Custom Endpoint Plugin

## Overview

This project provides the Custom Endpoint Plugin for the AWS Advanced .NET Data Provider Wrapper. The plugin adds support for [RDS custom endpoints](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.Endpoints.html#Aurora.Endpoints.Custom), ensuring that instances used in connections are part of the specified custom endpoint. This includes connections used in failover and read-write splitting.

## Dependencies

This project depends on:
- **[AWSSDK.RDS](https://www.nuget.org/packages/AWSSDK.RDS/)**: AWS SDK for Amazon RDS
- **[AWSSDK.SecurityToken](https://www.nuget.org/packages/AWSSDK.SecurityToken/)**: AWS Security Token Service SDK

## Usage

1. Install the plugin package:
   ```bash
   dotnet add package AWS.AdvancedDotnetDataProviderWrapper.Plugin.CustomEndpoint
   ```

2. Register the plugin before establishing connections:
   ```csharp
   using AwsWrapperDataProvider.Driver.Plugins;
   using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;

   ConnectionPluginChainBuilder.RegisterPluginFactory<CustomEndpointPluginFactory>(PluginCodes.CustomEndpoint);
   ```

3. Connect using a custom endpoint URL:
   ```csharp
   var connection = new AwsWrapperConnection(customEndpointConnectionString);
   ```

## Documentation

For detailed configuration parameters and usage instructions, see [Using the Custom Endpoint Plugin](../docs/using-the-dotnet-driver/using-plugins/UsingTheCustomEndpointPlugin.md). For general information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
