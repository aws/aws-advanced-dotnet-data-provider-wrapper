# Custom Endpoint Plugin

The Custom Endpoint Plugin adds support for RDS custom endpoints. When the Custom Endpoint Plugin is in use, the driver analyzes custom endpoint information to ensure instances used in connections are part of the custom endpoint being used. This includes connections used in failover and read-write splitting.

Plugin compatibility can be verified in the [List of Available Plugins](../UsingTheDotNetDataProviderDriver.md#list-of-available-plugins) table.

## Plugin Availability

The plugin is available in the `AWS.AdvancedDotnetDataProviderWrapper.Plugin.CustomEndpoint` NuGet package.

## Prerequisites

This plugin requires the following NuGet package to be installed in your project:

- [AWSSDK.RDS](https://www.nuget.org/packages/AWSSDK.RDS/)

> [!NOTE]\
> The AWSSDK.RDS package has transitive dependencies (such as AWSSDK.Core) that are automatically included when using a package manager.

## How to Use the Custom Endpoint Plugin with the AWS Advanced .NET Data Provider Wrapper

### Enabling the Custom Endpoint Plugin

1. Install the Custom Endpoint plugin package:
   ```bash
   dotnet add package AWS.AdvancedDotnetDataProviderWrapper.Plugin.CustomEndpoint
   ```

2. Register the plugin before establishing connections:
   ```dotnet
   using AwsWrapperDataProvider.Driver.Plugins;
   using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;

   ConnectionPluginChainBuilder.RegisterPluginFactory<CustomEndpointPluginFactory>(PluginCodes.CustomEndpoint);
   ```

3. If needed, create a custom endpoint using the AWS RDS Console:
   - Review the documentation about [creating a custom endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-custom-endpoint-creating.html).

4. Specify parameters that are required or specific to your case.

### Custom Endpoint Plugin Parameters

| Parameter                              |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                 | Default Value         | Example Value |
|----------------------------------------|:-------:|:--------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|---------------|
| `CustomEndpointRegion`                 | String  |    No    | The region of the cluster's custom endpoints. If not specified, the region will be parsed from the URL.                                                                                                                                                                                                                                     | `null`                | `us-west-1`   |
| `CustomEndpointInfoRefreshRateMs`      | Integer |    No    | Controls how frequently custom endpoint monitors fetch custom endpoint info, in milliseconds.                                                                                                                                                                                                                                                | `30000`               | `20000`       |
| `CustomEndpointInfoRefreshRateBackoffFactor` | Integer |    No    | Controls the exponential backoff factor for the custom endpoint monitor. In the event the custom endpoint monitor encounters a throttling exception from the AWS RDS SDK, the refresh time between fetches for custom endpoint info will increase by this factor. When a successful call is made, it will decrease by the same factor. | `2`                   | `5`           |
| `CustomEndpointInfoMaxRefreshRateMs`   | Integer |    No    | Controls the maximum time the custom endpoint monitor will wait in between fetches for custom endpoint info, in milliseconds.                                                                                                                                                                                                                | `300000`              | `600000`      |
| `CustomEndpointMonitorExpirationMs`    | Integer |    No    | Controls how long a monitor should run without use before expiring and being removed, in milliseconds.                                                                                                                                                                                                                                      | `900000` (15 minutes) | `600000`      |
| `WaitForCustomEndpointInfo`           | Boolean |    No    | Controls whether to wait for custom endpoint info to become available before connecting or executing a method. Waiting is only necessary if a connection to a given custom endpoint has not been opened or used recently. Note that disabling this may result in occasional connections to instances outside of the custom endpoint. | `true`                | `true`        |
| `WaitForCustomEndpointInfoTimeoutMs`   | Integer |    No    | Controls the maximum amount of time that the plugin will wait for custom endpoint info to be made available by the custom endpoint monitor, in milliseconds.                                                                                                                                                                                 | `5000`                | `7000`        |