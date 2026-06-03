# AWS Advanced .NET Wrapper Data Provider Documentation

## Development Guide
- [Architecture](./development-guide/Architecture.md)
- [Development Guide](./development-guide/DevelopmentGuide.md)
- [Integration Tests](./development-guide/IntegrationTests.md)
- [Pipelines](./development-guide/Pipelines.md)
- [Plugin Manager](./development-guide/PluginManager.md)
- [Plugin Service](./development-guide/PluginService.md)

## Using the .NET Wrapper
- [Custom Dialects](./using-the-dotnet-driver/CustomDialects.md)
- [Failover Configuration Guide](./using-the-dotnet-driver/FailoverConfigurationGuide.md)
- [Reader Selection Strategies](./using-the-dotnet-driver/ReaderSelectionStrategies.md)
- [Support For RDS Multi-Az DB Cluster](./using-the-dotnet-driver/SupportForRDSMultiAzDBCluster.md)
- [Telemetry](./using-the-dotnet-driver/Telemetry.md)
- [Using The .NET Data Provider Driver](./using-the-dotnet-driver/UsingTheDotNetDataProviderDriver.md)
- [Using the Entity Framework Integration](./using-the-dotnet-driver/UsingEntityFrameworkIntegration.md)

## Plugins
- [Aurora Connection Tracker Plugin](./using-the-dotnet-driver/using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)
- [Aurora Initial Connection Strategy Plugin](./using-the-dotnet-driver/using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md)
- [AWS Secrets Manager Plugin](./using-the-dotnet-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)
- [Blue/Green Deployment Plugin](./using-the-dotnet-driver/using-plugins/UsingTheBlueGreenPlugin.md)
- [Custom Endpoint Plugin](./using-the-dotnet-driver/using-plugins/UsingTheCustomEndpointPlugin.md)
- [Failover Plugin](./using-the-dotnet-driver/using-plugins/UsingTheFailoverPlugin.md)
- [Federated Authentication Plugin](./using-the-dotnet-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)
- [Host Monitoring Plugin](./using-the-dotnet-driver/using-plugins/UsingTheHostMonitoringPlugin.md)
- [IAM Authentication Plugin](./using-the-dotnet-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)
- [Limitless Connection Plugin](./using-the-dotnet-driver/using-plugins/UsingTheLimitlessConnectionPlugin.md)
- [Okta Authentication Plugin](./using-the-dotnet-driver/using-plugins/UsingTheOktaAuthenticationPlugin.md)
- [Read/Write Splitting Plugin](./using-the-dotnet-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md)

## Known Limitations

### EFM Plugin Incompatibility with MySQL Drivers
The EFM plugin is designed to proactively terminate database connections when failure conditions are detected, even if a query is currently executing. This requires the ability to abort a connection from a separate thread safely and reliably.
Both drivers lack support for safe, asynchronous connection abortion, the EFM plugin cannot be used with MySQL. This limitation is inherent to the driver implementations and not specific to the plugin itself.

### Amazon Aurora Global Databases Support

This wrapper currently does not support planned failover or switchover of Amazon Aurora Global Databases. Full Support for Amazon Aurora Global Databases is in the backlog, but we cannot comment on a timeline right now.
