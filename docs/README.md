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
- [Using The .NET Data Provider Driver](./using-the-dotnet-driver/UsingTheDotNetDataProviderDriver.md)
- [Using the Entity Framework Integration](./using-the-dotnet-driver/UsingEntityFrameworkIntegration.md)

## Plugins
- [Aurora Initial Connection Strategy Plugin](./using-the-dotnet-driver/using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md)
- [AWS Secrets Manager Plugin](./using-the-dotnet-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)
- [Failover Plugin](./using-the-dotnet-driver/using-plugins/UsingTheFailoverPlugin.md)
- [Federated Authentication Plugin](./using-the-dotnet-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)
- [Host Monitoring Plugin](./using-the-dotnet-driver/using-plugins/UsingTheHostMonitoringPlugin.md)
- [IAM Authentication Plugin](./using-the-dotnet-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)
- [Okta Authentication Plugin](./using-the-dotnet-driver/using-plugins/UsingTheOktaAuthenticationPlugin.md)

## Known Limitations

### PostgreSQL Entity Framework Core Support

Entity Framework Core is currently not supported for PostgreSQL when using the AWS Advanced .NET Data Provider Wrapper. This limitation is due to an open issue in the Npgsql EF Core provider (npgsql/efcore.pg#1922), which prevents proper compatibility with custom DbConnection implementations. We are actively collaborating with the Npgsql open-source project to help resolve this limitation; however, there is no confirmed timeline for full support at this time. In the meantime, we recommend using NHibernate or ADO.NET directly for PostgreSQL integrations.

### EFM Plugin Incompatibility with MySQL Drivers
The EFM plugin is designed to proactively terminate database connections when failure conditions are detected, even if a query is currently executing. This requires the ability to abort a connection from a separate thread safely and reliably.
Both drivers lack support for safe, asynchronous connection abortion, the EFM plugin cannot be used with MySQL. This limitation is inherent to the driver implementations and not specific to the plugin itself.

### Amazon Blue/Green Deployment Support

This wrapper currently does not support Amazon Blue/Green Deployment. Full Support for Amazon Blue/Green Deployment is in the backlog, but we cannot comment on a timeline right now.

### Amazon Aurora Limitless Support

This wrapper currently does not support Amazon Aurora Limitless Databases. Full Support for Amazon Aurora Limitless Databases is in the backlog, but we cannot comment on a timeline right now.

### Amazon Aurora Global Databases Support

This wrapper currently does not support planned failover or switchover of Amazon Aurora Global Databases. Full Support for Amazon Aurora Global Databases is in the backlog, but we cannot comment on a timeline right now.
