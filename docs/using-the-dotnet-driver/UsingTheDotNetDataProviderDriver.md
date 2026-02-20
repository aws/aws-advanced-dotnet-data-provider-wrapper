# Using the AWS Advanced .NET Data Provider Wrapper with AWS Aurora

The AWS Advanced .NET Data Provider Wrapper leverages community .NET data providers and enables support of AWS and Aurora functionalities.

## Using the AWS Advanced .NET Data Provider Wrapper with RDS Multi-AZ database Clusters
The [AWS RDS Multi-AZ DB Clusters](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) are capable of switching over the current writer host to another host in the cluster within approximately 1 second or less, in case of minor engine version upgrade or OS maintenance operations.
The AWS Advanced .NET Data Provider Wrapper has been optimized for such fast-failover when working with AWS RDS Multi-AZ DB Clusters.

With the Failover plugin, the downtime during certain DB cluster operations, such as engine minor version upgrades, can be reduced to one second or even less with finely tuned parameters. It supports both MySQL and PostgreSQL clusters.

Visit [this page](./SupportForRDSMultiAzDBCluster.md) for more details.

## Using the AWS Advanced .NET Data Provider Wrapper with plain RDS databases

It is possible to use the AWS Advanced .NET Data Provider Wrapper with plain RDS databases, but individual features may or may not be compatible. For example, failover handling and enhanced failure monitoring are not compatible with plain RDS databases and the relevant plugins must be disabled. Plugins can be enabled or disabled as seen in the [Connection Plugin Manager Parameters](#connection-plugin-manager-parameters) section. Please note that some plugins have been enabled by default. Plugin compatibility can be verified in the [plugins table](#list-of-available-plugins).

## AWS Advanced .NET Data Provider Wrapper Parameters

These parameters are applicable to any instance of the AWS Advanced .NET Data Provider Wrapper.

| Parameter                    | Description                                                                                                                                                                                                                                                                                                                                                                                | Required | Default Value |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| ClusterTopologyRefreshRateMs | Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection.                                                                                                                                                                           | False    | 30000         |
| ClusterId                    | A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. If unspecified, a cluster id is automatically created for AWS RDS clusters.                                                                                                                                                                                                      | False    | None          |
| ClusterInstanceHostPattern   | The cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for cluster instance names. This pattern is required to be specified for IP address or custom domain connections to AWS RDS clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters. | False    | `null`        |
| CommandTimeout               | 	The time in seconds to wait for a command to execute before timing out. A value of 0 indicates an infinite timeout (command will wait indefinitely). This setting applies to all database commands executed through the connection.                                                                                                                                                       | False    | 30            |


## Plugins

The AWS Advanced .NET Data Provider Wrapper uses plugins to execute database API calls. You can think of a plugin as an extensible code module that adds extra logic around any database API calls. The AWS Advanced .NET Data Provider Wrapper has a number of [built-in plugins](#list-of-available-plugins) available for use. 

Plugins are loaded and managed through the Connection Plugin Manager and may be identified by a `str` name in the form of plugin code.

### Connection Plugin Manager Parameters

| Parameter             | Value | Required | Description                                                                                                                                                                   | Default Value                          |
|-----------------------|-------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| `Plugins`             | `str` | No       | Comma separated list of connection plugin codes. <br><br>Example: `failover,efm`                                                                                              | `initialConnection,failover,efm` | 
| `AutoSortPluginOrder` | `str` | No       | Certain plugins require a specific plugin chain ordering to function correctly. When enabled, this property automatically sorts the requested plugins into the correct order. | `True`                                 |

### List of Available Plugins

The AWS Advanced .NET Data Provider Wrapper has several built-in plugins that are available to use. Please visit the individual plugin page for more details.

| Plugin name                                                                                            | Plugin Code         | Database Compatibility | Description                                                                                                                                                                                                  | Additional Required Dependencies                                                        |
|--------------------------------------------------------------------------------------------------------|---------------------|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)                                | `failover`          | Aurora                 | Enables the failover functionality supported by Amazon Aurora clusters. Prevents opening a wrong connection to an old writer host dues to stale DNS after failover event. This plugin is enabled by default. | None                                                                                    |                             
| [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)                   | `efm`               | Aurora                 | Enables enhanced host connection failure monitoring, allowing faster failure detection rates. This plugin is enabled by default.                                                                             | None                                                                                    |
| [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)             | `iam`               | Any database           | Enables users to connect to their Amazon Aurora clusters using AWS Identity and Access Management (IAM).                                                                                                     | [AWSSDK.CORE and AWSSDK.RDS](https://aws.amazon.com/developer/language/net/)            |
| [AWS Secrets Manager Connection Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md)            | `awsSecretsManager` | Any database           | Enables fetching database credentials from the AWS Secrets Manager service.                                                                                                                                  | [AWSSDK.CORE and AWSSDK.SecretsManager](https://aws.amazon.com/developer/language/net/) |
| [Federated Authentication Connection Plugin](./using-plugins/UsingTheFederatedAuthenticationPlugin.md) | `federatedAuth`     | Any database           | Enables users to authenticate via Federated Identity and then database access via IAM.                                                                                                                       | [AWSSDK.CORE and AWSSDK.SecurityToken](https://aws.amazon.com/developer/language/net/)  |
| [Okta Authentication Plugin](./using-plugins/UsingTheOktaAuthenticationPlugin.md)                      | `okta`              | Aurora, RDS            | Enables users to authenticate using Federated Identity and then connect to their Amazon Aurora Cluster using AWS Identity and Access Management (IAM).                                                       | [AWSSDK.CORE and AWSSDK.SecurityToken](https://aws.amazon.com/developer/language/net/)  |
| [Aurora Initial Connection Strategy](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | `initialConnection` | Aurora                 | Allows users to configure their initial connection strategy to reader cluster endpoints.                                                                                                                     | None                                                                                    |
| [Read/Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)                     | `readWriteSplitting`| Aurora                 | Switches between writer and reader instances based on session transaction mode (read-only vs read-write), so read traffic can be directed to readers.                                                        | None                                                                                    |
| Execution Time Connection Plugin                                                                       | `executionTime`     | Any database           | Logs the time taken to execute any .NET method.                                                                                                                                                              | None                                                                                    |

### AWS Authentication Dependent Plugins

AWS Authentication dependent plugins such as IAM Authentication, AWS Secrets Manager, Federated Authentication, and Okta Authentication require specific AWS SDK packages to be installed in your project. For example, the IAM Authentication plugin requires both AWSSDK.CORE and AWSSDK.RDS packages, while the AWS Secrets Manager plugin needs AWSSDK.CORE and AWSSDK.SecretsManager. Similarly, both Federated Authentication and Okta Authentication plugins depend on AWSSDK.CORE and AWSSDK.SecurityToken. These plugins must be explicitly enabled in your connection string or configuration by specifying their respective plugin codes (such as 'iam', 'awsSecretsManager', 'federatedAuth', or 'okta'), and the corresponding AWS SDK packages must be properly installed and referenced in your project before the plugins can be utilized. Unlike the default plugins such as failover and enhanced failure monitoring, these AWS-dependent plugins are not enabled by default and require manual configuration.

The following code is required to import the appropriate plugin desired.

```dotnet
ConnectionPluginChainBuilder.RegisterPluginFactory<OktaAuthPluginFactory>(PluginCodes.Okta);
ConnectionPluginChainBuilder.RegisterPluginFactory<FederatedAuthPluginFactory>(PluginCodes.FederatedAuth);
ConnectionPluginChainBuilder.RegisterPluginFactory<IamAuthPluginFactory>(PluginCodes.Iam);
ConnectionPluginChainBuilder.RegisterPluginFactory<SecretsManagerAuthPluginFactory>(PluginCodes.SecretsManager);
```

### Connection Dialects

Currently, [Npgsql](https://www.nuget.org/packages/Npgsql/) 9.0.3+, [MySql.Data](https://www.nuget.org/packages/mysql.data/) 9.4.0+ and [MySqlConnector](https://www.nuget.org/packages/MySqlConnector/) 2.4.0+ are supported. Compatibility with prior versions of these drivers has not been tested.
In order to use the Connection Dialects, the appropriate connection dialect must be imported according to the following code:

```dotnet
MySqlClientDialectLoader.Load();
MySqlConnectorDialectLoader.Load();
NpgsqlDialectLoader.Load();
```
# Logging

The AWS Wrapper .NET Data Provider Wrapper uses Microsoft.Extensions.Logging for comprehensive logging across all components. 

Logging output to a file `aws-dotnet-data-provider-wrapper-log.log` can be enabled and configured as shown below:

| Environment Variable | Description                                             | Default Value |
|----------------------|---------------------------------------------------------|---------------|
| `ENABLED_FILE_LOG`   | Set to `true` to enable file logging otherwise `false`. | `disabled`    |
| `LOG_LEVEL`          | Minimum log level for a message to be logged.           | `"trace"`     |
| `LOG_DIRECTORY_PATH` | Directory path for log files.                           | `"./"`        |

A custom logger provider through the `ILoggerProvider` interface can be provided which will be used for logging throughout the wrapper. This can be utilized for custom output logging behavior or by implementing third-party logging providers such as `SerilogLoggerProvider`, `NLog.Extensions.Logging`, or `log4net.Provider`.

The following example code displays how to use the `SerilogLoggerProvider` before wrapper usage in order to utilize the custom logger provider.
```dotnet
LoggerUtils.SetCustomLoggerProvider(new SerilogLoggerProvider(Log.Logger));
```