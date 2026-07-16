# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [2.1.0] - 2026-07-16

### :magic_wand: Added
- The `AwsCredentialsManager` in the new [AWS.AdvancedDotnetDataProviderWrapper.Authentication](./AwsWrapperDataProvider.Authentication/README.md) package, which lets applications supply and manage their own AWS credentials (e.g. an `AssumeRoleAWSCredentials`) for the AWS API calls made by the IAM Authentication, AWS Secrets Manager Authentication, and Custom Endpoint plugins. When no handler is registered (or the handler returns `null`), the AWS SDK default credentials chain is used as before. See the [AWS Credentials Provider Configuration documentation](./docs/using-the-dotnet-driver/custom-configuration/AwsCredentialsConfiguration.md) ([PR #320](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/320)).
- Connection pool stability under credential rotation for the token-based authentication plugins (IAM, AWS Secrets Manager, and federated ADFS/Okta): rotating tokens and passwords are now supplied to the target driver through its native password-provider mechanism (`NpgsqlDataSourceBuilder.UsePasswordProvider` for Npgsql, `MySqlConnection.ProvidePasswordCallback` for MySqlConnector) instead of the connection string, so rotation no longer fragments the driver's connection pool. `MySql.Data` does not offer a dynamic password mechanism and still uses connection-string injection. See the [IAM Authentication](./docs/using-the-dotnet-driver/using-plugins/UsingTheIamAuthenticationPlugin.md#connection-pool-stability-and-limitations) and [AWS Secrets Manager](./docs/using-the-dotnet-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md#connection-pool-stability-and-limitations) plugin documentation for details and limitations ([PR #314](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/314)).

### :bug: Fixed
- Reader failover when connecting through a custom endpoint: the original writer was resolved from the host list before topology discovery, which is empty for custom endpoints until the topology monitor populates it; it is now captured from the freshly refreshed topology inside the failover loop ([PR #315](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/315)).
- Blue/Green Deployment Plugin: green host names are now matched directly instead of through the shared regex match cache (preventing corrupted DNS pattern lookups for the same host), connection failures to substitute hosts during switchover are surfaced to the caller instead of being silently retried, and the Blue/Green status monitor no longer faults its background task on unexpected connection errors ([PR #299](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/299)).
- Read/Write Splitting Plugin: reader connection attempts now rethrow login/authentication failures immediately, readers that already failed in the same call are not retried, and transient topology-probe failures no longer mark hosts globally unavailable ([PR #322](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/322)).

## [2.0.0] - 2026-06-16

### :crab: Breaking Changes
> [!WARNING]\
> 2.0 removes the suggested ClusterId functionality ([PR #290](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/290)).
> #### Suggested ClusterId Functionality
> Prior to this change, the wrapper would generate a unique cluster id based on the connection string and the cluster topology; however, in some cases (such as custom endpoints, IP addresses, and CNAME aliases), the wrapper would generate an incorrect identifier. This change was needed to prevent applications with several clusters from accidentally relying on incorrect topology during failover, which could result in the wrapper failing to complete failover successfully. The `ClusterId` parameter now defaults to `1`, so connections that do not specify a `ClusterId` share a single topology cache.
> #### Migration
> | Number of Database Clusters in Use | Requires Changes | Action Items                                                                                                                                                                                              |
> |------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
> | Single database cluster            | No               | No changes required.                                                                                                                                                                                       |
> | Multiple database clusters         | Yes              | Review all connection strings and add the mandatory `ClusterId` parameter. See the [ClusterId documentation](./docs/using-the-dotnet-driver/ClusterId.md) for configuration guidance.                      |

> [!WARNING]\
> This breaking change only impacts customers implementing their own custom plugins. Otherwise no changes are required. 2.0 removes aliases from `HostSpec` and related APIs ([PR #302](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/302)).
> #### Alias Removal
> - Removed aliases from `HostSpec`.
> - Removed `IPluginService.FillAliasesAsync()`; use `IPluginService.IdentifyConnectionAsync()` instead.
> - Changed `IPluginService.SetAvailability()` signature to accept a `HostSpec` instead of a collection of host aliases.
> - Added `IPluginService.RoutedHostSpec` to record connection routing events and use it for the current connection info.
>
> If you have custom plugins that use `FillAliasesAsync()` or rely on `HostSpec` aliases, update them to use `IdentifyConnectionAsync()` and the new `RoutedHostSpec` property.

### :magic_wand: Added
- Support for [Amazon Aurora Global Databases](./docs/using-the-dotnet-driver/GlobalDatabases.md), including multi-region topology awareness and global writer cluster endpoint recognition ([PR #290](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/290)).
- The [Global Database Failover Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheGdbFailoverPlugin.md) (`gdbFailover`), introducing home-region awareness and configurable failover logic for in-home and out-of-home scenarios ([PR #292](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/292)).
- The [Global Database Read/Write Splitting Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) (`gdbReadWriteSplitting`), introducing home-region restrictions and optional global write forwarding for read/write splitting connections ([PR #293](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/293)).
- IAM and federated (ADFS/Okta) authentication support for Aurora Global Database endpoints, resolving the region via the `DescribeGlobalClusters` RDS API ([PR #297](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/297)).
- Telemetry for the Global Database and Blue/Green plugins ([PR #303](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/303)).
- Automatically trigger writer failover when connections are incorrectly established as read-only connections ([PR #306](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/306)).
- Documentation:
  - [ClusterId](./docs/using-the-dotnet-driver/ClusterId.md), [Aurora Global Databases](./docs/using-the-dotnet-driver/GlobalDatabases.md), and the GDB plugins ([PR #300](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/300)).
  - [Database Dialects](./docs/using-the-dotnet-driver/DatabaseDialects.md) and [Target Connection Dialects](./docs/using-the-dotnet-driver/TargetConnectionDialects.md) ([PR #304](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/304)).

## [1.2.0] - 2026-06-02

### :magic_wand: Added
- The [Blue/Green Deployment Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheBlueGreenPlugin.md) ([PR #244](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/244)).
- The [Aurora Connection Tracker Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) ([PR #250](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/250)).
- [Telemetry](./docs/using-the-dotnet-driver/Telemetry.md) support for traces and metrics, with OTLP and AWS X-Ray backends ([PR #291](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/291)).
- [Entity Framework Core support for PostgreSQL](./AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL/README.md) ([PR #280](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/280)).

### :warning: Deprecated
- The MySQL Entity Framework Core extension method `UseAwsWrapper` has been renamed to `UseAwsWrapperMySql`. The original name is still available but is marked `[Obsolete]` and will be removed in a future major version. Update existing call sites to `UseAwsWrapperMySql` to silence the deprecation warning ([PR #280](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/280)).

### :bug: Fixed
- Entity Framework Core + Pomelo + MySqlConnector connection string handling: wrapper-only properties (e.g. `Plugins=`) are now filtered case-insensitively before being passed to the target driver, and required Pomelo options (`AllowUserVariables=true`, `UseAffectedRows=false`) are enforced on the wrapper connection string ([Issue #268](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/issues/268), [PR #272](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/272)).

## [1.1.0] - 2026-03-24

### :magic_wand: Added
- The [Custom Endpoint Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheCustomEndpointPlugin.md) ([PR #230](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/230)).
- The [Read/Write Splitting Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md) ([Issue #179](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/issues/179), [PR #210](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/210)).
- The [Limitless Connection Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheLimitlessConnectionPlugin.md) for AWS RDS Limitless router balancing ([Issue #185](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/issues/185), [PR #208](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/208)).
- Support for custom endpoint and custom secret data formats in the [AWS Secrets Manager Authentication Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md) ([Issue #193](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/issues/193), [PR #209](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/209)).
- IWrapper interface to expose the underlying wrapped connection instance ([Issue #234](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/issues/234), [PR #238](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/238)).
- Support for [custom logger providers](./docs/using-the-dotnet-driver/UsingTheDotNetDataProviderDriver.md#logging) ([PR #165](https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/pull/165)).

## [1.0.1] - 2025-11-07

### :bug: Fixed
- AWS.AdvancedDotnetDataProviderWrapper.Dialect.MySqlClient depends on AWS.AdvancedDotnetDataProviderWrapper.Dialect.Npgsql

## [1.0.0] - 2025-11-06
The AWS Advanced .NET Data Provider Wrapper is complementary to existing .NET database drivers and aims to extend their functionality to enable applications to take full advantage of the features of clustered databases such as Amazon Aurora. The AWS Advanced .NET Data Provider Wrapper does not connect directly to any database, but enables support of AWS and Aurora functionalities on top of an underlying .NET driver of the user's choice.

### :magic_wand: Added
- Support for PostgreSQL and MySql
- Support for Entity Framework on [MySQL](./docs/using-the-dotnet-driver/UsingEntityFrameworkIntegration.md)
- Support for [NHibernate](./AwsWrapperDataProvider.NHibernate/README.md)
- The [Aurora Initial Connection Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md)
- The [Failover Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheFailoverPlugin.md)
- The [Host Monitoring Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheHostMonitoringPlugin.md)
- The [AWS IAM Authentication Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)
- The [AWS Secrets Manager Authentication Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)
- The [AWS Okta Authentication Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)
- The [AWS Federated Authentication Plugin](./docs/using-the-dotnet-driver/using-plugins/UsingTheOktaAuthenticationPlugin.md)


[1.0.0]: https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/releases/tag/1.0.0
[1.0.1]: https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/compare/1.0.0...1.0.1
[1.1.0]: https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/compare/1.0.1...1.1.0
[1.2.0]: https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/compare/1.1.0...1.2.0
[2.0.0]: https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/compare/1.2.0...2.0.0
[2.1.0]: https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper/compare/2.0.0...2.1.0
