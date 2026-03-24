# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

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
