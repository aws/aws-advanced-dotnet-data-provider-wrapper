# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

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
