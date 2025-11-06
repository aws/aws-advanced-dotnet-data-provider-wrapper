# Okta Authentication Plugin

The Okta Authentication Plugin adds support for authentication via Federated Identity and then database access via IAM.

## What is Federated Identity
Federated Identity allows users to use the same set of credentials to access multiple services or resources across different organizations. This works by having Identity Providers (IdP) that manage and authenticate user credentials, and Service Providers (SP) that are services or resources that can be internal, external, and/or belonging to various organizations. Multiple SPs can establish trust relationships with a single IdP.

When a user wants access to a resource, it authenticates with the IdP. From this a security token generated and is passed to the SP then grants access to said resource.
In the case of AD FS, the user signs into the ADFS sign in page. This generates a SAML Assertion which acts as a security token. The user then passes the SAML Assertion to the SP when requesting access to resources. The SP verifies the SAML Assertion and grants access to the user.

## Prerequisites
> [!WARNING]\
> To preserve compatibility with customers using the community driver, Okta Authentication requires the AWS SDK for .NET; [AWSSDK.RDS and AWSSDK.SecurityToken](https://aws.amazon.com/developer/language/net/).

To enable the Okta Authentication Connection Plugin, add the plugin code `okta` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) parameter.

In addition, the following line of code must be added before using the AWS Okta Connection Plugin in addition to importing the `AwsWrapperDataProvider.Plugin.Okta` package.

```dotnet
ConnectionPluginChainBuilder.RegisterPluginFactory<OktaAuthPluginFactory>(PluginCodes.Okta);
```

## How to use the Okta Authentication Plugin with the AWS Advanced .NET Data Provider Wrapper 

### Enabling the Okta Authentication Plugin
> [!NOTE]\
> AWS IAM database authentication is needed to use the Okta Authentication Plugin. This is because after the plugin
> acquires SAML assertion from the identity provider, the SAML Assertion is then used to acquire an AWS IAM token. The AWS
> IAM token is then subsequently used to access the database.

1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
   - If needed, review the documentation about [IAM authentication for MariaDB, MySQL, and PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).
2. Configure Okta as the AWS identity provider.
   - If needed, review the documentation about [Amazon Web Services Account Federation](https://help.okta.com/en-us/content/topics/deploymentguides/aws/aws-deployment.htm) on Okta's documentation.
3. Specify parameters that are required or specific to your case.

### Federated Authentication Plugin Parameters
| Parameter                  |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                        | Default Value | Example Value                                          |
|----------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------|
| `DbUser`                   | String  |   Yes    | The user name of the IAM user with access to your database. <br>If you have previously used the IAM Authentication Plugin, this would be the same IAM user. <br>For information on how to connect to your Aurora Database with IAM, see this [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.html). | `None`        | `some_user_name`                                       |
| `IdpUsername`              | String  |   Yes    | The user name for the `idp_endpoint` server. If this parameter is not specified, the plugin will fallback to using the `user` parameter.                                                                                                                                                                                                                           | `null`        | `jimbob@example.com`                                   |
| `IdpPassword`              | String  |   Yes    | The password associated with the `idp_endpoint` username. If this parameter is not specified, the plugin will fallback to using the `password` parameter.                                                                                                                                                                                                          | `null`        | `some_random_password`                                 |
| `IdpEndpoint`              | String  |   Yes    | The hosting URL for the service that you are using to authenticate into AWS Aurora.                                                                                                                                                                                                                                                                                | `null`        | `ec2amaz-ab3cdef.example.com`                          |
| `IdpPort`                  | Integer |   Yes    | The hosting port of Identity Provider.                                                                                                                                                                                                                                                                                                                             | `443`         | `444`                                                  |
| `IamRoleArn`               | String  |   Yes    | The ARN of the IAM Role that is to be assumed to access AWS Aurora.                                                                                                                                                                                                                                                                                                | `null`        | `arn:aws:iam::123456789012:role/adfs_example_iam_role` |
| `IamIdpArn`                | String  |   Yes    | The ARN of the Identity Provider.                                                                                                                                                                                                                                                                                                                                  | `null`        | `arn:aws:iam::123456789012:saml-provider/adfs_example` |
| `IamRegion`                | String  |   Yes    | The IAM region where the IAM token is generated.                                                                                                                                                                                                                                                                                                                   | `null`        | `us-east-2`                                            |
| `IamHost`                  | String  |    No    | Overrides the host that is used to generate the IAM token.                                                                                                                                                                                                                                                                                                         | `null`        | `database.cluster-hash.us-east-1.rds.amazonaws.com`    |
| `IamDefaultPort`           | String  |    No    | This property overrides the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for PostgreSQL and MySQL. Target drivers with different protocols will require users to provide a default port.                                                                 | `null`        | `1234`                                                 |
| `IamExpiration`            | Integer |    No    | Overrides the default IAM token cache expiration in seconds                                                                                                                                                                                                                                                                                                        | `870`         | `123`                                                  |
| `HttpClientConnectTimeout` | Integer |    No    | The timeout value in seconds to send the HTTP request data used by the FederatedAuthPlugin.                                                                                                                                                                                                                                                                        | `60`          | `60`                                                   |
| `SSLInsecure`              | Boolean |    No    | When set to true, disables server certificate verification. This poses significant security risks and should never be used in production environments.                                                                                                                                                                                                             | `false`       | `false`                                                |

## Examples
[PG Okta Authentication](../../examples/PGOktaAuthentication.cs)
[MySql Okta Authentication](../../examples/MySqlOktaAuthentication.cs)
