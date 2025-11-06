# Federated Authentication Plugin

The Federated Authentication Plugin adds support for authentication via Federated Identity and then database access via IAM. 
Currently, only Microsoft Active Directory Federation Services (AD FS) is supported.

## What is Federated Identity
Federated Identity allows users to use the same set of credentials to access multiple services or resources across different organizations. This works by having Identity Providers (IdP) that manage and authenticate user credentials, and Service Providers (SP) that are services or resources that can be internal, external, and/or belonging to various organizations. Multiple Service Providers can establish trust relationships with a single IdP.

When a user wants access to a resource, it authenticates with the IdP. From this a security token generated and is passed to the SP then grants access to said resource.
In the case of AD FS, the user signs into the AD FS sign in page. This generates a SAML Assertion which acts as a security token. The user then passes the SAML Assertion to the SP when requesting access to resources. The SP verifies the SAML Assertion and grants access to the user. 

## Prerequisites
> [!WARNING]\
> To preserve compatibility with customers using the community driver, Federated Authentication requires the AWS SDK for .NET; [AWSSDK.RDS and AWSSDK.SecurityToken](https://aws.amazon.com/developer/language/net/).

To enable the IAM Authentication Connection Plugin, add the plugin code `federatedAuth` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) parameter.

In addition, the following line of code must be added before using the AWS Federated Authentication Plugin in addition to importing the `AwsWrapperDataProvider.Plugin.FederatedAuth` package.

```dotnet
ConnectionPluginChainBuilder.RegisterPluginFactory<FederatedAuthPluginFactory>(PluginCodes.FederatedAuth);
```

## How to use the Federated Authentication Plugin with the AWS Advanced .NET Data Provider Wrapper 

### Enabling the Federated Authentication Plugin
> [!NOTE]\
> AWS IAM database authentication is needed to use the Federated Authentication Plugin.
> This is because after the plugin acquires SAML assertion from the identity provider, the SAML Assertion is then used to acquire an AWS IAM token.
> The AWS IAM token is then subsequently used to access the database.

1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
   - If needed, review the documentation about [IAM authentication for MySQL, and PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).
2. Set up an IAM Identity Provider and IAM role. The IAM role should be using the IAM policy set up in step 1. 
   - If needed, review the documentation about [creating IAM identity providers](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create.html). For AD FS, see the documentation about [creating IAM SAML identity providers](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_saml.html).
3. Specify parameters that are required or specific to your case.

### Federated Authentication Plugin Parameters
| Parameter                  |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                        | Default Value            | Example Value                                          |
|----------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|--------------------------------------------------------|
| `DbUser`                   | String  |   Yes    | The user name of the IAM user with access to your database. <br>If you have previously used the IAM Authentication Plugin, this would be the same IAM user. <br>For information on how to connect to your Aurora Database with IAM, see this [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.html). | `null`                   | `some_user_name`                                       |
| `IdpUsername`              | String  |   Yes    | The user name for the `idp_endpoint` server. If this parameter is not specified, the plugin will fallback to using the `user` parameter.                                                                                                                                                                                                                           | `null`                   | `jimbob@example.com`                                   |
| `IdpPassword`              | String  |   Yes    | The password associated with the `idp_endpoint` username. If this parameter is not specified, the plugin will fallback to using the `password` parameter.                                                                                                                                                                                                          | `null`                   | `some_random_password`                                 |
| `IdpEndpoint`              | String  |   Yes    | The hosting URL for the service that you are using to authenticate into AWS Aurora.                                                                                                                                                                                                                                                                                | `null`                   | `ec2amaz-ab3cdef.example.com`                          |
| `IdpPort`                  | Integer |   Yes    | The hosting port of Identity Provider.                                                                                                                                                                                                                                                                                                                             | `443`                    | `444`                                                  |
| `IamRoleArn`               | String  |   Yes    | The ARN of the IAM Role that is to be assumed to access AWS Aurora.                                                                                                                                                                                                                                                                                                | `null`                   | `arn:aws:iam::123456789012:role/adfs_example_iam_role` |
| `IamIdpArn`                | String  |   Yes    | The ARN of the Identity Provider.                                                                                                                                                                                                                                                                                                                                  | `null`                   | `arn:aws:iam::123456789012:saml-provider/adfs_example` |
| `IamRegion`                | String  |   Yes    | The IAM region where the IAM token is generated.                                                                                                                                                                                                                                                                                                                   | `null`                   | `us-east-2`                                            |
| `RpIdentifier`             | String  |    No    | The relaying party identifier.                                                                                                                                                                                                                                                                                                                                     | `urn:amazon:webservices` | `urn:amazon:webservices`                               |
| `IamHost`                  | String  |    No    | Overrides the host that is used to generate the IAM token.                                                                                                                                                                                                                                                                                                         | `null`                   | `database.cluster-hash.us-east-1.rds.amazonaws.com`    |
| `IamDefaultPort`           | String  |    No    | This property overrides the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for PostgreSQL and MySQL. Target drivers with different protocols will require users to provide a default port.                                                                 | `null`                   | `1234`                                                 |
| `IamExpiration`            | Integer |    No    | Overrides the default IAM token cache expiration in seconds                                                                                                                                                                                                                                                                                                        | `870`                    | `123`                                                  |
| `HttpClientConnectTimeout` | Integer |    No    | The timeout value in seconds to send the HTTP request data used by the FederatedAuthPlugin.                                                                                                                                                                                                                                                                        | `60`                     | `60`                                                   |
| `SSLInsecure`              | Boolean |    No    | When set to true, disables server certificate verification. This poses significant security risks and should never be used in production environments.                                                                                                                                                                                                             | `false`                  | `false`                                                |

## Examples
[PG Federated Authentication](../../examples/PGFederatedAuthentication.cs)
[MySql Federated Authentication](../../examples/MySqlFederatedAuthentication.cs)
