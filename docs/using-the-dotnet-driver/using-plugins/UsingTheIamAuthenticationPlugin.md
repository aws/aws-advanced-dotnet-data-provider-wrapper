# AWS IAM Authentication Plugin

## What is IAM?
AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports granular permissions, giving you the ability to grant different permissions to different users. For more information on IAM and its use cases, please refer to the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## Prerequisites
> [!WARNING]\
> To preserve compatibility with customers using the community driver, IAM Authentication requires the AWS SDK for .NET; [.NET SDK](https://aws.amazon.com/developer/language/net/).

The IAM Authentication plugin requires authentication via AWS Credentials. These credentials can be defined in `~/.aws/credentials` or set as environment variables. All users must set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Users who are using temporary security credentials will also need to additionally set `AWS_SESSION_TOKEN`.

Alternatively, you can supply and manage your own AWS credentials (for example an `AssumeRoleAWSCredentials` instance) by registering a custom handler with the `AwsCredentialsManager`. See [AWS Credentials Provider Configuration](../custom-configuration/AwsCredentialsConfiguration.md).

To enable the IAM Authentication Connection Plugin, add the plugin code `iam` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) parameter.

In addition, the following line of code must be added before using the AWS Iam Connection Plugin in addition to importing the `AwsWrapperDataProvider.Plugin.Iam` package.

```dotnet
ConnectionPluginChainBuilder.RegisterPluginFactory<IamAuthPluginFactory>(PluginCodes.Iam);
```

## AWS IAM Database Authentication
The AWS .NET Data Provider Wrapper supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>i.e. `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`

IAM database authentication use is limited to certain database engines. For more information on limitations and recommendations, please [review the IAM documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

## How do I use IAM with the AWS Advanced .NET Data Provider Wrapper?
1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
    1. If needed, review the documentation about [creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
    2. If needed, review the documentation about [modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
2. Set up an [AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication.
3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication. This will be the user specified in the connection string or connection properties.
    1. Connect to your database of choice using primary logins.
        1. For a MySQL database, use the following command to create a new user:<br>
           `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`
        2. For a PostgreSQL database, use the following command to create a new user:<br>
           `CREATE USER db_userx;
           GRANT rds_iam TO db_userx;`
4. Add the plugin code `iam` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) parameter value.

| Parameter        |  Value  | Required | Description                                                                                                                                                                                                                                                                                            | Example Value                                       |
|------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `IamDefaultPort` | String  |    No    | This property will override the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for PostgreSQL and MySQL. Target drivers with different protocols will require users to provide a default port. | `1234`                                              |
| `IamHost`        | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string. This parameter is required when users are connecting with custom endpoints.                                                                       | `database.cluster-hash.us-east-1.rds.amazonaws.com` |
| `IamRegion`      | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string.                                                                                                                                                        | `us-east-2`                                         |
| `IamExpiration`  | Integer |    No    | This property determines how long an IAM token is kept in the driver cache before a new one is generated. The default expiration time is set to 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.                                                 | `600`                                               |

## Connection Pool Stability and Limitations

IAM authentication tokens are short-lived (about 15 minutes) and are regenerated on each rotation. Because the community drivers key their internal connection pool on the connection string, injecting a rotated token directly into the connection string would change the pool key on every rotation, fragmenting the pool (creating new pools, causing cold starts, and increasing connection/memory usage).

To avoid this, the wrapper supplies the rotated token to the target driver through its native password-provider mechanism — keeping the password out of the connection string so the pool key stays stable across rotations:

| Target driver | Password delivery | Pool stable across token rotation? |
|---|---|---|
| `Npgsql.NpgsqlConnection` | `NpgsqlDataSourceBuilder.UsePasswordProvider` | Yes |
| `MySqlConnector.MySqlConnection` | `MySqlConnection.ProvidePasswordCallback` | Yes |
| `MySql.Data.MySqlClient.MySqlConnection` | Injected into the connection string | **No** |

> [!WARNING]\
> **`MySql.Data` (Oracle Connector/NET) does not provide a dynamic password mechanism.** With this driver the IAM token is injected into the connection string, so each token rotation produces a new connection string and a new connection pool. This causes pool fragmentation under IAM token rotation. If you require a stable connection pool with IAM authentication on MySQL, use the `MySqlConnector.MySqlConnection` target driver instead.

## Examples
[PG Iam Authentication](../../examples/AwsWrapperDataProviderExample/PGIamAuthentication.cs)
[MySql Iam Authentication](../../examples/AwsWrapperDataProviderExample/MySqlIamAuthentication.cs)

## Using IAM Authentication with Global Databases

When using IAM authentication with [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/), the IAM user or role requires the additional `rds:DescribeGlobalClusters` permission. This permission allows the driver to resolve the Global Database endpoint to the appropriate regional cluster for IAM token generation.

Example IAM policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds-db:connect",
                "rds:DescribeGlobalClusters"
            ],
            "Resource": "*"
        }
    ]
}
```

> [!NOTE]
> The credentials used by the wrapper to call `DescribeGlobalClusters` are resolved from a custom handler registered with the [`AwsCredentialsManager`](../custom-configuration/AwsCredentialsConfiguration.md) if one is present, and otherwise from the AWS SDK's default credentials chain (environment variables, shared profile, instance profile, etc.) — separately from the database-side IAM user.
