# AWS Secrets Manager Plugin

The AWS Advanced .NET Data Provider Wrapper supports usage of database credentials stored as secrets in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Connection Plugin. When you create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created with the credentials inside that secret.

## Enabling the AWS Secrets Manager Connection Plugin
> [!WARNING]\
> To use this plugin, you must install [AWSSDK.CORE and AWSSDK.SecretsManager](https://aws.amazon.com/developer/language/net/) in your project. These parameters are required for the AWS Advanced .NET Data Provider Wrapper to pass database credentials to the underlying driver.

To enable the AWS Secrets Manager Connection Plugin, add the plugin code `awsSecretsManager` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) value.

In addition, the following line of code must be added before using the AWS Secrets Manager Connection Plugin in addition to importing the `AwsWrapperDataProvider.Plugin.SecretsManager` package.

```dotnet
ConnectionPluginChainBuilder.RegisterPluginFactory<SecretsManagerAuthPluginFactory>(PluginCodes.SecretsManager);
```

This plugin requires valid AWS credentials.

## AWS Secrets Manager Connection Plugin Parameters
The following properties are required for the AWS Secrets Manager Connection Plugin to retrieve database credentials from the AWS Secrets Manager.


| Parameter                     | Value  |                         Required                         | Description                                                                                                                                                                                                                      | Example                 | Default Value |
|-------------------------------|:------:|:--------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------|---------------|
| `SecretsManagerSecretId`      | String |                           Yes                            | Set this value to be the secret name or the secret ARN.                                                                                                                                                                          | `secretId`              | `null`        |
| `SecretsManagerRegion`        | String | Yes unless the `SecretsManagerSecretId` is a Secret ARN. | Set this value to be the region your secret is in.                                                                                                                                                                               | `us-east-2`             | `us-east-1`   |
| `SecretsManagerEndpoint`      | String |                            No                            | Set this value to be the endpoint override to retrieve your secret from. This parameter value should be in the form of a URL, with a valid protocol (ex. `http://`) and domain (ex. `localhost`). A port number is not required. | `http://localhost:1234` | `null`        |
| `SecretsManagerExpirationSec` |  int   |                            No                            | Set this value to be the expiration time in seconds the secret is stored in the cache. The value must be above 0 or will throw an exception.                                                                                     | 500                     | 870           |

[!NOTE] A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`

## Secret Data
The plugin assumes that the secret contains the following properties `Username` and `Password`

### Example

The following code snippet shows how you can establish a PostgreSQL connection with the AWS Secrets Manager Plugin.

Note that the `SecretsManagerRegion` is not a required parameter. If it is not provided, the default region `us-east-1` or the parsed region from the `SecretsManagerSecretId` will be used.

```dotnet
AwsWrapperConnection<NpgsqlConnection> connection = new(
        "Host=database.cluster-xyz.us-east-1.rds.amazonaws.com;
        Database=postgres;
        SecretsManagerSecretId=secret_name;
        SecretsManagerRegion=us-east-2;
        Plugins=awsSecretsManager"
);
```

If you specify a secret ARN as the `SecretsManagerSecretId`, the AWS Advanced .NET Data Provider Wrapper will parse the region from the ARN and set it as the `SecretsManagerRegion` value.
```dotnet
AwsWrapperConnection<NpgsqlConnection> connection = new(
        "Host=database.cluster-xyz.us-east-1.rds.amazonaws.com;
        Database=postgres;
        SecretsManagerSecretId=arn:aws:secretsmanager:us-east-2:<AccountId>:secret:Secre78tName-6RandomCharacters;
        Plugins=awsSecretsManager"
);
```

For more examples: 
[PG Secret Manager Authentication](../../examples/PGIamAuthentication.cs)
[MySql Secret Manager Authentication](../../examples/MySqlIamAuthentication.cs)
