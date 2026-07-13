# AWS Advanced .NET Data Provider Wrapper - Authentication

This package provides the shared AWS credential-selection infrastructure used by the AWS-authenticating
plugins of the AWS Advanced .NET Data Provider Wrapper (IAM authentication, AWS Secrets Manager, and
custom endpoint monitoring).

## AwsCredentialsManager

By default, the AWS-authenticating plugins locate AWS credentials using the AWS SDK's default
credentials chain. If you would like to supply and manage your own credentials — for example an
`AssumeRoleAWSCredentials` instance whose refresh behavior you control — you can register a custom
handler with the `AwsCredentialsManager`:

```csharp
using Amazon.Runtime;
using Amazon.SecurityToken;
using AwsWrapperDataProvider.Authentication;

// Register once at application startup. The handler receives the target endpoint and the connection
// properties, so a single handler can return different credentials per endpoint.
AwsCredentialsManager.SetCustomHandler((hostSpec, props) =>
    new AssumeRoleAWSCredentials(
        FallbackCredentialsFactory.GetCredentials(),
        "arn:aws:iam::123456789012:role/MyRole",
        "my-session"));
```

Return `null` from the handler to fall back to the AWS SDK default credentials chain. Call
`AwsCredentialsManager.ResetCustomHandler()` to remove a previously registered handler.

The registration is process-global, so the most recently registered handler is used.

See the [AWS Credentials Configuration documentation](../docs/using-the-dotnet-driver/custom-configuration/AwsCredentialsConfiguration.md)
for more details.
