# AWS Credentials Provider Configuration

### Applicable plugins: IamAuthenticationPlugin, SecretsManagerAuthPlugin, CustomEndpointPlugin

The `IamAuthenticationPlugin`, `SecretsManagerAuthPlugin`, and `CustomEndpointPlugin` all require authentication via AWS credentials to provide the functionality they offer. By default, these plugins locate your credentials using the [AWS SDK for .NET default credentials chain](https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/creds-assign.html) — for example credentials defined in `~/.aws/credentials`, or the `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` environment variables.

If you would like to supply and manage your own credentials — for example an [`AssumeRoleAWSCredentials`](https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/Runtime/TAssumeRoleAWSCredentials.html) instance whose refresh behavior you control — you can do so using the `AwsCredentialsManager` class. Call `AwsCredentialsManager.SetCustomHandler`, passing in a lambda (or an `AwsCredentialsProviderHandler` delegate) that returns an `Amazon.Runtime.AWSCredentials`:

```csharp
using Amazon.Runtime;
using AwsWrapperDataProvider.Authentication;

// Register once at application startup. The handler receives the target endpoint (HostSpec) and the
// connection properties, so a single handler can return different credentials per endpoint.
AwsCredentialsManager.SetCustomHandler((hostSpec, props) =>
    new AssumeRoleAWSCredentials(
        FallbackCredentialsFactory.GetCredentials(),
        "arn:aws:iam::123456789012:role/MyRole",
        "my-session"));
```

Because a .NET `AWSCredentials` is itself the refreshable unit (`AssumeRoleAWSCredentials`, for instance, refreshes internally when the SDK resolves it), the handler only needs to return the `AWSCredentials` instance; you do not need to manage refresh yourself.

Return `null` from the handler to fall back to the AWS SDK default credentials chain. Call `AwsCredentialsManager.ResetCustomHandler()` to remove a previously registered handler. The registration is process-global, so the most recently registered handler is used.

## Sample code
[AwsCredentialsManagerExample.cs](../../examples/AwsWrapperDataProviderExample/AwsCredentialsManager.cs)
