## Custom Dialects

If you are interested in using the AWS Advanced .NET Data Provider Wrapper but your desired database type is not currently supported, it is possible to create a custom dialect.

To create a custom dialect, implement the [`GenericTargetConnectionDialect`](../../AwsWrapperDataProvider/Driver/TargetConnectionDialects/GenericTargetConnectionDialect.cs) class.

Once the custom dialect class has been created, tell the AWS Advanced .NET Data Provider Wrapper to use it by setting the `CustomDialect` attribute in the `DialectProvider` class.

## Configuration Parameters

| Name            | Required             | Description                                                                          | Example                                                                                                                                                                                                    |
|-----------------|----------------------|--------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `CustomDialect` | No (see notes below) | Custom dialect type. Should be AssemblyQualifiedName of class implementing IDialect. | `AwsWrapperDataProvider.Tests.Driver.TargetConnectionDialects.TargetConnectionDialectProviderTests+TestCustomDialect, AwsWrapperDataProvider.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null` |

> [!NOTE]:\
> 
> The `CustomDialect` parameter is not required. When it is not provided by the user, the AWS Advanced .NET Data Provider Wrapper will attempt to determine which of the existing dialects to use based on other connection details.
