# AWS Advanced .NET Data Provider Wrapper Examples

## Running Examples

To run a specific example, change the `StartupObject` in `AwsWrapperDataProviderExample.csproj`:

```xml
<StartupObject>AwsWrapperDataProviderExample.MySqlFailover</StartupObject>
```

Available examples include `MySqlFailover`, `PGFailover`, `PGReadWriteSplitting`, `LimitlessPostgresql`, and the authentication examples (`PGIamAuthentication`, `MySqlIamAuthentication`, etc.). Use the fully qualified class name as the `StartupObject` value.
