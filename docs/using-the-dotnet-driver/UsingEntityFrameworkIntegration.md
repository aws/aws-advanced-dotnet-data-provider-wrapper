# Using the AWS Advanced .NET Data Provider Wrapper with Entity Framework

The AWS Advanced .NET Data Provider Wrapper can be seamlessly integrated with Entity Framework (EF) Core to provide enhanced AWS and Aurora functionalities while maintaining the familiar Entity Framework development experience.

## Database Provider Compatibility

The AWS Advanced .NET Data Provider Wrapper works with Entity Framework Core through the underlying supported database providers:

| Database Provider | Entity Framework Package                  | Minimum Version | Wrapper Package                                                                       | Extension Method        |
|-------------------|-------------------------------------------|-----------------|---------------------------------------------------------------------------------------|-------------------------|
| MySQL             | `Pomelo.EntityFrameworkCore.MySql`        | 9.0.0+          | [`AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector`](../../AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector/README.md) | `UseAwsWrapperMySql`    |
| PostgreSQL        | `Npgsql.EntityFrameworkCore.PostgreSQL`   | 9.0.4+          | [`AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL`](../../AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL/README.md)         | `UseAwsWrapperNpgsql`   |

> [!NOTE]\
> The MySQL extension method `UseAwsWrapper` has been renamed to `UseAwsWrapperMySql`. The original name is still available but is marked `[Obsolete]` and will be removed in a future major version. Update existing call sites to `UseAwsWrapperMySql` to silence the deprecation warning.

## Usage

### MySQL

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseAwsWrapperMySql(
        connectionString,
        wrappedOptions => wrappedOptions.UseMySql(connectionString)));
```

See the package [README](../../AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector/README.md) for additional configuration, including registering custom EF Core MySQL providers.

### PostgreSQL

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseAwsWrapperNpgsql(
        connectionString,
        wrappedOptions => wrappedOptions.UseNpgsql(connectionString)));
```

See the package [README](../../AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL/README.md) for additional details.

## Examples

- [MySQL Entity Framework Example](../examples/MySqlEntityFrameworkExample/)
- [PostgreSQL Entity Framework Example](../examples/PgEntityFrameworkExample/)
