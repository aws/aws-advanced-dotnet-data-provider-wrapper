# Using the AWS Advanced .NET Data Provider Wrapper with Entity Framework

The AWS Advanced .NET Data Provider Wrapper can be seamlessly integrated with Entity Framework (EF) Core to provide enhanced AWS and Aurora functionalities while maintaining the familiar Entity Framework development experience.

## Database Provider Compatibility

The AWS Advanced .NET Data Provider Wrapper works with Entity Framework Core through the underlying supported database providers:

| Database Provider | Entity Framework Package                  | Minimum Version | Wrapper Package                                                                       | Extension Method        |
|-------------------|-------------------------------------------|-----------------|---------------------------------------------------------------------------------------|-------------------------|
| MySQL             | `Microting.EntityFrameworkCore.MySql`     | 10.0.11+        | [`AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector`](../../AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector/README.md) | `UseAwsWrapperMySql`    |
| PostgreSQL        | `Npgsql.EntityFrameworkCore.PostgreSQL`   | 10.0.3+         | [`AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL`](../../AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL/README.md)         | `UseAwsWrapperNpgsql`   |

Both wrapper packages require Entity Framework Core 10, so the minimum versions above are the first releases of each provider built against it.

> [!IMPORTANT]\
> The MySQL provider is [`Microting.EntityFrameworkCore.MySql`](https://www.nuget.org/packages/Microting.EntityFrameworkCore.MySql/), a fork of `Pomelo.EntityFrameworkCore.MySql`. The fork is required because upstream Pomelo has no Entity Framework Core 10 release — its latest version targets EF Core 9 and caps `Microsoft.EntityFrameworkCore.Relational` below 10.
>
> The fork renamed its assembly and namespaces from `Pomelo.*` to `Microting.*`, and the wrapper selects the MySQL dialect by matching that assembly name. **Referencing upstream Pomelo instead will not work**: the wrapper cannot resolve a dialect for it and `UseAwsWrapperMySql` throws. Aside from the package id and the `using` directives, the API is unchanged — `UseMySql` and `MySqlServerVersion` are called exactly as before.

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
