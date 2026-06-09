# Target Connection Dialects

## What are target connection dialects?
The AWS Advanced .NET Data Provider Wrapper is a wrapper that requires an underlying community .NET data provider, and it is meant to be compatible with any of them. Target connection dialects help the AWS Advanced .NET Data Provider Wrapper to properly pass calls to the underlying data provider. To function correctly, the wrapper requires details unique to a specific data provider such as how to build a connection string from a host specification and connection properties, how to map driver-specific connection string keys to wrapper properties, how to ping the connection to check it is alive, and which methods are allowed to be called on the connection. These details can be defined and provided to the wrapper by using target connection dialects.

By default, the target connection dialect is determined based on the type of the underlying `DbConnection` being used. If a data provider specific implementation is not found, the wrapper falls back to a generic target connection dialect.

> [!NOTE]
>
> The built-in target connection dialects for the supported data providers do not ship inside the core `AwsWrapperDataProvider` package. Each one is published as a separate NuGet package, so you only take a dependency on the data provider you actually use. Add the package that matches your data provider and call its loader before opening a connection. See the [Connection Dialects](./UsingTheDotNetDataProviderDriver.md#connection-dialects) section for more details:
>
> ```dotnet
> MySqlClientDialectLoader.Load();
> MySqlConnectorDialectLoader.Load();
> NpgsqlDialectLoader.Load();
> ```

## Configuration Parameters
| Name                          | Required             | Description                                                                                                                | Example                                                                                          |
|-------------------------------|----------------------|----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `CustomTargetConnectionDialect` | No (see notes below) | A custom target connection dialect. The value should be the `AssemblyQualifiedName` of a class that implements `ITargetConnectionDialect`. | `MyNamespace.MyCustomTargetConnectionDialect, MyAssembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null` |

> [!NOTE]
>
> The `CustomTargetConnectionDialect` parameter is not required. When it is not provided by the user, the AWS Advanced .NET Data Provider Wrapper will attempt to determine which of the registered target connection dialects to use based on the underlying connection type. If a data provider specific implementation is not found, the wrapper uses a generic target connection dialect.

### List of Available Target Connection Dialects
Target connection dialects are selected by the underlying `DbConnection` type. The dialects for the supported data providers each ship in their own NuGet package, separate from the core `AwsWrapperDataProvider` package; the generic fallback dialect is built into the core package. The table below lists the built-in dialects together with the connection types they support and the package that provides them.

| Target Connection Dialect Class    | Underlying Connection Type                | Data Provider                                                    | NuGet Package                                                  |
|-------------------------------------|-------------------------------------------|-----------------------------------------------------------------|----------------------------------------------------------------|
| `NpgsqlDialect`                     | `Npgsql.NpgsqlConnection`                 | [Npgsql](https://www.nuget.org/packages/Npgsql/)                | `AWS.AdvancedDotnetDataProviderWrapper.Dialect.Npgsql`         |
| `MySqlConnectorDialect`             | `MySqlConnector.MySqlConnection`          | [MySqlConnector](https://www.nuget.org/packages/MySqlConnector/)| `AWS.AdvancedDotnetDataProviderWrapper.Dialect.MySqlConnector` |
| `MySqlClientDialect`                | `MySql.Data.MySqlClient.MySqlConnection`  | [MySql.Data](https://www.nuget.org/packages/mysql.data/)        | `AWS.AdvancedDotnetDataProviderWrapper.Dialect.MySqlClient`    |
| `GenericTargetConnectionDialect`    | Any other `DbConnection`                  | Any other .NET data provider                                    | `AwsWrapperDataProvider` (core)                                |

## Custom Target Connection Dialects
If you are interested in using the AWS Advanced .NET Data Provider Wrapper but your desired data provider has unique features so the existing generic dialect doesn't work well with it, it is possible to create a custom target connection dialect.

To create a custom target connection dialect, implement the [`ITargetConnectionDialect`](../../AwsWrapperDataProvider/Driver/TargetConnectionDialects/ITargetConnectionDialect.cs) interface, or extend the [`AbstractTargetConnectionDialect`](../../AwsWrapperDataProvider/Driver/TargetConnectionDialects/AbstractTargetConnectionDialect.cs) base class to inherit common behavior. See the following classes for examples:

- [`GenericTargetConnectionDialect`](../../AwsWrapperDataProvider/Driver/TargetConnectionDialects/GenericTargetConnectionDialect.cs)
  - This is a generic dialect that should work with any .NET data provider.

Once the custom target connection dialect class has been created, you can tell the AWS Advanced .NET Data Provider Wrapper to use it in one of two ways:

1. Set the `CustomTargetConnectionDialect` connection property to the `AssemblyQualifiedName` of the custom dialect class:

```dotnet
var builder = new AwsWrapperConnectionStringBuilder
{
    // ... other connection properties ...
    CustomTargetConnectionDialect = typeof(MyCustomTargetConnectionDialect).AssemblyQualifiedName,
};
```

2. Register the dialect for a specific underlying connection type using `TargetConnectionDialectProvider.RegisterDialect`. This is the same mechanism the built-in data provider loaders use:

```dotnet
TargetConnectionDialectProvider.RegisterDialect(
    "MyNamespace.MyConnection",
    new MyCustomTargetConnectionDialect());
```
