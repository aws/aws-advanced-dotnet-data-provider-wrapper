# Using the AWS Advanced .NET Data Provider Wrapper with Entity Framework

The AWS Advanced .NET Data Provider Wrapper can be seamlessly integrated with Entity Framework (EF) to provide enhanced AWS and Aurora functionalities while maintaining the familiar Entity Framework development experience.

> [!NOTE]\
> Currently Npgsql Entity Framework is not supported. [For more information on the limitations.](../README.md#known-limitations)

## Database Provider Compatibility

The AWS Advanced .NET Data Provider Wrapper works with Entity Framework through the underlying supported database providers:

| Database Provider | Entity Framework Package                | Minimum Version |
|-------------------|-----------------------------------------|-----------------|
| MySQL (Oracle)    | `MySql.EntityFrameworkCore`             | 9.0.0+          |

### Example

[MySql Entity Framework Example](../examples/MySqlEntityFramework.cs)
  