# AWS Advanced .NET Data Provider Wrapper - IAM Authentication Plugin

## Overview

This plugin provides AWS IAM database authentication support for the AWS Advanced .NET Data Provider Wrapper, enabling applications to authenticate to RDS and Aurora databases using IAM credentials instead of traditional username/password authentication.

## Dependencies

This project depends on:
- **AWSSDK.RDS**: AWS SDK for RDS to generate authentication tokens

## Usage

Enable the IAM plugin in your connection string:

```csharp
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "User Id=db-user;" +
                       "Plugins=iam;";
```

### Prerequisites

1. **IAM Policy**: Your IAM user/role must have the `rds-db:connect` permission
2. **Database User**: Create a database user mapped to your IAM user/role
3. **AWS Credentials**: Configure AWS credentials (via AWS CLI, environment variables, or IAM roles)

### Example IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds-db:connect"
            ],
            "Resource": [
                "arn:aws:rds-db:region:account-id:dbuser:db-instance-id/db-user-name"
            ]
        }
    ]
}
```

## Supported Databases

- Aurora MySQL
- Aurora PostgreSQL
- RDS MySQL
- RDS PostgreSQL

## Documentation

For comprehensive information about IAM database authentication and the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
