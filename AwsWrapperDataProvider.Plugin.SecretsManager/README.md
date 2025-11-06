# AWS Advanced .NET Data Provider Wrapper - Secrets Manager Plugin

## Overview

This plugin provides AWS Secrets Manager integration for the AWS Advanced .NET Data Provider Wrapper, enabling applications to retrieve database credentials securely from AWS Secrets Manager instead of hardcoding them in connection strings.

## Dependencies

This project depends on:
- **AWSSDK.SecretsManager**: AWS SDK for Secrets Manager to retrieve secrets

## Usage

Enable the Secrets Manager plugin in your connection string:

```csharp
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "SecretArn=arn:aws:secretsmanager:region:account:secret:secret-name;" +
                       "Plugins=secretsManager;";
```

### Prerequisites

1. **IAM Permissions**: Your application must have permissions to access the secret
2. **Secret Format**: The secret must contain database credentials in the expected format
3. **AWS Credentials**: Configure AWS credentials (via AWS CLI, environment variables, or IAM roles)

### Example IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": [
                "arn:aws:secretsmanager:region:account:secret:your-secret-name-*"
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
- Self-managed databases

## Documentation

For comprehensive information about AWS Secrets Manager integration and the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
