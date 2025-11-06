# AWS Advanced .NET Data Provider Wrapper - Federated Authentication Plugin

## Overview

This plugin provides AWS federated authentication support for the AWS Advanced .NET Data Provider Wrapper, enabling applications to authenticate to RDS and Aurora databases using federated identity providers through AWS Security Token Service (STS).

## Dependencies

This project depends on:
- **AWSSDK.RDS**: AWS SDK for RDS to generate authentication tokens
- **AWSSDK.SecurityToken**: AWS SDK for STS to handle federated authentication

## Usage

Enable the federated authentication plugin in your connection string:

```csharp
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "User Id=db-user;" +
                       "Plugins=federatedAuth;";
```

### Prerequisites

1. **Identity Provider**: Configure an external identity provider (SAML, OIDC)
2. **IAM Role**: Create an IAM role that can be assumed by the federated identity
3. **Database User**: Create a database user mapped to the IAM role
4. **Trust Relationship**: Configure trust relationship between the identity provider and IAM role

### Example Trust Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "arn:aws:iam::account-id:saml-provider/provider-name"
            },
            "Action": "sts:AssumeRoleWithSAML",
            "Condition": {
                "StringEquals": {
                    "SAML:aud": "https://signin.aws.amazon.com/saml"
                }
            }
        }
    ]
}
```

## Supported Authentication Methods

- SAML 2.0 federation
- OKTA federation
- Web identity federation

## Supported Databases

- Aurora MySQL
- Aurora PostgreSQL
- RDS MySQL
- RDS PostgreSQL

## Documentation

For comprehensive information about federated authentication and the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
