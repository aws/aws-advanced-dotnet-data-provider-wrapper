# Integration Tests

### Prerequisites

- Docker Desktop:
    - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
    - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
- [Environment variables](#Environment-Variables)

##### Aurora Test Requirements
- An AWS account with:
    - RDS permissions
    - EC2 permissions so integration tests can add the current IP address in the Aurora cluster's EC2 security group.
    - For more information, see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

- An available Aurora PostgreSQL or MySQL DB cluster is required if you're running the tests against an existing DB cluster.

### Aurora Integration Tests

The Aurora integration tests are focused on testing connection strings and failover capabilities of any driver.
The tests are run in Docker but make a connection to test against a MySql and Postgres Aurora cluster.

### Standard Integration Tests

These integration tests are focused on testing connection strings against a local MySql and Postgres database inside a Docker container.

### Environment Variables

If the environment variable `REUSE_RDS_DB` is set to true, the integration tests will use the existing cluster defined by your environment variables. Otherwise, the integration tests will create a new Aurora cluster and then delete it automatically when the tests are done. Note that you will need a valid Docker environment to run any of the integration tests because they are run using a Docker environment as a host. The appropriate Docker containers will be created automatically when you run the tests, so you will not need to execute any Docker commands manually.

>[!NOTE] if you are running tests against an existing cluster, the tests will only run against the Aurora database engine of that cluster. For example, if you specify a MySQL cluster using the environment variables, only the MySQL tests will be run even if you pick test-all-aurora as the task. To run against Postgres instead, you will need to change your environment variables

| Environment Variable Name | Required | Description                                                                                                                                                                                                      | Example Value                                |
|---------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `DB_USERNAME`             | Yes      | The username to access the database.                                                                                                                                                                             | `admin`                                      |
| `DB_PASSWORD`             | Yes      | The database cluster password.                                                                                                                                                                                   | `password`                                   |
| `DB_DATABASE_NAME`        | No       | Name of the database that will be used by the tests. The default database name is test.                                                                                                                          | `test_db_name`                               |
| `RDS_DB_NAME`        | Yes      | The database identifier for your Aurora or RDS cluster. Must be a unique value to avoid conflicting with existing clusters.                                                                                      | `db-identifier`                              |
| `RDS_DB_DOMAIN`      | No       | The existing database connection suffix. Use this variable to run against an existing database.                                                                                                                  | `XYZ.us-east-2.rds.amazonaws.com`            |
| `IAM_USER`                | No       | User within the database that is identified with AWSAuthenticationPlugin. This is used for AWS IAM Authentication and is optional                                                                                | `example_user_name`                          |
| `AWS_ACCESS_KEY_ID`       | Yes      | An AWS access key associated with an IAM user or role with RDS permissions.                                                                                                                                      | `ASIAIOSFODNN7EXAMPLE`                       |
| `AWS_SECRET_ACCESS_KEY`   | Yes      | The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                                                                                                   | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`   |
| `AWS_SESSION_TOKEN`       | No       | AWS Session Token for CLI, SDK, & API access. This value is for MFA credentials only. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | `AQoDYXdzEJr...<remainder of session token>` |                                          |
| `REUSE_RDS_DB`            | Yes      | Set to true if you would like to use an existing cluster for your tests.                                                                                                                                         | `false`                                      |
| `RDS_DB_REGION`           | Yes      | The database region.                                                                                                                                                                                             | `us-east-2`                                  |

### Running the Integration Tests

To run the integration tests, you can select from a number of tasks:
- `test-all-mysql-aurora`: run all MySql Aurora tests
- `test-all-mysql-aurora-ef`: run all MySql Aurora Entity Framework tests
- `test-all-mysql-multi-az-cluster`: run all MySql multi-az cluster tests
- `test-all-mysql-multi-az-cluster-ef`:run all MySql multi-az cluster Entity Framework tests
- `test-all-mysql-multi-az-instance`: run all MySql multi-az instance tests
- `test-all-mysql-multi-az-instance-ef`: run all MySql multi-az instance Entity Framework tests
- `test-all-pg-aurora`: run all Postgres Aurora tests
- `test-all-pg-multi-az-cluster`: run all Postgres Aurora multi-az cluster tests
- `test-all-pg-multi-az-instance`: run all Postgres Aurora multi-az instance tests  

For example, to run all MySql Aurora integration tests, you can use the following commands:

macOS:
```bash
./gradlew --no-parallel --no-daemon test-all-mysql-aurora
```

Windows:
```bash
cmd /c ./gradlew --no-parallel --no-daemon test-all-mysql-aurora
```

# Set required environment variables
$env:DB_USERNAME = "admin"
$env:DB_PASSWORD = "your_password_here"
$env:RDS_DB_NAME = "your-cluster-name"
$env:AWS_ACCESS_KEY_ID = "your_access_key_id"
$env:AWS_SECRET_ACCESS_KEY = "your_secret_access_key"
$env:REUSE_RDS_DB = "false"
$env:RDS_DB_REGION = "us-east-2"

# Set optional environment variables
$env:DB_DATABASE_NAME = "test"  # Optional - defaults to 'test' if not set
$env:RDS_DB_DOMAIN = ""    # Optional - for existing database
$env:IAM_USER = ""             # Optional - for AWS IAM Authentication
$env:AWS_SESSION_TOKEN = ""     # Optional - for MFA credentials

# Display set environment variables
Write-Host "Environment variables have been set:"
Write-Host "DB_USERNAME: $env:DB_USERNAME"
Write-Host "RDS_DB_NAME: $env:RDS_DB_NAME"
Write-Host "REUSE_RDS_DB: $env:REUSE_RDS_DB"
Write-Host "RDS_DB_REGION: $env:RDS_DB_REGION"
Write-Host "DB_DATABASE_NAME: $env:DB_DATABASE_NAME"
