# Getting Started

This guide provides instructions on how to build, run, and lint the AwsWrapperDataProvider project.

## Prerequisites

Before you begin, ensure you have the following installed:

- [.NET SDK](https://dotnet.microsoft.com/download) (version 8.0 recommended)

## Project Structure

The solution contains several projects:

- **AwsWrapperDataProvider**: Core library for AWS Wrapper functionality
- **AwsWrapperDataProvider.Tests**: Unit tests for the core library

## Building the Project

### Using the Command Line

To build the entire solution:

```bash
dotnet build
```

To build a specific project:

```bash
dotnet build AwsWrapperDataProvider/AwsWrapperDataProvider.csproj
```

### Using an IDE

- **Visual Studio**: Open the `AwsWrapperDataProvider.sln` file and build using the Build menu or F6
- **Visual Studio Code**: Open the project folder and use the .NET Core Build task
- **JetBrains Rider**: Open the solution file and build using the Build menu

## Running Tests

### Using the Command Line

To run all tests:

```bash
dotnet test
```

To run tests for a specific project:

```bash
dotnet test AwsWrapperDataProvider.Tests/AwsWrapperDataProvider.Tests.csproj
```

To run a specific test:

```bash
dotnet test --filter "FullyQualifiedName=Namespace.TestClass.TestMethod"
```

### Using an IDE

Most IDEs provide integrated test runners that allow you to run and debug tests directly from the test files.

### Running Federated Auth Manual Integration Tests

#### OKTA

##### Where to Find XYZ

1. Endpoint

    Found on Okta Admin Console

    Okta Admin Dashboard, go to Employee and Customer Identity Solutions | Okta  and login with the admin credentials (sign up information)

    Under Admin Console (Left Sidebar), go to Application → Applications

    Select your application

    Navigate to the Sign On tab

    Within Settings, under SAML 2.0, there will be a Metadata URL

    The Endpoint will be up to, and before, the /app section, e.g. 

    `https://<okta-account-id>.okta.com/app/<application-id>/sso/saml/metadata`, the endpoint will be `<okta-account-id>.okta.com`.

2. AppID

    Found on Okta Admin Console

    Okta Admin Dashboard, go to Employee and Customer Identity Solutions | Okta  and login with the admin credentials (sign up information)

    Under Admin Console (Left Sidebar), go to Application → Applications

    Select your application

    Navigate to the Sign On tab

    Within Settings, under SAML 2.0, there will be a Metadata URL

    The AppID will the section between /app/ and /sso. e.g.

    `https://<okta-account-id>.okta.com/app/<application-id>/sso/saml/metadata` the appid will be `<application-id>`

3. Username / Password

    Found on Okta Admin Console

    Okta Admin Dashboard, go to www.okta.com and login with the admin credentials (sign up information)

    Under Admin Console (Left Sidebar), go to Directory → People

    Can now add person, reset password, etc

    Clicking into the created user, can now also Assign Application

4. SAML Provider ARN

    Found on AWS Console

    Login to AWS Console

    Go to IAM

    In the left sidebar, under Access management select Identity Providers

    Choose your provider, and the ARN will be near the top right in the form of: `arn:aws:iam::<account-id>:saml-provider/<identity-provider-name>`

5. Role ARN

    Found on AWS Console

    Login to AWS Console

    Go to IAM

    In the left sidebar, under Access management select Roles

    Choose your role, and the ARN will be near the top middle in the form of: `arn:aws:iam::<account-id>:role/<role-name>`

#### Running the tests

To run the OKTA integration tests manually, you will need the following information:

- IDP Endpoint (e.g., *.okta.com)
- App ID
- IDP Port (often HTTPS; 443)
- IDP Username (OKTA)
- IDP Password (OKTA)
- IAM Role ARN (AWS)
- IAM SAML Provider ARN (AWS)

As well as the database connectivity information:

- Host
- DB user with access via IAM authentication
- Database
- Port
- Etc.

Additionally, your IP address must be on the allowlist on the OKTA dashboard.

With all of this information, you can simply replace the properties as labelled in AwsWrapperDataProvider.Tests/FederatedAuthConnectivityTests.cs.

## Code Linting and Style Checking

The project uses StyleCop.Analyzers for code style enforcement and .editorconfig for consistent formatting.

### Running Code Analysis

To run code analysis and verify that all files conform to the style rules:

```bash
dotnet format --verify-no-changes
```

This command will check all files against the style rules defined in .editorconfig and StyleCop, but won't make any changes. It will exit with a non-zero code if any files would need formatting, making it ideal for CI/CD pipelines.

### Formatting Code

To format your code according to the project's style rules:

```bash
dotnet format
```

This command will automatically format your code to match the style defined in the .editorconfig file.

For more control over the formatting process, you can use the following command:

```bash
# Windows (Command Prompt)
dotnet format AwsWrapperDataProvider.sln --include .\AwsWrapperDataProvider\ .\AwsWrapperDataProvider.Tests\ --verbosity diagnostic

# macOS/Linux/Windows (PowerShell)
dotnet format AwsWrapperDataProvider.sln --include ./AwsWrapperDataProvider/ ./AwsWrapperDataProvider.Tests/ --verbosity diagnostic
```

> **Note:** The main difference between OS platforms is the path separator: Windows Command Prompt uses backslashes (`\`), while macOS/Linux and Windows PowerShell can use forward slashes (`/`).

This command provides more specific formatting with the following flags:
- `AwsWrapperDataProvider.sln`: Specifies the solution file to format
- `--include`: Limits formatting to specific directories (in this case, only the core library and its tests)
- `--verify-no-changes`: Fails if formatting would change any files (useful in CI pipelines to ensure code is already formatted)
- `--verbosity diagnostic`: Provides detailed output about the formatting process, showing exactly what would be changed

