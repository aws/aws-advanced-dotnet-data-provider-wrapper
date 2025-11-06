# Development Guide

## Prerequisites

Before you begin, ensure you have the following installed:

- [.NET SDK](https://dotnet.microsoft.com/download) (version 8.0 recommended)

### Setup
Clone the AWS Advanced .NET Data Provider Wrapper repository:

```bash
git clone https://github.com/aws/aws-advanced-dotnet-data-provider-wrapper.git
```

You can now make changes in the repository.

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

- **Visual Studio**:
  1. Press F6
  2. Click "Build" → "Build Solution"
  3. Press Ctrl + Shift + B to build
- **Visual Studio Code**:
  1. Open Command Palette (Ctrl + Shift + P)
  2. Search for `Tasks: Configure Default Build Task`
  3. Select `.NET Core Build`
     4. Creates a `tasks.json` file
  5. Press Ctrl + Shift + B to build
- **JetBrains Rider**:
  1. Build using the build menu.

## Testing Overview

The AWS Advanced .NET Data Provider Wrapper uses the following tests to verify its correctness:

| Tests                                         | Description                                                                                                                    |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Unit tests                                    | Tests for AWS Advanced .Net Data Provider Wrapper correctness.                                                                 |
| Failover integration tests                    | Driver-specific tests for different reader and writer failover workflows using the Failover Connection Plugin.                 |
| Enhanced failure monitoring integration tests | Driver-specific tests for the enhanced failure monitoring functionality using the Host Monitoring Connection Plugin.           |
| AWS authentication integration tests          | Driver-specific tests for AWS authentication methods with the AWS Secrets Manager Plugin or the AWS IAM Authentication Plugin. |

### Performance Tests

The AWS Advanced .NET Data Provider Wrapper has the following tests to verify its performance:

| Tests                                | Description                                                                                                                                                                                                                                                                                                   |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Connection plugin manager benchmarks | The benchmarks subproject uses BenchmarkDotNet to measure the overhead from executing .NET method calls with multiple connection plugins enabled. Tests include scenarios with and without plugins, running 500,000 operations per test to measure performance impact.                                        |
| Manually-triggered performance tests | Two types of performance tests measure plugin behavior: failover plugin performance tests evaluate recovery time and system behavior during failures, while enhanced failure monitoring performance tests measure the impact of read/write splitting and monitoring configurations under different scenarios. |

### Running the Tests

Running the tests will also validate your environment is set up correctly.

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

#### Integration Tests
For more information on how to run the integration tests, please visit [Integration Tests](../development-guide/IntegrationTests.md).

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
