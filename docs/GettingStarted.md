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

