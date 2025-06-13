# Benchmarks for AWS Advanced .NET Data Provider Wrapper

This directory contains a set of benchmarks for the AWS Advanced .NET Data Provider Wrapper.
These benchmarks measure the overhead from executing ADO.NET operations with multiple plugins enabled.
The benchmarks do not measure the performance of target data providers nor the performance of the failover process.

## Usage

1. Build the benchmarks with the following command:
   ```bash
   dotnet build -c Release
   ```

2. Run the benchmarks with the following command:
   ```bash
   dotnet run -c Release
   ```

3. To run specific benchmarks:
   ```bash
   dotnet run -c Release -- --filter *ConnectionPluginManager*
   ```

## Running Specific Benchmark Categories

You can filter benchmarks by class name or method name:

```bash
# Run only ConnectionPluginManagerBenchmarks
dotnet run -c Release -- --filter *ConnectionPluginManagerBenchmarks*

# Run only PluginBenchmarks
dotnet run -c Release -- --filter *PluginBenchmarks*
```

## Additional Benchmark Options

You can customize the benchmark run with additional BenchmarkDotNet options:

```bash
# Run shorter benchmarks
dotnet run -c Release -- --job short

# Export results to specific formats
dotnet run -c Release -- --exporters json html

# Combine options
dotnet run -c Release -- --filter *ConnectionPluginManager* --job short --exporters json
```

## Benchmark Categories

- **ConnectionPluginManagerBenchmarks**: Tests the performance of the connection plugin manager
- **PluginBenchmarks**: Tests the performance of various plugins

## Results

Benchmark results will be generated in the `BenchmarkDotNet.Artifacts` directory in various formats including:
- Plain text reports
- GitHub Markdown
- HTML reports
- CSV files for further analysis
