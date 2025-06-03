# Benchmarks for AWS Advanced .NET Data Provider Wrapper

This directory contains a set of benchmarks for the AWS Advanced .NET Data Provider Wrapper.
These benchmarks measure the overhead from executing ADO.NET operations with multiple plugins enabled.
The benchmarks do not measure the performance of target data providers nor the performance of the failover process.

## Usage

1. Build the benchmarks with the following command:
   ```
   dotnet build -c Release
   ```

2. Run the benchmarks with the following command:
   ```
   dotnet run -c Release
   ```

3. To run specific benchmarks:
   ```
   dotnet run -c Release --filter *PluginManagerBenchmarks*
   ```

## Benchmark Categories

- **PluginManagerBenchmarks**: Tests the performance of the plugin manager with and without plugins
- **ConnectionBenchmarks**: Tests connection operations with different configurations

## Results

Benchmark results will be generated in the `BenchmarkDotNet.Artifacts` directory in various formats including:
- Plain text reports
- GitHub Markdown
- HTML reports
- CSV files for further analysis
