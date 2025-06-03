using BenchmarkDotNet.Running;
using AwsWrapperDataProvider.Benchmarks;

// Run all benchmarks
var summary = BenchmarkRunner.Run(typeof(Program).Assembly);

// Alternatively, you can run specific benchmarks:
// var summary = BenchmarkRunner.Run<PluginManagerBenchmarks>();
// var summary = BenchmarkRunner.Run<ConnectionBenchmarks>();
