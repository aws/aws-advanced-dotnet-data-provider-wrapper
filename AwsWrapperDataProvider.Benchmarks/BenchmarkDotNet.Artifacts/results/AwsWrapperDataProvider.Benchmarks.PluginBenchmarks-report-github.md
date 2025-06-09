```

BenchmarkDotNet v0.13.12, macOS 15.5 (24F74) [Darwin 24.5.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 8.0.408
  [Host]     : .NET 8.0.15 (8.0.1525.16413), Arm64 RyuJIT AdvSIMD
  Job-PKKRDH : .NET 8.0.15 (8.0.1525.16413), Arm64 RyuJIT AdvSIMD

InvocationCount=1  RunStrategy=Monitoring  UnrollFactor=1  
WarmupCount=3  

```
| Method                                                         | Mean       | Error     | StdDev   | Gen0   | Gen1   | Allocated |
|--------------------------------------------------------------- |-----------:|----------:|---------:|-------:|-------:|----------:|
| ConnectionPluginManager_InitAndRelease_WithPlugins             |   965.3 ns | 112.77 ns | 74.59 ns | 0.4440 | 0.2220 |   3.63 KB |
| ConnectionPluginMananger_InitAndRelease_WithoutPlugins         |   551.3 ns |  33.95 ns | 22.46 ns | 0.1860 | 0.0920 |   1.52 KB |
| ExecuteStatement_WithPlugins                                   | 3,056.0 ns |  57.49 ns | 38.03 ns | 1.4820 | 0.4940 |  12.11 KB |
| ExecuteStatement_WithoutPlugins                                | 3,047.8 ns |  47.79 ns | 31.61 ns | 1.4820 | 0.4940 |  12.11 KB |
| ConnectionPluginManager_InitAndRelease_WithExecutionTimePlugin |   837.9 ns |   8.42 ns |  5.57 ns | 0.4480 | 0.2240 |   3.66 KB |
| ExecuteStatement_WithExecutionTimePlugin                       | 3,719.5 ns | 113.61 ns | 75.15 ns | 1.6740 | 0.5580 |  13.69 KB |
