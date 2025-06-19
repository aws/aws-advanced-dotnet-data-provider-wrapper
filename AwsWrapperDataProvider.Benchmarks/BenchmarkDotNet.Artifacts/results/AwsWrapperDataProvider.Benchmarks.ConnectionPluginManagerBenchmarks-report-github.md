```

BenchmarkDotNet v0.13.12, macOS 15.5 (24F74) [Darwin 24.5.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 8.0.408
  [Host]     : .NET 8.0.15 (8.0.1525.16413), Arm64 RyuJIT AdvSIMD
  Job-PKKRDH : .NET 8.0.15 (8.0.1525.16413), Arm64 RyuJIT AdvSIMD

InvocationCount=1  RunStrategy=Monitoring  UnrollFactor=1  
WarmupCount=3  

```
| Method                | Mean     | Error    | StdDev   | Gen0   | Gen1   | Allocated |
|---------------------- |---------:|---------:|---------:|-------:|-------:|----------:|
| Open_WithPlugins      | 451.6 ns |  8.36 ns |  5.53 ns | 0.2860 |      - |    2408 B |
| Open_WithNoPlugins    | 181.1 ns | 85.91 ns | 56.82 ns | 0.0480 |      - |     408 B |
| Execute_WithPlugins   | 442.1 ns | 33.71 ns | 22.30 ns | 0.1700 | 0.0840 |    1432 B |
| Execute_WithNoPlugins | 251.1 ns | 23.69 ns | 15.67 ns | 0.0360 | 0.0180 |     312 B |
