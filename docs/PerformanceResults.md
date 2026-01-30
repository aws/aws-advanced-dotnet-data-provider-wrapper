# Plugin Pipeline Performance Results

## Performance Tests

The failure detection performance tests below will execute a long query, and monitoring will not begin until the `FailureDetectionGraceTime` has passed. A network outage will be triggered after the `NetworkOutageDelayMillis` has passed. The `FailureDetectionInterval` multiplied by the `FailureDetectionCount` represents how long the monitor will take to detect a failure once it starts sending probes to the host. This value combined with the time remaining from `FailureDetectionGraceTime` after the network outage is triggered will result in the expected failure detection time.

### PG Enhanced Failure Monitoring Performance with Different Failure Detection Configuration

| Failure Detection Grace Time | Failure Detection Interval | Failure Detection Count | Network Outage Delay Ms | Min Failure Detection Time Ms | Max Failure Detection Time Ms | Avg Failure Detection Time Ms |
|------------------------------|----------------------------|-------------------------|-------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30000 | 5000 | 3 | 10000 | 28266 | 29330 | 28880 |
| 30000 | 5000 | 3 | 15000 | 24259 | 24294 | 24276 |
| 30000 | 5000 | 3 | 20000 | 18158 | 19326 | 18646 |
| 30000 | 5000 | 3 | 25000 | 14186 | 14314 | 14241 |
| 30000 | 5000 | 3 | 30000 | 10451 | 11419 | 11171 |
| 30000 | 5000 | 3 | 35000 | 10310 | 11466 | 10995 |
| 30000 | 5000 | 3 | 40000 | 10354 | 11386 | 11157 |
| 30000 | 5000 | 3 | 45000 | 10454 | 11479 | 11235 |
| 30000 | 5000 | 3 | 50000 | 11340 | 11522 | 11415 |
| 30000 | 5000 | 3 | 55000 | 10447 | 13953 | 12560 |
| 30000 | 5000 | 3 | 60000 | 8956 | 8967 | 8960 |
| 6000 | 1000 | 1 | 1000 | 4016 | 4096 | 4037 |
| 6000 | 1000 | 1 | 2000 | 3005 | 3107 | 3041 |
| 6000 | 1000 | 1 | 3000 | 2029 | 2122 | 2079 |
| 6000 | 1000 | 1 | 4000 | 1010 | 1128 | 1081 |
| 6000 | 1000 | 1 | 5000 | 2000 | 2167 | 2052 |
| 6000 | 1000 | 1 | 6000 | 2048 | 2106 | 2066 |
| 6000 | 1000 | 1 | 7000 | 2016 | 2064 | 2036 |
| 6000 | 1000 | 1 | 8000 | 2013 | 2127 | 2068 |
| 6000 | 1000 | 1 | 9000 | 2053 | 2198 | 2098 |

### PG Failover and Enhanced Failure Monitoring Performance with Different Failure Detection Configuration

| Failure Detection Grace Time | Failure Detection Interval | Failure Detection Count | Network Outage Delay Ms | Min Failure Detection Time Ms | Max Failure Detection Time Ms | Avg Failure Detection Time Ms |
|------------------------------|----------------------------|-------------------------|-------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30000 | 5000 | 3 | 10000 | 30686 | 31611 | 31297 |
| 30000 | 5000 | 3 | 15000 | 25606 | 26960 | 26286 |
| 30000 | 5000 | 3 | 20000 | 21538 | 22055 | 21678 |
| 30000 | 5000 | 3 | 25000 | 15506 | 16712 | 16079 |
| 30000 | 5000 | 3 | 30000 | 10623 | 11992 | 11394 |
| 30000 | 5000 | 3 | 35000 | 10599 | 11675 | 11423 |
| 30000 | 5000 | 3 | 40000 | 10669 | 12062 | 11513 |
| 30000 | 5000 | 3 | 45000 | 10550 | 12127 | 11596 |
| 30000 | 5000 | 3 | 50000 | 10651 | 12082 | 11339 |
| 30000 | 5000 | 3 | 55000 | 11635 | 11739 | 11694 |
| 30000 | 5000 | 3 | 60000 | 10590 | 11746 | 11145 |
| 6000 | 1000 | 1 | 1000 | 6289 | 6728 | 6390 |
| 6000 | 1000 | 1 | 2000 | 5284 | 5356 | 5306 |
| 6000 | 1000 | 1 | 3000 | 4290 | 4345 | 4304 |
| 6000 | 1000 | 1 | 4000 | 3268 | 3340 | 3299 |
| 6000 | 1000 | 1 | 5000 | 2306 | 2324 | 2312 |
| 6000 | 1000 | 1 | 6000 | 2271 | 2334 | 2310 |
| 6000 | 1000 | 1 | 7000 | 2311 | 2773 | 2416 |
| 6000 | 1000 | 1 | 8000 | 2370 | 2399 | 2381 |
| 6000 | 1000 | 1 | 9000 | 2304 | 2394 | 2357 |

### Advanced PG Failover and Enhanced Failure Monitoring Performance with Different Failover Delay Time

| Driver Configuration | Failover Delay Ms | Min Failure Detection Time Ms | Max Failure Detection Time Ms | Avg Failure Detection Time Ms | Min Reconnect Time Ms | Max Reconnect Time Ms | Avg Reconnect Time Ms | Min DNS Update Time Ms | Max DNS Update Time Ms | Avg DNS Update Time Ms |
|---------------------|-------------------|-------------------------------|-------------------------------|-------------------------------|----------------------|----------------------|----------------------|------------------------|------------------------|------------------------|
| AWS Wrapper (PG, EFM, Failover) | 10000 | 0 | 0 | 0 | 10948 | 16503 | 14606.66667 | 0 | 0 | 0 |
| AWS Wrapper (PG, EFM) | 10000 | 9584 | 15418 | 13431 | 0 | 0 | 0 | 0 | 0 | 0 |
| DirectDriver - PG | 10000 | 10947 | 16504 | 14607.33333 | 0 | 0 | 0 | 0 | 0 | 0 |
| DNS | 10000 | 0 | 0 | 0 | 0 | 0 | 0 | 18300 | 28430 | 23373.33333 |
| AWS Wrapper (PG, EFM, Failover) | 20000 | 0 | 0 | 0 | 7959 | 18795 | 14920.66667 | 0 | 0 | 0 |
| AWS Wrapper (PG, EFM) | 20000 | 6873 | 17304 | 13764.66667 | 0 | 0 | 0 | 0 | 0 | 0 |
| DirectDriver - PG | 20000 | 7964 | 18781 | 14916.33333 | 0 | 0 | 0 | 0 | 0 | 0 |
| DNS | 20000 | 0 | 0 | 0 | 0 | 0 | 0 | 23487 | 43794 | 31926.33333 |
| AWS Wrapper (PG, EFM, Failover) | 30000 | 0 | 0 | 0 | 9480 | 12055 | 10611.66667 | 0 | 0 | 0 |
| AWS Wrapper (PG, EFM) | 30000 | 8128 | 11297 | 9443.333333 | 0 | 0 | 0 | 0 | 0 | 0 |
| DirectDriver - PG | 30000 | 9474 | 12052 | 10606.66667 | 0 | 0 | 0 | 0 | 0 | 0 |
| DNS | 30000 | 0 | 0 | 0 | 0 | 0 | 0 | 19357 | 22744 | 20574.33333 |
| AWS Wrapper (PG, EFM, Failover) | 40000 | 0 | 0 | 0 | 11886 | 15756 | 13547.66667 | 0 | 0 | 0 |
| AWS Wrapper (PG, EFM) | 40000 | 10447 | 14435 | 12389 | 0 | 0 | 0 | 0 | 0 | 0 |
| DirectDriver - PG | 40000 | 11892 | 15742 | 13546.33333 | 0 | 0 | 0 | 0 | 0 | 0 |
| DNS | 40000 | 0 | 0 | 0 | 0 | 0 | 0 | 23765 | 28902 | 25493 |
| AWS Wrapper (PG, EFM, Failover) | 50000 | 0 | 0 | 0 | 11493 | 255504 | 94657.33333 | 0 | 0 | 0 |
| AWS Wrapper (PG, EFM) | 50000 | 8227 | 15897 | 11686.66667 | 0 | 0 | 0 | 0 | 0 | 0 |
| DirectDriver - PG | 50000 | 11485 | 255086 | 94513.66667 | 0 | 0 | 0 | 0 | 0 | 0 |
| DNS | 50000 | 0 | 0 | 0 | 0 | 0 | 0 | 22904 | 28905 | 25227.66667 |
| AWS Wrapper (PG, EFM, Failover) | 60000 | 0 | 0 | 0 | 8446 | 14351 | 11398.66667 | 0 | 0 | 0 |
| AWS Wrapper (PG, EFM) | 60000 | 6957 | 13008 | 10006 | 0 | 0 | 0 | 0 | 0 | 0 |
| DirectDriver - PG | 60000 | 8446 | 14367 | 11400.66667 | 0 | 0 | 0 | 0 | 0 | 0 |
| DNS | 60000 | 0 | 0 | 0 | 0 | 0 | 0 | 14707 | 23876 | 19114.66667 |
