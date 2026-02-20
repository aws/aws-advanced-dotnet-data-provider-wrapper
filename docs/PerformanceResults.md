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

| Driver Configuration | Failover Delay Ms | EFM Detection Time | Failover/EFM Detection Time | DirectDriver Detection Time | DNS Update Time |
|---------------------|-------------------|-------------------|----------------------------|---------------------------|-----------------|
| PG Run Iteration 1 | 10000 | 12336 | 13638 | 19471 | 23656 |
| PG Run Iteration 2 | 10000 | 6584 | 7517 | 13466 | 18337 |
| PG Run Iteration 3 | 10000 | 15158 | 16658 | 22406 | 28860 |
| PG Run Iteration 4 | 10000 | 8003 | 8919 | 15205 | 19263 |
| PG Run Iteration 5 | 10000 | 9072 | 10597 | 16079 | 23611 |
| PG Run Iteration 1 | 20000 | 9958 | 11468 | 17083 | 39138 |
| PG Run Iteration 2 | 20000 | 11765 | 12718 | 19621 | 23420 |
| PG Run Iteration 3 | 20000 | 17395 | 18614 | 25141 | 28443 |
| PG Run Iteration 4 | 20000 | 15113 | 16592 | 21369 | 28678 |
| PG Run Iteration 5 | 20000 | 11189 | 12511 | 17814 | 18670 |
| PG Run Iteration 1 | 30000 | 12933 | 14221 | 20493 | 28280 |
| PG Run Iteration 2 | 30000 | 11891 | 14631 | 18777 | 24511 |
| PG Run Iteration 3 | 30000 | 12584 | 13172 | 19607 | 28530 |
| PG Run Iteration 4 | 30000 | 15304 | 16533 | 22786 | 22876 |
| PG Run Iteration 5 | 30000 | 13696 | 14941 | 21835 | 27567 |
| PG Run Iteration 1 | 40000 | 6904 | 8099 | 14050 | 34444 |
| PG Run Iteration 2 | 40000 | 7929 | 9400 | 14659 | 18651 |
| PG Run Iteration 3 | 40000 | 14717 | 15557 | 32773 | 39068 |
| PG Run Iteration 4 | 40000 | 9639 | 10381 | 17930 | 19957 |
| PG Run Iteration 5 | 40000 | 10499 | 11237 | 18775 | 18813 |
| PG Run Iteration 1 | 50000 | 5525 | 6414 | 13179 | 14244 |
| PG Run Iteration 2 | 50000 | 8547 | 9256 | 16940 | 20157 |
| PG Run Iteration 3 | 50000 | 13000 | 14346 | 32578 | 24274 |
| PG Run Iteration 4 | 50000 | 9711 | 10377 | 16060 | 24481 |
| PG Run Iteration 5 | 50000 | 12056 | 13439 | 32488 | 24278 |

