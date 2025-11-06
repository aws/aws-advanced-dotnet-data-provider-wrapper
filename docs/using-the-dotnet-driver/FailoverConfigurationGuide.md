# Failover Configuration Guide

## Tips to Keep in Mind

### Failover Time Profiles
A failover time profile refers to a specific combination of failover parameters that determine the time in which failover should be completed and define the aggressiveness of failover. Some failover parameters include `FailoverTimeoutMs` and `ReaderHostSelectorStrategy`. Failover should be completed within 5 minutes by default. If the connection is not re-established during this time, then the failover process times out and fails. Users can configure the failover parameters to adjust the aggressiveness of the failover and fulfill the needs of their specific application. For example, a user could take a more aggressive approach and shorten the time limit on failover to promote a fail-fast approach for an application that does not tolerate database outages. Examples of normal and aggressive failover time profiles are shown below. 
<br><br>

> [!WARNING]:\
> Aggressive failover does come with its side effects. Since the time limit on failover is shorter, it becomes more likely that a problem is caused not by a failure, but rather because of a timeout.

#### Example of the configuration for a normal failover time profile:
| Parameter                                    | Value |
|----------------------------------------------|-------|
| `FailoverTimeoutMs`                       | `300000` |

#### Example of the configuration for an aggressive failover time profile:
| Parameter                                    | Value |
|----------------------------------------------|-------|
| `FailoverTimeoutMs`                       | `30000`  |

### Writer Cluster Endpoints After Failover
Connecting to a writer cluster endpoint after failover can result in a faulty connection because there can be a delay before the endpoint is updated to point to the new writer. On the AWS DNS server, this change is usually updated after 15-20 seconds, but the other DNS servers sitting between the application and the AWS DNS server may take longer to update. Using the stale DNS data will most likely cause problems for users, so it is important to keep this in mind.

### 2-Host Clusters
The failover process has limited advantages for a 2-host cluster because there are not as many instances available to replace the instance that has failed. In particular, when a reader instance fails, there are no other readers to fail over to. Instead, Aurora must revive the same instance that has failed. To improve the stability of the cluster, we recommend that your database cluster has at least 3 instances.

### Monitor Failures and Investigate
If you are experiencing difficulties with the failover plugin, try the following:
- Enable logging to find the cause of the failure. If it is a timeout, review the [failover time profiles](#failover-time-profiles) section and adjust the timeout values.
