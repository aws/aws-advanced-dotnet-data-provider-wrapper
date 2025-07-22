// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Data.Common;

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitorConnectionContext
{
    public Mutex Lock = new();

    private readonly long failureDetectionTimeMillis;
    private readonly DbConnection connectionToAbort;
    private volatile bool activeContext;
    private volatile bool nodeUnhealthy;
    private DateTime startMonitorTime;

    public DateTime ExpectedActiveMonitoringStartTime { get; private set; }
    public long FailureCount { get; set; }
    public long FailureDetectionIntervalMillis { get; }
    public long FailureDetectionCount { get; }
    public DateTime InvalidNodeStartTime { get; private set; }
    public IHostMonitor Monitor { get; private set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="HostMonitorConnectionContext"/> class.
    /// </summary>
    /// <param name="monitor">A reference to a monitor object.</param>
    /// <param name="connectionToAbort">A reference to the connection associated with this context that will be aborted in case of server failure.</param>
    /// <param name="failureDetectionTimeMillis">Grace period after which node monitoring starts.</param>
    /// <param name="failureDetectionIntervalMillis">Interval between each failed connection check.</param>
    /// <param name="failureDetectionCount">Number of failed connection checks before considering database node as unhealthy.</param>
    public HostMonitorConnectionContext(
        IHostMonitor monitor,
        DbConnection connectionToAbort,
        long failureDetectionTimeMillis,
        long failureDetectionIntervalMillis,
        long failureDetectionCount)
    {
        this.Monitor = monitor;
        this.connectionToAbort = connectionToAbort;
        this.failureDetectionTimeMillis = failureDetectionTimeMillis;
        this.FailureDetectionIntervalMillis = failureDetectionIntervalMillis;
        this.FailureDetectionCount = failureDetectionCount;

        this.InvalidNodeStartTime = DateTime.UnixEpoch;
        this.activeContext = true;
        this.nodeUnhealthy = false;
    }

    public void SetStartMonitorTime(DateTime startMonitorTime)
    {
        this.startMonitorTime = startMonitorTime;
        this.ExpectedActiveMonitoringStartTime = startMonitorTime.AddMilliseconds(this.failureDetectionTimeMillis);
    }

    public void ResetInvalidNodeStartTime()
    {
        this.InvalidNodeStartTime = DateTime.UnixEpoch;
    }

    public bool IsInvalidNodeStartTimeDefined()
    {
        return this.InvalidNodeStartTime != DateTime.UnixEpoch;
    }

    public bool IsNodeUnhealthy()
    {
        return this.nodeUnhealthy;
    }

    public void SetNodeUnhealthy(bool nodeUnhealthy)
    {
        this.nodeUnhealthy = nodeUnhealthy;
    }

    public bool IsActiveContext()
    {
        return this.activeContext;
    }

    public void SetInactive()
    {
        this.activeContext = false;
    }

    public void AbortConnection()
    {
        if (this.connectionToAbort != null && this.activeContext)
        {
            try
            {
                // forcibly disconnects and frees all resources associated with the DbConnection
                // (.Close() may block, as it attempts to cleanly close)
                this.connectionToAbort.Dispose();
            }
            catch
            {
                // ignore exception while disposing
            }
        }
    }

    /// <summary>
    /// Update whether the connection is still valid if the total elapsed time has passed the grace period.
    /// </summary>
    /// <param name="hostName">A node name for logging purposes.</param>
    /// <param name="statusCheckStart">The time when connection status check started in nanos.</param>
    /// <param name="statusCheckEnd">The time when connection status check ended in nanos.</param>
    /// <param name="isValid">Whether the connection is valid.</param>
    public void UpdateConnectionStatus(string hostName, DateTime statusCheckStart, DateTime statusCheckEnd, bool isValid)
    {
        if (this.activeContext)
        {
            if (statusCheckStart - this.startMonitorTime > TimeSpan.FromMilliseconds(this.failureDetectionTimeMillis))
            {
                this.SetConnectionValid(hostName, isValid, statusCheckStart, statusCheckEnd);
            }
        }
    }

    /// <summary>
    /// Set whether the connection to the server is still valid based on the monitoring settings set in the connection.
    /// </summary>
    /// <param name="hostName">A node name for logging purposes.</param>
    /// <param name="connectionValid">Boolean indicating whether the server is still responsive.</param>
    /// <param name="statusCheckStart">The time when connection status check started in nanos.</param>
    /// <param name="statusCheckEnd">The time when connection status check ended in nanos.</param>
    public void SetConnectionValid(string hostName, bool connectionValid, DateTime statusCheckStart, DateTime statusCheckEnd)
    {
        if (!connectionValid)
        {
            this.FailureCount++;

            if (!this.IsInvalidNodeStartTimeDefined())
            {
                this.InvalidNodeStartTime = statusCheckStart;
            }

            TimeSpan invalidNodeDuration = statusCheckEnd - this.InvalidNodeStartTime;
            long maxInvalidNodeDurationMillis = this.FailureDetectionIntervalMillis * Math.Max(0, this.FailureDetectionCount);

            if (invalidNodeDuration >= TimeSpan.FromMilliseconds(maxInvalidNodeDurationMillis))
            {
                // host is dead
                this.SetNodeUnhealthy(true);
                this.AbortConnection();
                return;
            }

            // host is not responding
            return;
        }

        this.FailureCount = 0;
        this.ResetInvalidNodeStartTime();
        this.SetNodeUnhealthy(false);

        // host is alive
    }
}
