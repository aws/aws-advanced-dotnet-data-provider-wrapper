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

using System.Collections.Concurrent;
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;

/// <summary>
/// The default custom endpoint monitor implementation. This class uses a background thread to monitor a given custom
/// endpoint for custom endpoint information and future changes to the custom endpoint.
/// </summary>
public class CustomEndpointMonitor : ICustomEndpointMonitor
{
    private static readonly ILogger<CustomEndpointMonitor> Logger = LoggerUtils.GetLogger<CustomEndpointMonitor>();

    protected readonly ManualResetEventSlim refreshRequiredEvent = new(false);

    // Keys are custom endpoint URLs, values are information objects for the associated custom endpoint.
    internal static readonly ConcurrentDictionary<string, CustomEndpointInfo> CustomEndpointInfoCache = new();

    protected static readonly TimeSpan UnauthorizedSleepDuration = TimeSpan.FromMinutes(5);

    protected readonly CancellationTokenSource cancellationTokenSource = new();
    protected readonly AmazonRDSClient rdsClient;
    protected readonly HostSpec customEndpointHostSpec;
    protected readonly string endpointIdentifier;
    protected readonly RegionEndpoint region;
    protected readonly TimeSpan minRefreshRate;
    protected readonly TimeSpan maxRefreshRate;
    protected readonly int refreshRateBackoffFactor;

    protected readonly IPluginService pluginService;
    protected readonly Task monitorTask;

    protected TimeSpan currentRefreshRate;

    public CustomEndpointMonitor(
        IPluginService pluginService,
        HostSpec customEndpointHostSpec,
        string endpointIdentifier,
        RegionEndpoint region,
        TimeSpan refreshRate,
        int refreshRateBackoffFactor,
        TimeSpan maxRefreshRate,
        Func<RegionEndpoint, AmazonRDSClient> rdsClientFunc)
    {
        this.pluginService = pluginService;
        this.customEndpointHostSpec = customEndpointHostSpec;
        this.endpointIdentifier = endpointIdentifier;
        this.region = region;
        this.minRefreshRate = refreshRate;
        this.maxRefreshRate = maxRefreshRate;
        this.refreshRateBackoffFactor = refreshRateBackoffFactor;
        this.currentRefreshRate = refreshRate;
        this.rdsClient = rdsClientFunc(this.region);

        this.monitorTask = Task.Run(this.RunAsync, this.cancellationTokenSource.Token);
    }

    public static void ClearCache()
    {
        Logger.LogInformation(Resources.CustomEndpointMonitorImpl_ClearCache);
        CustomEndpointInfoCache.Clear();
    }

    /// <summary>
    /// Analyzes a given custom endpoint for changes to custom endpoint information.
    /// </summary>
    private async Task RunAsync()
    {
        Logger.LogTrace(Resources.CustomEndpointMonitorImpl_StartingMonitor, this.customEndpointHostSpec.Host);

        try
        {
            while (!this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                try
                {
                    DateTime start = DateTime.UtcNow;

                    var request = new DescribeDBClusterEndpointsRequest
                    {
                        DBClusterEndpointIdentifier = this.endpointIdentifier,
                        Filters = new List<Filter>
                        {
                            new()
                            {
                                Name = "db-cluster-endpoint-type",
                                Values = new List<string> { "custom" },
                            },
                        },
                    };

                    DescribeDBClusterEndpointsResponse endpointsResponse = await this.rdsClient.DescribeDBClusterEndpointsAsync(request);

                    List<DBClusterEndpoint> endpoints = endpointsResponse.DBClusterEndpoints;
                    if (endpoints.Count != 1)
                    {
                        List<string> endpointUrls = endpoints.Select(e => e.Endpoint).ToList();

                        Logger.LogWarning(Resources.CustomEndpointMonitorImpl_UnexpectedNumberOfEndpoints,
                            this.endpointIdentifier,
                            this.region.SystemName,
                            endpoints.Count,
                            string.Join(", ", endpointUrls));

                        this.Sleep(this.currentRefreshRate);
                        continue;
                    }

                    CustomEndpointInfo endpointInfo = CustomEndpointInfo.FromDBClusterEndpoint(endpoints[0]);
                    CustomEndpointInfoCache.TryGetValue(this.customEndpointHostSpec.Host, out CustomEndpointInfo? cachedEndpointInfo);
                    if (cachedEndpointInfo != null && cachedEndpointInfo.Equals(endpointInfo))
                    {
                        TimeSpan elapsedTime = DateTime.UtcNow - start;
                        TimeSpan sleepDuration = this.currentRefreshRate - elapsedTime;
                        if (sleepDuration > TimeSpan.Zero)
                        {
                            this.Sleep(sleepDuration);
                        }

                        continue;
                    }

                    Logger.LogTrace(Resources.CustomEndpointMonitorImpl_DetectedChangeInCustomEndpointInfo,
                        this.customEndpointHostSpec.Host,
                        endpointInfo);

                    // The custom endpoint info has changed, so we need to update the set of allowed/blocked hosts.
                    HashSet<string>? allowedHostIds = null;
                    HashSet<string>? blockedHostIds = null;

                    if (endpointInfo.MemberListType == MemberTypeList.StaticList)
                    {
                        allowedHostIds = endpointInfo.GetStaticMembers();
                    }
                    else
                    {
                        blockedHostIds = endpointInfo.GetExcludedMembers();
                    }

                    var allowedAndBlockedHosts = new AllowedAndBlockedHosts(
                        allowedHostIds,
                        blockedHostIds);

                    this.pluginService.SetAllowedAndBlockedHosts(this.customEndpointHostSpec.Host, allowedAndBlockedHosts);
                    CustomEndpointInfoCache[this.customEndpointHostSpec.Host] = endpointInfo;
                    this.refreshRequiredEvent.Reset();

                    this.SpeedupRefreshRate();

                    TimeSpan elapsed = DateTime.UtcNow - start;
                    TimeSpan sleep = this.currentRefreshRate - elapsed;
                    if (sleep > TimeSpan.Zero)
                    {
                        this.Sleep(sleep);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (AmazonRDSException rdsEx)
                {
                    // Handle AWS RDS exceptions with special logic
                    Logger.LogError(rdsEx, Resources.CustomEndpointMonitorImpl_Exception, this.customEndpointHostSpec.Host);

                    if (rdsEx.StatusCode == System.Net.HttpStatusCode.TooManyRequests ||
                        rdsEx.ErrorCode?.Contains("Throttling") == true ||
                        rdsEx.ErrorCode?.Contains("Throttled") == true)
                    {
                        // Throttling exception - slow down refresh rate
                        this.SlowdownRefreshRate();
                        this.Sleep(this.currentRefreshRate);
                    }
                    else if (rdsEx.StatusCode == System.Net.HttpStatusCode.Unauthorized ||
                             rdsEx.StatusCode == System.Net.HttpStatusCode.Forbidden)
                    {
                        // Unauthorized/Forbidden - sleep for longer duration
                        this.Sleep(UnauthorizedSleepDuration);
                    }
                    else
                    {
                        this.Sleep(this.currentRefreshRate);
                    }
                }
                catch (Exception e)
                {
                    // If the exception is not an OperationCanceledException, log it and continue monitoring.
                    Logger.LogError(e, Resources.CustomEndpointMonitorImpl_Exception, this.customEndpointHostSpec.Host);
                    this.Sleep(this.currentRefreshRate);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogTrace(Resources.CustomEndpointMonitorImpl_Interrupted, this.customEndpointHostSpec.Host);
        }
        finally
        {
            this.cancellationTokenSource.Dispose();
            this.refreshRequiredEvent.Dispose();
            CustomEndpointInfoCache.TryRemove(this.customEndpointHostSpec.Host, out _);
            this.rdsClient.Dispose();

            Logger.LogTrace(Resources.CustomEndpointMonitorImpl_StoppedMonitor, this.customEndpointHostSpec.Host);
        }
    }

    protected void Sleep(TimeSpan duration)
    {
        DateTime endTime = DateTime.UtcNow + duration;
        TimeSpan waitDuration = duration < TimeSpan.FromMilliseconds(500) ? duration : TimeSpan.FromMilliseconds(500);

        while (DateTime.UtcNow < endTime && !this.cancellationTokenSource.Token.IsCancellationRequested)
        {
            if (this.refreshRequiredEvent.IsSet)
            {
                this.refreshRequiredEvent.Reset();
                break;
            }

            try
            {
                bool wasSignaled = this.refreshRequiredEvent.Wait(waitDuration, this.cancellationTokenSource.Token);
                if (wasSignaled)
                {
                    this.refreshRequiredEvent.Reset();
                    break;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Speeds up the refresh rate after a successful call (moves toward minRefreshRate).
    /// </summary>
    protected void SpeedupRefreshRate()
    {
        if (this.currentRefreshRate > this.minRefreshRate)
        {
            this.currentRefreshRate = TimeSpan.FromMilliseconds(
                this.currentRefreshRate.TotalMilliseconds / this.refreshRateBackoffFactor);
            if (this.currentRefreshRate < this.minRefreshRate)
            {
                this.currentRefreshRate = this.minRefreshRate;
            }
        }
    }

    /// <summary>
    /// Slows down the refresh rate after a throttling exception (moves toward maxRefreshRate).
    /// </summary>
    protected void SlowdownRefreshRate()
    {
        if (this.currentRefreshRate < this.maxRefreshRate)
        {
            this.currentRefreshRate = TimeSpan.FromMilliseconds(
                this.currentRefreshRate.TotalMilliseconds * this.refreshRateBackoffFactor);
            if (this.currentRefreshRate > this.maxRefreshRate)
            {
                this.currentRefreshRate = this.maxRefreshRate;
            }
        }
    }

    public bool HasCustomEndpointInfo()
    {
        bool hasInfo = CustomEndpointInfoCache.ContainsKey(this.customEndpointHostSpec.Host);

        if (!hasInfo && !this.refreshRequiredEvent.IsSet)
        {
            this.refreshRequiredEvent.Set();
        }

        return hasInfo;
    }

    public bool ShouldDispose()
    {
        return true;
    }

    public void Dispose()
    {
        Logger.LogTrace(Resources.CustomEndpointMonitorImpl_StoppingMonitor, this.customEndpointHostSpec.Host);

        this.cancellationTokenSource.Cancel();

        try
        {
            const int terminationTimeoutSec = 5;
            if (!this.monitorTask.Wait(TimeSpan.FromSeconds(terminationTimeoutSec)))
            {
                Logger.LogInformation(
                    Resources.CustomEndpointMonitorImpl_MonitorTerminationTimeout,
                    terminationTimeoutSec,
                    this.customEndpointHostSpec.Host);
            }
        }
        catch (Exception e)
        {
            Logger.LogInformation(
                e,
                Resources.CustomEndpointMonitorImpl_InterruptedWhileTerminating,
                this.customEndpointHostSpec.Host);
        }
        finally
        {
            this.cancellationTokenSource.Dispose();
            CustomEndpointInfoCache.TryRemove(this.customEndpointHostSpec.Host, out _);
            this.rdsClient.Dispose();
        }
    }
}
