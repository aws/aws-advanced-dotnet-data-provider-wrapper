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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;

/// <summary>
/// The default custom endpoint monitor implementation. This class uses a background thread to monitor a given custom
/// endpoint for custom endpoint information and future changes to the custom endpoint.
/// </summary>
public class CustomEndpointMonitor : ICustomEndpointMonitor
{
    private static readonly ILogger<CustomEndpointMonitor> Logger = LoggerUtils.GetLogger<CustomEndpointMonitor>();
    private const string TelemetryEndpointInfoChanged = "customEndpoint.infoChanged.counter";

    // Keys are custom endpoint URLs, values are information objects for the associated custom endpoint.
    protected static readonly ConcurrentDictionary<string, CustomEndpointInfo> CustomEndpointInfoCache = new();
    protected static readonly TimeSpan CustomEndpointInfoExpiration = TimeSpan.FromMinutes(5);

    protected readonly CancellationTokenSource cancellationTokenSource = new();
    protected readonly AmazonRDSClient rdsClient;
    protected readonly HostSpec customEndpointHostSpec;
    protected readonly string endpointIdentifier;
    protected readonly RegionEndpoint region;
    protected readonly TimeSpan refreshRate;

    protected readonly IPluginService pluginService;
    protected readonly Task monitorTask;

    /// <summary>
    /// Constructs a CustomEndpointMonitor instance for the host specified by customEndpointHostSpec.
    /// </summary>
    /// <param name="pluginService">The plugin service to use to update the set of allowed/blocked hosts according to the custom endpoint info.</param>
    /// <param name="customEndpointHostSpec">The host information for the custom endpoint to be monitored.</param>
    /// <param name="endpointIdentifier">An endpoint identifier.</param>
    /// <param name="region">The region of the custom endpoint to be monitored.</param>
    /// <param name="refreshRate">Controls how often the custom endpoint information should be fetched and analyzed for changes.</param>
    /// <param name="rdsClientFunc">The function to call to create the RDS client that will fetch custom endpoint information.</param>
    public CustomEndpointMonitor(
        IPluginService pluginService,
        HostSpec customEndpointHostSpec,
        string endpointIdentifier,
        RegionEndpoint region,
        TimeSpan refreshRate,
        Func<HostSpec, RegionEndpoint, AmazonRDSClient> rdsClientFunc)
    {
        this.pluginService = pluginService;
        this.customEndpointHostSpec = customEndpointHostSpec;
        this.endpointIdentifier = endpointIdentifier;
        this.region = region;
        this.refreshRate = refreshRate;
        this.rdsClient = rdsClientFunc(customEndpointHostSpec, this.region);

        this.monitorTask = Task.Run(this.RunAsync, this.cancellationTokenSource.Token);
    }

    /// <summary>
    /// Analyzes a given custom endpoint for changes to custom endpoint information.
    /// </summary>
    private async Task RunAsync()
    {
        // Logger.LogTrace(Resources.CustomEndpointMonitorImpl_StartingMonitor, this.customEndpointHostSpec.Host);

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
                            new Filter
                            {
                                Name = "db-cluster-endpoint-type",
                                Values = new List<string> { "custom" }
                            }
                        }
                    };

                    DescribeDBClusterEndpointsResponse endpointsResponse = await this.rdsClient.DescribeDBClusterEndpointsAsync(request);

                    List<DBClusterEndpoint> endpoints = endpointsResponse.DBClusterEndpoints;
                    if (endpoints.Count != 1)
                    {
                        List<string> endpointUrls = endpoints.Select(e => e.Endpoint).ToList();
                        // Logger.LogWarning(Resources.CustomEndpointMonitorImpl_UnexpectedNumberOfEndpoints,
                        //     this.endpointIdentifier,
                        //     this.region.SystemName,
                        //     endpoints.Count,
                        //     string.Join(", ", endpointUrls));

                        await Task.Delay(this.refreshRate, this.cancellationTokenSource.Token);
                        continue;
                    }

                    CustomEndpointInfo endpointInfo = CustomEndpointInfo.FromDBClusterEndpoint(endpoints[0]);
                    CustomEndpointInfoCache.TryGetValue(this.customEndpointHostSpec.Host, out CustomEndpointInfo? cachedEndpointInfo);
                    if (cachedEndpointInfo != null && cachedEndpointInfo.Equals(endpointInfo))
                    {
                        TimeSpan elapsedTime = DateTime.UtcNow - start;
                        TimeSpan sleepDuration = this.refreshRate - elapsedTime;
                        if (sleepDuration > TimeSpan.Zero)
                        {
                            await Task.Delay(sleepDuration, this.cancellationTokenSource.Token);
                        }
                        continue;
                    }

                    // Logger.LogTrace(Resources.CustomEndpointMonitorImpl_DetectedChangeInCustomEndpointInfo,
                    //     this.customEndpointHostSpec.Host,
                    //     endpointInfo);

                    // The custom endpoint info has changed, so we need to update the set of allowed/blocked hosts.
                    if (endpointInfo.MemberListType == MemberTypeList.StaticList)
                    {
                        HashSet<string>? staticMembers = endpointInfo.GetStaticMembers();
                        if (staticMembers != null)
                        {
                            // Set availability for static members as available, others as unavailable
                            this.UpdateHostAvailabilityForStaticList(staticMembers);
                        }
                    }
                    else
                    {
                        HashSet<string>? excludedMembers = endpointInfo.GetExcludedMembers();
                        if (excludedMembers != null)
                        {
                            // Set availability for excluded members as unavailable
                            this.UpdateHostAvailability(excludedMembers, HostAvailability.Unavailable);
                        }
                    }

                    CustomEndpointInfoCache[this.customEndpointHostSpec.Host] = endpointInfo;

                    TimeSpan elapsed = DateTime.UtcNow - start;
                    TimeSpan sleep = this.refreshRate - elapsed;
                    if (sleep > TimeSpan.Zero)
                    {
                        await Task.Delay(sleep, this.cancellationTokenSource.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    // If the exception is not an OperationCanceledException, log it and continue monitoring.
                    // Logger.LogError(e, Resources.CustomEndpointMonitorImpl_Exception, this.customEndpointHostSpec.Host);
                    await Task.Delay(this.refreshRate, this.cancellationTokenSource.Token);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Logger.LogTrace(Resources.CustomEndpointMonitorImpl_Interrupted, this.customEndpointHostSpec.Host);
        }
        finally
        {
            CustomEndpointInfoCache.TryRemove(this.customEndpointHostSpec.Host, out _);
            this.rdsClient.Dispose();
            // Logger.LogTrace(Resources.CustomEndpointMonitorImpl_StoppedMonitor, this.customEndpointHostSpec.Host);
        }
    }

    private void UpdateHostAvailability(HashSet<string> instanceIds, HostAvailability availability = HostAvailability.Available)
    {
        IList<HostSpec> allHosts = this.pluginService.AllHosts;
        List<HostSpec> hostsToChange = allHosts
            .Where(host => instanceIds.Contains(host.HostId ?? string.Empty))
            .Distinct()
            .ToList();

        if (hostsToChange.Count == 0)
        {
            return;
        }

        foreach (HostSpec host in hostsToChange)
        {
            host.Availability = availability;
        }
    }

    private void UpdateHostAvailabilityForStaticList(HashSet<string> staticMemberInstanceIds)
    {
        IList<HostSpec> allHosts = this.pluginService.AllHosts;

        // Set static members as available
        foreach (HostSpec host in allHosts)
        {
            if (staticMemberInstanceIds.Contains(host.HostId ?? string.Empty))
            {
                host.Availability = HostAvailability.Available;
            }
            else
            {
                // Set non-static members as unavailable
                host.Availability = HostAvailability.Unavailable;
            }
        }
    }

    public bool HasCustomEndpointInfo()
    {
        return CustomEndpointInfoCache.ContainsKey(this.customEndpointHostSpec.Host);
    }

    public bool ShouldDispose()
    {
        return true;
    }

    /// <summary>
    /// Stops the custom endpoint monitor.
    /// </summary>
    public void Dispose()
    {
        // Logger.LogTrace(Resources.CustomEndpointMonitorImpl_StoppingMonitor, this.customEndpointHostSpec.Host);

        this.cancellationTokenSource.Cancel();

        try
        {
            const int terminationTimeoutSec = 5;
            if (!this.monitorTask.Wait(TimeSpan.FromSeconds(terminationTimeoutSec)))
            {
                // Logger.LogInformation(Resources.CustomEndpointMonitorImpl_MonitorTerminationTimeout,
                //     terminationTimeoutSec,
                //     this.customEndpointHostSpec.Host);
            }
        }
        catch (Exception e)
        {
            // Logger.LogInformation(e, Resources.CustomEndpointMonitorImpl_InterruptedWhileTerminating,
                // this.customEndpointHostSpec.Host);
        }
        finally
        {
            this.cancellationTokenSource.Dispose();
            CustomEndpointInfoCache.TryRemove(this.customEndpointHostSpec.Host, out _);
            this.rdsClient.Dispose();
        }
    }

    /// <summary>
    /// Clears the shared custom endpoint information cache.
    /// </summary>
    public static void ClearCache()
    {
        // Logger.LogInformation(Resources.CustomEndpointMonitorImpl_ClearCache);
        CustomEndpointInfoCache.Clear();
    }
}
