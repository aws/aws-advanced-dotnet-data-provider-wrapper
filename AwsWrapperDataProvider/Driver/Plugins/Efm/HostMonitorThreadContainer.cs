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

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitorThreadContainer
{
    private static readonly Mutex LockObject = new();
    private static readonly Mutex MonitorLockObject = new();
    private static HostMonitorThreadContainer? singleton = null;
    private readonly ConcurrentDictionary<IHostMonitor, CancellationTokenSource> ctsMap = new();
    private readonly ConcurrentDictionary<IHostMonitor, Task> tasksMap = new();
    private readonly ConcurrentDictionary<string, IHostMonitor> monitorMap = new();

    public static HostMonitorThreadContainer GetInstance()
    {
        if (singleton != null)
        {
            return singleton;
        }

        LockObject.WaitOne();
        try
        {
            singleton = new HostMonitorThreadContainer();
            LockObject.ReleaseMutex();
            return singleton;
        }
        catch (Exception ex)
        {
            LockObject.ReleaseMutex();
            throw new Exception("Could not create singleton HostMonitorThreadContainer", ex);
        }
    }

    public static void ReleaseInstance()
    {
        if (singleton == null)
        {
            return;
        }

        LockObject.WaitOne();
        try
        {
            singleton.ReleaseResources();
            singleton = null;
        }
        catch (Exception ex)
        {
            // previous singleton should not be reused; something bad happened
            singleton = null;
            throw new Exception("Could not release resources for HostMonitorThreadContainer singleton.", ex);
        }
        finally
        {
            LockObject.ReleaseMutex();
        }
    }

    private HostMonitorThreadContainer()
    {
    }

    public ConcurrentDictionary<string, IHostMonitor> GetMonitorMap()
    {
        return this.monitorMap;
    }

    public ConcurrentDictionary<IHostMonitor, Task> GetTasksMap()
    {
        return this.tasksMap;
    }

    public IHostMonitor? GetMonitor(string node)
    {
        return this.monitorMap.TryGetValue(node, out IHostMonitor? monitor) ? monitor : null;
    }

    public IHostMonitor GetOrCreateMonitor(string[] nodeKeys, Func<IHostMonitor> createMonitor)
    {
        if (nodeKeys.Length == 0)
        {
            throw new Exception("nodeKeys must not be empty.");
        }

        MonitorLockObject.WaitOne();
        try
        {
            IHostMonitor? monitor = null;

            foreach (string nodeKey in nodeKeys)
            {
                if (this.monitorMap.TryGetValue(nodeKey, out monitor))
                {
                    break;
                }
            }

            if (monitor == null)
            {
                monitor = createMonitor();
                this.AddTask(monitor);
            }

            this.PopulateMonitorMap(nodeKeys, monitor);
            return monitor;
        }
        catch (Exception ex)
        {
            throw new Exception("Unable to get or create monitor.", ex);
        }
        finally
        {
            MonitorLockObject.ReleaseMutex();
        }
    }

    public void ReleaseResource(IHostMonitor monitor)
    {
        MonitorLockObject.WaitOne();
        try
        {
            if (this.tasksMap.TryRemove(monitor, out Task? task))
            {
                // signal for monitor to cancel
                if (this.ctsMap.TryRemove(monitor, out CancellationTokenSource? cts))
                {
                    cts.Cancel();
                }
                else
                {
                    // this shouldn't happen; but maybe log if it does?
                }

                task.Wait();
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Could not release resource.", ex);
        }
        finally
        {
            MonitorLockObject.ReleaseMutex();
        }
    }

    public void ReleaseResources()
    {
        MonitorLockObject.WaitOne();
        try
        {
            foreach (IHostMonitor monitor in this.monitorMap.Values)
            {
                // will release cancellation token source and task
                this.ReleaseResource(monitor);
            }

            // this should be empty, but just incase, cancel any remaining cts
            foreach (CancellationTokenSource cts in this.ctsMap.Values)
            {
                cts.Cancel();
            }

            this.ctsMap.Clear();
            this.tasksMap.Clear();
            this.monitorMap.Clear();
        }
        catch (Exception ex)
        {
            throw new Exception("Could not release resources.", ex);
        }
        finally
        {
            MonitorLockObject.ReleaseMutex();
        }
    }

    private void AddTask(IHostMonitor monitor)
    {
        CancellationTokenSource cts = new CancellationTokenSource();
        this.ctsMap[monitor] = cts;

        Task task = Task.Run(() => monitor.Run(cts.Token));
        this.tasksMap[monitor] = task;
    }

    private void PopulateMonitorMap(string[] nodeKeys, IHostMonitor monitor)
    {
        foreach (string nodeKey in nodeKeys)
        {
            this.monitorMap.TryAdd(nodeKey, monitor);
        }
    }
}
