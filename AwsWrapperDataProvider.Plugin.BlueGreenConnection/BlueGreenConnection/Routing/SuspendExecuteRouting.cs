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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Properties;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Routing;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Routing;

public class SuspendExecuteRouting : BaseExecuteRouting
{
    private static readonly ILogger<SuspendExecuteRouting> Logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<SuspendExecuteRouting>();

    private readonly long sleepTimeMs = 100L;

    protected string BgdId;

    public SuspendExecuteRouting(string? hostAndPort, BlueGreenRoleType? role, string bgdId)
        : base(hostAndPort, role)
    {
        this.BgdId = bgdId;
    }

    public override Task<T?> Apply<T>(
        IConnectionPlugin plugin,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> jdbcMethodFunc,
        object[] jdbcMethodArgs,
        IPluginService pluginService,
        Dictionary<string, string> props) where T : default
    {
        Logger.LogTrace(Resources.SuspendExecuteRouting_Apply_InProgressSuspendMethod, methodName);

        var bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.BgdId);

        long? timeoutMs = BlueGreenConnectionPlugin.BgConnectTimeout.GetLong(props);
        long holdStartTime = this.GetNanoTime();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds((double)timeoutMs!));

        try
        {
            while (bgStatus is { CurrentPhase: BlueGreenPhaseType.IN_PROGRESS })
            {
                this.Delay(this.sleepTimeMs, bgStatus, this.BgdId, cts.Token);
                bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.BgdId);
            }

            if (bgStatus is { CurrentPhase: BlueGreenPhaseType.IN_PROGRESS })
            {
                throw new TimeoutException(string.Format(Resources.SuspendExecuteRouting_Apply_StillInProgressTryMethodLater, timeoutMs, methodName));
            }

            Logger.LogTrace(Resources.SuspendExecuteRouting_Apply_SwitchoverCompletedContinueWithMethod, methodName, (this.GetNanoTime() - holdStartTime) / 1_000_000);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(string.Format(Resources.SuspendExecuteRouting_Apply_StillInProgressTryMethodLater, timeoutMs, methodName));
        }

        return Task.FromResult<T?>(default);
    }
}
