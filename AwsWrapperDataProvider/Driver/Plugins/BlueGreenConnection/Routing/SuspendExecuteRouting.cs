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

using System.Diagnostics;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection.Routing;

public class SuspendExecuteRouting : BaseExecuteRouting
{
    private const long SleepTimeMs = 100L;

    private static readonly ILogger<SuspendExecuteRouting> Logger = LoggerUtils.GetLogger<SuspendExecuteRouting>();

    protected string bgdId;

    public SuspendExecuteRouting(string? hostAndPort, BlueGreenRoleType? role, string bgdId)
        : base(hostAndPort, role)
    {
        this.bgdId = bgdId;
    }

    public override async Task<T?> Apply<T>(
        IConnectionPlugin plugin,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> jdbcMethodFunc,
        object[] jdbcMethodArgs,
        IPluginService pluginService,
        Dictionary<string, string> props) where T : default
    {
        Logger.LogTrace(Resources.SuspendExecuteRouting_Apply_InProgressSuspendMethod, methodName);

        var bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);

        long? timeoutMs = PropertyDefinition.BgConnectTimeout.GetLong(props);
        Stopwatch stopwatch = Stopwatch.StartNew();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds((double)timeoutMs!));

        try
        {
            while (stopwatch.ElapsedMilliseconds <= timeoutMs
                   && bgStatus is { CurrentPhase: BlueGreenPhaseType.IN_PROGRESS }
                   && !cts.Token.IsCancellationRequested)
            {
                await this.Delay(SleepTimeMs, bgStatus, this.bgdId, cts.Token);
                bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
            }
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(string.Format(Resources.SuspendExecuteRouting_Apply_SwitchoverCompletedContinueWithMethod, methodName, stopwatch.ElapsedMilliseconds));
        }

        if (bgStatus is { CurrentPhase: BlueGreenPhaseType.IN_PROGRESS })
        {
            throw new TimeoutException(string.Format(Resources.SuspendExecuteRouting_Apply_StillInProgressTryMethodLater, timeoutMs, methodName));
        }

        Logger.LogTrace(Resources.SuspendExecuteRouting_Apply_SwitchoverCompletedContinueWithMethod, methodName, stopwatch.ElapsedMilliseconds);

        return default;
    }
}
