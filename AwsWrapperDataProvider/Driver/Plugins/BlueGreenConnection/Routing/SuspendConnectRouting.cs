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
using System.Diagnostics;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection.Routing;

public class SuspendConnectRouting : BaseConnectRouting
{
    private const long SleepTimeMs = 100L;

    private static readonly ILogger<SuspendConnectRouting> Logger = LoggerUtils.GetLogger<SuspendConnectRouting>();

    private readonly string bgdId;

    public SuspendConnectRouting(string? hostAndPort, BlueGreenRoleType? role, string bgdId)
        : base(hostAndPort, role)
    {
        this.bgdId = bgdId;
    }

    public override async Task<DbConnection?> Apply(
        IConnectionPlugin plugin,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool useForceConnect,
        ADONetDelegate<DbConnection> connectFunc,
        IPluginService pluginService)
    {
        Logger.LogTrace(Resources.SuspendConnectRouting_Apply_InProgressHoldConnect);

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
            throw new TimeoutException(string.Format(Resources.SuspendConnectRouting_Apply_InProgressTryConnectLater, timeoutMs));
        }

        if (bgStatus is { CurrentPhase: BlueGreenPhaseType.IN_PROGRESS })
        {
            throw new TimeoutException(string.Format(Resources.SuspendConnectRouting_Apply_InProgressTryConnectLater, timeoutMs));
        }

        Logger.LogTrace(Resources.SuspendConnectRouting_Apply_SwitchoverCompleteContinueWithConnect, stopwatch.ElapsedMilliseconds);

        return null;
    }
}
