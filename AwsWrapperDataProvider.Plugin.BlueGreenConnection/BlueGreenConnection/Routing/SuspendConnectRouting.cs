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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.BlueGreenConnection.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Routing;

public class SuspendConnectRouting : BaseConnectRouting
{
    private static readonly ILogger<SuspendConnectRouting> Logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<SuspendConnectRouting>();

    private readonly long sleepTimeMs = 100L;

    protected string BgdId;

    public SuspendConnectRouting(string? hostAndPort, BlueGreenRoleType? role, string bgdId)
        : base(hostAndPort, role)
    {
        this.BgdId = bgdId;
    }

    public override async Task<DbConnection> Apply(
        IConnectionPlugin plugin,
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        bool useForceConnect,
        ADONetDelegate<DbConnection> connectFunc,
        IPluginService pluginService)
    {
        Logger.LogTrace(Resources.SuspendConnectRouting_Apply_InProgressHoldConnect);

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
                throw new TimeoutException(
                    string.Format(Resources.SuspendConnectRouting_Apply_InProgressTryConnectLater, timeoutMs));
            }

            Logger.LogTrace(Resources.SuspendConnectRouting_Apply_SwitchoverCompleteContinueWithConnect, (this.GetNanoTime() - holdStartTime) / 1_000_000);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(
                string.Format(Resources.SuspendConnectRouting_Apply_InProgressTryConnectLater, timeoutMs));
        }


        return null!;
    }
}
