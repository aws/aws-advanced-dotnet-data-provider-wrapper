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

public class SuspendUntilCorrespondingNodeFoundConnectRouting : BaseConnectRouting
{
    private static readonly ILogger<SuspendUntilCorrespondingNodeFoundConnectRouting> Logger = 
        LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<SuspendUntilCorrespondingNodeFoundConnectRouting>();

    private const long SleepTimeMs = 100L;

    protected string BgdId;

    public SuspendUntilCorrespondingNodeFoundConnectRouting(string? hostAndPort, BlueGreenRoleType? role, string bgdId)
        : base(hostAndPort, role)
    {
        BgdId = bgdId;
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
        Logger.LogTrace(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_WaitConnectUntilCorrespondingNodeFound, hostSpec.Host);

        var bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.BgdId);
        var correspondingPair = bgStatus?.CorrespondingNodes.GetValueOrDefault(hostSpec.Host);

        long? timeoutMs = BlueGreenConnectionPlugin.BgConnectTimeout.GetLong(props);
        long holdStartTime = this.GetNanoTime();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds((double)timeoutMs!));

        try
        {
            while (bgStatus != null
                   && bgStatus.CurrentPhase != BlueGreenPhaseType.COMPLETED
                   && (correspondingPair == null))
            {
                this.Delay(SleepTimeMs, bgStatus, this.BgdId, cts.Token);

                bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.BgdId);
                correspondingPair = bgStatus?.CorrespondingNodes.GetValueOrDefault(hostSpec.Host);
            }

            if (bgStatus == null || bgStatus.CurrentPhase == BlueGreenPhaseType.COMPLETED)
            {
                Logger.LogTrace(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CompletedContinueWithConnect, (this.GetNanoTime() - holdStartTime) / 1_000_000);
                return null!;
            }

            Logger.LogTrace(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CorrespondingNodeFoundContinueWithConnect, hostSpec.Host, (this.GetNanoTime() - holdStartTime) / 1_000_000);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(
                string.Format(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CorrespondingNodeNotFoundTryConnectLater, hostSpec.Host, timeoutMs));
        }

        return null!;
    }
}
