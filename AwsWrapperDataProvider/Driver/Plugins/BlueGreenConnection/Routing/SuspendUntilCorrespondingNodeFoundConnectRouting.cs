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

public class SuspendUntilCorrespondingNodeFoundConnectRouting : BaseConnectRouting
{
    private const long SleepTimeMs = 100L;

    private static readonly ILogger<SuspendUntilCorrespondingNodeFoundConnectRouting> Logger = LoggerUtils.GetLogger<SuspendUntilCorrespondingNodeFoundConnectRouting>();

    private readonly string bgdId;

    public SuspendUntilCorrespondingNodeFoundConnectRouting(string? hostAndPort, BlueGreenRoleType? role, string bgdId)
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
        if (hostSpec == null)
        {
            return null;
        }

        Logger.LogTrace(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_WaitConnectUntilCorrespondingNodeFound, hostSpec.Host);

        var bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
        var correspondingPair = bgStatus?.CorrespondingNodes.GetValueOrDefault(hostSpec.Host);

        long? timeoutMs = PropertyDefinition.BgConnectTimeout.GetLong(props) ?? long.Parse(PropertyDefinition.BgConnectTimeout.DefaultValue!);
        var stopwatch = Stopwatch.StartNew();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds((double)timeoutMs!));

        try
        {
            while (stopwatch.ElapsedMilliseconds <= timeoutMs
                   && bgStatus != null
                   && bgStatus.CurrentPhase != BlueGreenPhaseType.COMPLETED
                   && (correspondingPair?.Green == null)
                   && !cts.Token.IsCancellationRequested)
            {
                await this.Delay(SleepTimeMs, bgStatus, this.bgdId, cts.Token);

                bgStatus = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(this.bgdId);
                correspondingPair = bgStatus?.CorrespondingNodes.GetValueOrDefault(hostSpec.Host);
            }
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(string.Format(
                Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CorrespondingNodeNotFoundTryConnectLater, hostSpec.Host, timeoutMs));
        }

        if (bgStatus == null || bgStatus.CurrentPhase == BlueGreenPhaseType.COMPLETED)
        {
            Logger.LogTrace(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CompletedContinueWithConnect, stopwatch.ElapsedMilliseconds);
            return null;
        }

        if (stopwatch.ElapsedMilliseconds > timeoutMs || cts.Token.IsCancellationRequested)
        {
            throw new TimeoutException(string.Format(
                Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CorrespondingNodeNotFoundTryConnectLater, hostSpec.Host, timeoutMs));
        }

        Logger.LogTrace(Resources.SuspendUntilCorrespondingNodeFoundConnectRouting_Apply_CorrespondingNodeFoundContinueWithConnect, hostSpec.Host, stopwatch.ElapsedMilliseconds);

        return null;
    }
}
