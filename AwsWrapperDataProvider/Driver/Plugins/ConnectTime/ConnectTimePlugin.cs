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

namespace AwsWrapperDataProvider.Driver.Plugins.ConnectTime;
public class ConnectTimePlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<ConnectTimePlugin> Logger = LoggerUtils.GetLogger<ConnectTimePlugin>();

    private static double connectTime;
    public static void ReseConnectTime()
    {
        connectTime = 0;
    }

    public static double GetTotalConnectTime()
    {
        return connectTime;
    }

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>()
    {
        "DbConnection.Open",
        "DbConnection.OpenAsync",
    };

    public override async Task<DbConnection> OpenConnection(
    HostSpec? hostSpec,
    Dictionary<string, string> props,
    bool isInitialConnection,
    ADONetDelegate<DbConnection> methodFunc,
    bool async)
    {
        var sw = Stopwatch.StartNew();
        DbConnection results = await methodFunc();
        sw.Stop();

        long ticks = sw.ElapsedTicks;
        double nanoseconds = (double)ticks * 1_000_000_000.0 / Stopwatch.Frequency;

        Logger.LogInformation(Resources.ConnectTimePlugin_ConnectTime, nanoseconds);
        connectTime += nanoseconds;

        return results;
    }
}
