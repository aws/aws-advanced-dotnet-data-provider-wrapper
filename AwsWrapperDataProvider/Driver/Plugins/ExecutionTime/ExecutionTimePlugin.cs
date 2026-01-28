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

namespace AwsWrapperDataProvider.Driver.Plugins.ExecutionTime;

public class ExecutionTimePlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<ExecutionTimePlugin> Logger = LoggerUtils.GetLogger<ExecutionTimePlugin>();

    private static double executionTime;

    public static void ResetExecutionTime()
    {
        executionTime = 0;
    }

    public static double GetTotalExecutionTime()
    {
        return executionTime;
    }

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>() { "*" };

    public override async Task<T> Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        var sw = Stopwatch.StartNew();
        T results = await base.Execute(methodInvokedOn, methodName, methodFunc, methodArgs);
        sw.Stop();

        long ticks = sw.ElapsedTicks;
        double nanoseconds = (double)ticks * 1_000_000_000.0 / Stopwatch.Frequency;

        Logger.LogInformation(Resources.ExecutionTimePlugin_Execute_ExecutionTime, ticks, nanoseconds);
        executionTime += nanoseconds;

        return results;
    }
}
