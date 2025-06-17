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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.ExecutionTime;

public class ExecutionTimePlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<ExecutionTimePlugin> _logger = LoggerUtils.GetLogger<ExecutionTimePlugin>();

    public override ISet<string> SubscribedMethods { get; } = new HashSet<string>() { "*" };

    public override T Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        var sw = Stopwatch.StartNew();
        T results = base.Execute(methodInvokedOn, methodName, methodFunc, methodArgs);
        sw.Stop();

        long ticks = sw.ElapsedTicks;
        double nanoseconds = (double)ticks / Stopwatch.Frequency * 1_000_000_000;

        _logger.LogInformation($"Execution time: {ticks}ms, {nanoseconds}ns");

        return results;
    }
}
