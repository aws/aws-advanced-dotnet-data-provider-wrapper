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

using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver.HostInfo;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace AwsWrapperDataProvider.Driver.Utils;

public static class LoggerUtils
{
    private static readonly ILoggerFactory LoggerFactory;

    static LoggerUtils()
    {
        LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder
            .SetMinimumLevel(LogLevel.Trace)
            .AddDebug()
            .AddConsole(options => options.FormatterName = "simple");

            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.UseUtcTimestamp = true;
                options.ColorBehavior = LoggerColorBehavior.Enabled;
            });
        });
    }

    public static ILogger<T> GetLogger<T>() => LoggerFactory.CreateLogger<T>();

    public static string LogTopology(IList<HostSpec>? hosts, string? messagePrefix)
    {
        if (hosts == null)
        {
            return $"{messagePrefix} Topology is null";
        }

        var topology = string.Join($"{Environment.NewLine}    ", hosts.Select(h => h.ToString()));
        return $"{messagePrefix} Topology@{RuntimeHelpers.GetHashCode(hosts)}{Environment.NewLine}    {topology}";
    }

    public static void MonitoringLogWithHost(HostSpec hostSpec, ILogger logger, LogLevel level, string message, params object?[] args)
    {
        using (logger.BeginScope("Monitoring node: {}", hostSpec))
        {
            logger.Log(level, message, args);
        }
    }

    public static void MonitoringLogWithHost(HostSpec hostSpec, ILogger logger, LogLevel level, Exception ex, string message, params object?[] args)
    {
        using (logger.BeginScope("Monitoring node: {}", hostSpec))
        {
            logger.Log(level, ex, message, args);
        }
    }
}
