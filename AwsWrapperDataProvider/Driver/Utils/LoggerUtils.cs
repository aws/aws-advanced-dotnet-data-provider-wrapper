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
    private static readonly bool EnabledFileLog = bool.TryParse(Environment.GetEnvironmentVariable("ENABLED_FILE_LOG"), out var result) && result;
    private static readonly string LogPath = Environment.GetEnvironmentVariable("LOG_DIRECTORY_PATH") ?? "./";
    private static readonly string MinimumLogLevel = Environment.GetEnvironmentVariable("LOG_LEVEL") ?? "Trace";
    private static ILoggerProvider loggerProvider = new FileLoggerProvider(LogPath);
    private static ILoggerFactory loggerFactory = CreateLoggerFactory();

    private static ILoggerFactory CreateLoggerFactory()
    {
        return LoggerFactory.Create(builder =>
        {
            LogLevel logLevel = Enum.TryParse<LogLevel>(MinimumLogLevel, true, out var result) ? result : LogLevel.Trace;

            builder
                .SetMinimumLevel(logLevel)
                .AddDebug()
                .AddConsole(options => options.FormatterName = "simple");

            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.UseUtcTimestamp = true;
                options.ColorBehavior = LoggerColorBehavior.Enabled;
            });

            if (EnabledFileLog)
            {
                builder.AddProvider(loggerProvider);
            }
        });
    }

    public static ILogger<T> GetLogger<T>() => loggerFactory.CreateLogger<T>();

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

    public static void SetCustomLoggerProvider<T>(T loggerProviderType) where T : ILoggerProvider
    {
        loggerProvider = loggerProviderType;
        loggerFactory.Dispose();
        loggerFactory = CreateLoggerFactory();
    }
}
