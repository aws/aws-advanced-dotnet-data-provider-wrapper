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

using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Utils;

public class FileLogger : ILogger
{
    private readonly string categoryName;
    private readonly string filePath;
    private readonly object lockObject;
    private readonly string logFileName = "aws-dotnet-data-provider-wrapper-log.log";

    public FileLogger(string categoryName, string directory, object lockObject)
    {
        this.categoryName = categoryName;
        this.filePath = Path.Combine(Path.GetFullPath(directory), this.logFileName);
        this.lockObject = lockObject;
        this.EnsureDirectoryAndFileExists();
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull
    {
        return null;
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!this.IsEnabled(logLevel))
        {
            return;
        }

        var message = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} [{logLevel}] {this.categoryName}: {formatter(state, exception)}";
        if (exception != null)
        {
            message += Environment.NewLine + exception;
        }

        lock (this.lockObject)
        {
            File.AppendAllText(this.filePath, message + Environment.NewLine);
        }
    }

    private void EnsureDirectoryAndFileExists()
    {
        lock (this.lockObject)
        {
            var directory = Path.GetDirectoryName(this.filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            if (!File.Exists(this.filePath))
            {
                File.Create(this.filePath).Dispose();
            }
        }
    }
}
