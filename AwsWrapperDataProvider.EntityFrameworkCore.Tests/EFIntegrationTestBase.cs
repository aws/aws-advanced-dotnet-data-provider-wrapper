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

using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests;

/// <summary>
/// Shared base class for EF Core integration tests.
/// Provides common logger setup and engine-branching DbContextOptions builders.
/// </summary>
public abstract class EFIntegrationTestBase : IntegrationTestBase
{
    protected readonly ITestOutputHelper Logger;
    protected readonly ILoggerFactory LoggerFactory;

    protected EFIntegrationTestBase(ITestOutputHelper output)
    {
        this.Logger = output;

        this.LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
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

    protected DbContextOptions<PersonDbContext> BuildOptions(
        string wrapperConnectionString, string connectionString)
    {
        if (Engine == DatabaseEngine.PG)
        {
            return new DbContextOptionsBuilder<PersonDbContext>()
                .UseAwsWrapperNpgsql(
                    wrapperConnectionString,
                    wrappedOptionBuilder => wrappedOptionBuilder.UseNpgsql(connectionString))
                .LogTo(Console.WriteLine)
                .Options;
        }

        if (Engine == DatabaseEngine.MYSQL)
        {
            return new DbContextOptionsBuilder<PersonDbContext>()
                .UseAwsWrapperMySql(
                    wrapperConnectionString,
                    wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)))
                .LogTo(Console.WriteLine)
                .Options;
        }

        throw new InvalidOperationException($"Unsupported engine {Engine}");
    }

    protected DbContextOptions<PersonDbContext> BuildOptionsWithLogger(
        string wrapperConnectionString, string connectionString)
    {
        if (Engine == DatabaseEngine.PG)
        {
            return new DbContextOptionsBuilder<PersonDbContext>()
                .UseLoggerFactory(this.LoggerFactory)
                .UseAwsWrapperNpgsql(
                    wrapperConnectionString,
                    wrappedOptionBuilder => wrappedOptionBuilder
                        .UseLoggerFactory(this.LoggerFactory)
                        .UseNpgsql(connectionString))
                .Options;
        }

        if (Engine == DatabaseEngine.MYSQL)
        {
            return new DbContextOptionsBuilder<PersonDbContext>()
                .UseLoggerFactory(this.LoggerFactory)
                .UseAwsWrapperMySql(
                    wrapperConnectionString,
                    wrappedOptionBuilder => wrappedOptionBuilder
                        .UseLoggerFactory(this.LoggerFactory)
                        .UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)))
                .Options;
        }

        throw new InvalidOperationException($"Unsupported engine {Engine}");
    }
}
