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

using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests;
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

    /// <summary>
    /// Skips the current test when the runtime engine does not match the engine this test
    /// assembly is wired for. The shared EF test sources are linked into both the PostgreSQL
    /// (EF Core 10) and MySQL (EF Core 9) assemblies, and CI runs <c>dotnet test --filter</c>
    /// solution-wide, so each assembly is asked to run the other engine's selection too. The
    /// non-matching assembly skips, which counts as a pass.
    /// </summary>
    protected void SkipIfNotThisEngine()
        => Assert.SkipUnless(
            Engine == EngineTestConfig.ThisEngine,
            $"Test selected for {Engine}; this assembly is wired for {EngineTestConfig.ThisEngine}.");

    protected DbContextOptions<PersonDbContext> BuildOptions(
        string wrapperConnectionString, string connectionString)
    {
        this.SkipIfNotThisEngine();
        return EngineTestConfig.BuildPersonOptions(wrapperConnectionString, connectionString);
    }

    protected DbContextOptions<PersonDbContext> BuildOptionsWithLogger(
        string wrapperConnectionString, string connectionString)
    {
        this.SkipIfNotThisEngine();
        return EngineTestConfig.BuildPersonOptionsWithLogger(wrapperConnectionString, connectionString, this.LoggerFactory);
    }

    /// <summary>
    /// Asserts that executing <paramref name="action"/> surfaces a successful-failover signal.
    /// Failover that completes successfully throws <see cref="FailoverSuccessException"/> when no transaction
    /// is in progress, or <see cref="TransactionStateUnknownException"/> when it interrupts an in-flight
    /// transaction (for example a Pomelo MySQL <c>SaveChanges</c>, which wraps the write in an explicit
    /// transaction). EF Core also wraps exceptions thrown during <c>SaveChanges</c> in a
    /// <see cref="DbUpdateException"/>, so the signal may arrive directly or nested in an outer exception.
    /// This helper accepts all of those shapes and walks the inner-exception chain.
    /// </summary>
    protected static async Task AssertFailoverSuccessAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            Assert.True(
                ContainsFailoverSuccess(ex),
                $"Expected a {nameof(FailoverSuccessException)} or {nameof(TransactionStateUnknownException)} "
                + $"(possibly wrapped by EF), but got: {ex}");
            return;
        }

        Assert.Fail(
            $"Expected a {nameof(FailoverSuccessException)} or {nameof(TransactionStateUnknownException)} "
            + "but no exception was thrown.");
    }

    private static bool ContainsFailoverSuccess(Exception? exception)
    {
        while (exception != null)
        {
            // Both indicate failover completed successfully; TransactionStateUnknownException is raised
            // instead of FailoverSuccessException when failover interrupted an in-flight transaction.
            if (exception is FailoverSuccessException or TransactionStateUnknownException)
            {
                return true;
            }

            exception = exception.InnerException;
        }

        return false;
    }
}
