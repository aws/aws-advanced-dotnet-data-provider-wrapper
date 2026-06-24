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

using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests;

/// <summary>
/// PostgreSQL (EF Core 10) binding for the shared EF integration tests.
///
/// The shared test sources are compiled into two assemblies — this one (PostgreSQL,
/// EF Core 10) and the MySQL one (EF Core 9) — because Pomelo has no EF Core 10 release
/// and the two EF Core majors cannot coexist in a single assembly. Each assembly supplies
/// its own <see cref="EngineTestConfig"/> with the same shape; the linked shared sources
/// call into it without referencing any provider-specific <c>UseAwsWrapper*</c> method
/// directly. <see cref="ThisEngine"/> lets the shared base skip tests selected for the
/// other engine (the CI <c>--filter</c> runs solution-wide across both assemblies).
/// </summary>
internal static class EngineTestConfig
{
    /// <summary>The database engine this test assembly is wired for.</summary>
    public const DatabaseEngine ThisEngine = DatabaseEngine.PG;

    /// <summary>Loads the dialect this assembly's engine requires.</summary>
    public static void LoadDialect() => NpgsqlDialectLoader.Load();

    /// <summary>Returns the ADO connection string for this assembly's engine.</summary>
    public static string GetConnectionString() => EFUtils.GetNpgsqlConnectionString();

    /// <summary>Builds wrapper-backed options for this assembly's engine.</summary>
    public static DbContextOptions<PersonDbContext> BuildPersonOptions(
        string wrapperConnectionString, string connectionString)
        => new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrapped => wrapped.UseNpgsql(connectionString))
            .LogTo(Console.WriteLine)
            .Options;

    /// <summary>Builds wrapper-backed options with the supplied logger factory.</summary>
    public static DbContextOptions<PersonDbContext> BuildPersonOptionsWithLogger(
        string wrapperConnectionString, string connectionString, Microsoft.Extensions.Logging.ILoggerFactory loggerFactory)
        => new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(loggerFactory)
            .UseAwsWrapperNpgsql(
                wrapperConnectionString,
                wrapped => wrapped
                    .UseLoggerFactory(loggerFactory)
                    .UseNpgsql(connectionString))
            .Options;

    /// <summary>Builds design-time options used by <see cref="PersonDbContextFactory"/>.</summary>
    public static DbContextOptions<PersonDbContext> BuildDesignTimeOptions(string connectionString)
        => new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapperNpgsql(
                connectionString,
                wrapped => wrapped.UseNpgsql(connectionString))
            .LogTo(Console.WriteLine, Microsoft.Extensions.Logging.LogLevel.Trace)
            .Options;
}
