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

using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests;

/// <summary>
/// MySQL (EF Core 9 / Pomelo) binding for the shared EF integration tests.
///
/// The shared test sources are compiled into two assemblies — the PostgreSQL one
/// (EF Core 10) and this one (MySQL, EF Core 9) — because Pomelo has no EF Core 10 release
/// and the two EF Core majors cannot coexist in a single assembly. This file is the MySQL
/// counterpart of the PostgreSQL assembly's <see cref="EngineTestConfig"/>; it has the same
/// shape so the linked shared sources compile unchanged against either provider.
/// <see cref="ThisEngine"/> lets the shared base skip tests selected for the other engine
/// (the CI <c>--filter</c> runs solution-wide across both assemblies).
/// </summary>
internal static class EngineTestConfig
{
    /// <summary>The database engine this test assembly is wired for.</summary>
    public const DatabaseEngine ThisEngine = DatabaseEngine.MYSQL;

    /// <summary>Loads the dialect this assembly's engine requires.</summary>
    public static void LoadDialect() => MySqlConnectorDialectLoader.Load();

    /// <summary>Returns the ADO connection string for this assembly's engine.</summary>
    public static string GetConnectionString() => EFUtils.GetMySqlConnectionString();

    /// <summary>Builds wrapper-backed options for this assembly's engine.</summary>
    public static DbContextOptions<PersonDbContext> BuildPersonOptions(
        string wrapperConnectionString, string connectionString)
        => new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapperMySql(
                wrapperConnectionString,
                wrapped => wrapped.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)))
            .LogTo(Console.WriteLine)
            .Options;

    /// <summary>Builds wrapper-backed options with the supplied logger factory.</summary>
    public static DbContextOptions<PersonDbContext> BuildPersonOptionsWithLogger(
        string wrapperConnectionString, string connectionString, Microsoft.Extensions.Logging.ILoggerFactory loggerFactory)
        => new DbContextOptionsBuilder<PersonDbContext>()
            .UseLoggerFactory(loggerFactory)
            .UseAwsWrapperMySql(
                wrapperConnectionString,
                wrapped => wrapped
                    .UseLoggerFactory(loggerFactory)
                    .UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)))
            .Options;

    /// <summary>Builds design-time options used by <see cref="PersonDbContextFactory"/>.</summary>
    public static DbContextOptions<PersonDbContext> BuildDesignTimeOptions(string connectionString)
        => new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapperMySql(
                connectionString,
                wrapped => wrapped.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)))
            .LogTo(Console.WriteLine, Microsoft.Extensions.Logging.LogLevel.Trace)
            .Options;
}
