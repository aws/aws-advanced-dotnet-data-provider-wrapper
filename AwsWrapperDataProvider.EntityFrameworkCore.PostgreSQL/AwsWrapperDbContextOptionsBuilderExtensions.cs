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

using AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Microsoft.EntityFrameworkCore;

public static class AwsWrapperDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseAwsWrapper(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<DbContextOptionsBuilder> wrappedOptionsBuilderAction)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        var wrappedOptionBuilder = new DbContextOptionsBuilder();
        wrappedOptionsBuilderAction(wrappedOptionBuilder);

        IDbContextOptionsExtension? targetOptionExtension = wrappedOptionBuilder.Options.Extensions
            .Where(x => x is not CoreOptionsExtension).FirstOrDefault();

        // Add the wrapped provider extension (e.g. NpgsqlOptionsExtension) directly —
        // it remains the real database provider and registers all Npgsql services.
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(targetOptionExtension!);

        // Add our wrapper extension (IsDatabaseProvider=false) which replaces
        // IRelationalConnection and IModificationCommandBatchFactory via ApplyServices.
        var extension = new AwsWrapperOptionsExtension(targetOptionExtension!)
        {
            WrapperConnectionString = connectionString,
        };
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseAwsWrapper<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<DbContextOptionsBuilder> wrappedOptionsBuilderAction)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseAwsWrapper((DbContextOptionsBuilder)optionsBuilder, connectionString, wrappedOptionsBuilderAction);

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsExtension = optionsBuilder.Options.FindExtension<CoreOptionsExtension>() ?? new CoreOptionsExtension();

        coreOptionsExtension = coreOptionsExtension.WithWarningsConfiguration(
            coreOptionsExtension.WarningsConfiguration.TryWithExplicit(
                RelationalEventId.AmbientTransactionWarning, WarningBehavior.Throw));

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(coreOptionsExtension);
    }
}
