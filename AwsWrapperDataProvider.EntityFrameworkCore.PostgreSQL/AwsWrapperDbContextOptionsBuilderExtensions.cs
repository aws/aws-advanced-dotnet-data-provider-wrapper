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

        IDbContextOptionsExtension? targetOptionExtension = wrappedOptionBuilder.Options.Extensions.Where(x => x is not CoreOptionsExtension).FirstOrDefault();

        // Extract the clean connection string from the wrapped extension (e.g. NpgsqlOptionsExtension)
        // so that the base RelationalOptionsExtension only sees provider-understood properties.
        // The full wrapper connection string (with Plugins=... etc.) is stored separately.
        var wrappedRelationalExtension = targetOptionExtension as RelationalOptionsExtension;
        var cleanConnectionString = wrappedRelationalExtension?.ConnectionString ?? connectionString;

        var extension = GetOrCreateExtension(optionsBuilder, targetOptionExtension!);
        extension.WrapperConnectionString = connectionString;
        extension = (AwsWrapperOptionsExtension)extension.WithConnectionString(cleanConnectionString);
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

    /// <param name="optionsBuilder">The <see cref="DbContextOptionsBuilder"/> to search.</param>
    /// <param name="targetOptionExtension">The target <see cref="IDbContextOptionsExtension"/> to wrap.</param>
    /// <returns>
    /// An existing instance of <see cref="AwsWrapperOptionsExtension"/>, or a new instance if one does not exist.
    /// </returns>
    private static AwsWrapperOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder, IDbContextOptionsExtension targetOptionExtension)
    {
        if (optionsBuilder.Options.FindExtension<AwsWrapperOptionsExtension>() is AwsWrapperOptionsExtension existing)
        {
            var extension = new AwsWrapperOptionsExtension(existing)
            {
                WrappedExtension = targetOptionExtension,
            };
            return extension;
        }
        else
        {
            return new AwsWrapperOptionsExtension(targetOptionExtension);
        }
    }

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsExtension = optionsBuilder.Options.FindExtension<CoreOptionsExtension>() ?? new CoreOptionsExtension();

        coreOptionsExtension = coreOptionsExtension.WithWarningsConfiguration(
            coreOptionsExtension.WarningsConfiguration.TryWithExplicit(
                RelationalEventId.AmbientTransactionWarning, WarningBehavior.Throw));

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(coreOptionsExtension);
    }
}
