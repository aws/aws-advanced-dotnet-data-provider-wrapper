using AwsWrapperDataProvider;
using AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System.Diagnostics;

namespace Microsoft.EntityFrameworkCore;

public static class AwsWrapperDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseAwsWrapper(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<DbContextOptionsBuilder> wrappedOptionsBuilderAction)
    {
        if (optionsBuilder == null)
        {
            throw new ArgumentNullException(nameof(optionsBuilder));
        }
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        var wrappedOptionBuilder = new DbContextOptionsBuilder();
        wrappedOptionsBuilderAction(wrappedOptionBuilder);

        IDbContextOptionsExtension? targetOptionExtension = wrappedOptionBuilder.Options.Extensions.Where(x => x is not CoreOptionsExtension).FirstOrDefault();
        Debug.Assert(targetOptionExtension != null);

        var extension = (AwsWrapperOptionsExtension)GetOrCreateExtension(optionsBuilder, targetOptionExtension).WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        return optionsBuilder;
    }

    /// <summary>
    /// Returns an existing instance of <see cref="NpgsqlOptionsExtension"/>, or a new instance if one does not exist.
    /// </summary>
    /// <param name="optionsBuilder">The <see cref="DbContextOptionsBuilder"/> to search.</param>
    /// <returns>
    /// An existing instance of <see cref="NpgsqlOptionsExtension"/>, or a new instance if one does not exist.
    /// </returns>
    private static AwsWrapperOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder, IDbContextOptionsExtension targetOptionExtension)
    {
        if (optionsBuilder.Options.FindExtension<AwsWrapperOptionsExtension>() is AwsWrapperOptionsExtension existing)
        {
            var extension = new AwsWrapperOptionsExtension(existing);
            extension.WrappedExtension = targetOptionExtension;
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
