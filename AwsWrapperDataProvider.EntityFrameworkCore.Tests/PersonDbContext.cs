using Microsoft.EntityFrameworkCore;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests
{
    public class PersonDbContext : DbContext
    {
        public DbSet<Person> Persons { get; set; }

        //protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        //    => optionsBuilder.UseNpgsql(
        //        "Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=test;",
        //        builder => builder.CommandTimeout(100));

        //#pragma warning disable EF1001 // Internal EF Core API usage.
        //        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => 
        //            optionsBuilder
        //                .UseNpgsql()
        //                .UseAwsWrapper<INpgsqlRelationalConnection>("Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=test;");
        //#pragma warning restore EF1001 // Internal EF Core API usage.

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseAwsWrapper(
                    "Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=test;",
                    wrappedOptionBuilder => wrappedOptionBuilder.UseNpgsql());
    }
}
