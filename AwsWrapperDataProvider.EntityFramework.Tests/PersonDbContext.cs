using Microsoft.EntityFrameworkCore;

namespace AwsWrapperDataProvider.EntityFramework.Tests
{
    public class PersonDbContext : DbContext
    {
        public DbSet<Person> Persons { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseNpgsql("Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=test;");
    }
}
