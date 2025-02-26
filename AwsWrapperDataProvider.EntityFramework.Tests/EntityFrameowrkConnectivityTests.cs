using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace AwsWrapperDataProvider.EntityFramework.Tests
{
    public class EntityFrameowrkConnectivityTests
    {
        [Fact]
        public void PgConnectivityTest() 
        {
            using(var db = new PersonDbContext()) 
            {
                Person person = new Person() { FirstName = "Jane", LastName = "Smith" };
                db.Add(person);
                db.SaveChanges();
            }

            using (var db = new PersonDbContext())
            {
                foreach(Person p in db.Persons.Where(x => x.LastName != null && x.LastName.StartsWith("S")))
                {
                    Console.WriteLine($"{p.Id}: {p.FirstName} {p.LastName}");
                }
            }
        }
    }
}
