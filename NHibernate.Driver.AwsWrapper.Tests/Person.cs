using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.Driver.AwsWrapper.Tests
{
    public class Person
    {
        public virtual int Id { get; set; }
        public virtual string? FirstName { get; set; }
        public virtual string? LastName { get; set; }
    }
}
