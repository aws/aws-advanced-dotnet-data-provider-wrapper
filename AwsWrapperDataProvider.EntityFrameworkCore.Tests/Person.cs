using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests
{
    [Table("persons")]
    public class Person
    {
        [Column("id")]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public virtual int Id { get; set; }

        [Column("firstname")]
        public virtual string? FirstName { get; set; }

        [Column("lastname")]
        public virtual string? LastName { get; set; }
    }
}
