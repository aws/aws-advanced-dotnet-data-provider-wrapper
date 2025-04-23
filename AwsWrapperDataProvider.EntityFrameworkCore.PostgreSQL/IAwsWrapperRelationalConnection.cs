using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL
{
    public interface IAwsWrapperRelationalConnection : IRelationalConnection
    {
        IRelationalConnection? TargetRelationalConnection { get; set; }
    }
}
