using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL
{
    public class WrappedServiceCollection : List<ServiceDescriptor>, IServiceCollection
    {
        public WrappedServiceCollection() { }
    }
}
