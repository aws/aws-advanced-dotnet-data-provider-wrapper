using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL
{
    public class AwsWrapperRelationalConnection : RelationalConnection, IAwsWrapperRelationalConnection
    {
        public AwsWrapperRelationalConnection(
            RelationalConnectionDependencies dependencies, IRelationalConnection targetRelationalConnection) : base(dependencies) 
        { 
            this.TargetRelationalConnection = targetRelationalConnection;
        }

        public IRelationalConnection? TargetRelationalConnection { get; set; }

        protected override DbConnection CreateDbConnection()
        {
            throw new NotImplementedException(); // it shouldn't be called
        }

        [AllowNull]
        public override DbConnection DbConnection 
        {
            get
            {
                Debug.Assert(TargetRelationalConnection != null);
                return new AwsWrapperConnection(TargetRelationalConnection.DbConnection);
            }
            set
            {
                Debug.Assert(TargetRelationalConnection != null);
                TargetRelationalConnection.DbConnection = value;
            }
        }
    }
}
