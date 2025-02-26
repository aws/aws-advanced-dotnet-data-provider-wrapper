using System;
using System.Collections.Generic;
using System.Configuration.Provider;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider.Tests
{
    public class DbProviderFactoryTests
    {
        [Fact]
        public void GetAllRegisteredFactories() {

            DataTable table = DbProviderFactories.GetFactoryClasses();
            foreach (DataRow row in table.Rows)
            {
                foreach (DataColumn column in table.Columns)
                {
                    Console.WriteLine(row[column]);
                }
            }
        }

        [Fact]
        public void GetAwsWrapperConnection() 
        {
            string providerName = "AwsWrapperProviderFactory2";
            string connectionString = ""; // simplest allowed value

            DbConnection? connection = null;

            // Create the DbProviderFactory and DbConnection.
            if (connectionString != null)
            {
                try
                {
                    DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);

                    connection = factory.CreateConnection();
                    Debug.Assert(connection != null);
                    connection.ConnectionString = connectionString;
                }
                catch (Exception ex)
                {
                    // Set the connection to null if it was created.
                    if (connection != null)
                    {
                        connection = null;
                    }
                    Console.WriteLine(ex.Message);
                }
            }
            Debug.Assert(connection != null);   
        }
    }
}
