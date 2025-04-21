// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Configuration.Provider;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace AwsWrapperDataProvider.Tests
{
    public class DbProviderFactoryTests
    {
        [Fact]
        public void GetAllRegisteredFactories()
        {
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
        [Trait("Category", "Integration")]
        public void GetAwsWrapperConnection()
        {
            string providerName = "AwsWrapperProviderFactory2";
            string connectionString = string.Empty; // simplest allowed value

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
