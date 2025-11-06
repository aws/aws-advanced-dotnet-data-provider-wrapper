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

using AwsWrapperDataProvider.Driver.Plugins.Failover;
using Microsoft.EntityFrameworkCore;

namespace MySqlEntityFrameworkExample;

public class MySqlEntityFrameworkExample
{
    private static readonly DbContextOptions<ProductDbContext> Options = CreateDbContextOptions();

    public static async Task Main(string[] args)
    {
        var start = DateTime.UtcNow;
        var threshold = TimeSpan.FromMinutes(5);

        int i = 0;
        try
        {
            // Keep inserting data for 5 minutes.
            while (DateTime.UtcNow - start < threshold)
            {
                var product = new Product
                {
                    Name = $"Product{i}",
                    Price = 29.99m + i,
                    CreatedAt = DateTime.UtcNow,
                };

                await InsertProduct(product);
                i++;
            }
        }
        catch (FailoverSuccessException ex)
        {
            // Exception should be a FailoverSuccessException if failover occurred.
            // For more information regarding FailoverSuccessException please visit the driver's documentation.
            Console.WriteLine($"Failover completed successfully: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error occurred: {ex.Message}");
        }
    }

    private static async Task InsertProduct(Product product)
    {
        await using var context = new ProductDbContext(Options);
        context.Products.Add(product);
        await context.SaveChangesAsync();
    }

    private static DbContextOptions<ProductDbContext> CreateDbContextOptions()
    {
        const string connectionString = "Server=<endpoint>;" +
                                        "Database=<db name>;" +
                                        "User Id=<user>;" +
                                        "Password=<password>;" +
                                        "Plugins=failover,initialConnection;";

        return new DbContextOptionsBuilder<ProductDbContext>()
            .UseAwsWrapper(
                connectionString,
                wrappedOptions => wrappedOptions.UseMySql(
                    connectionString,
                    new MySqlServerVersion("9.0.0")))
            .Options;
    }
}
