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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySQL.Tests;

public class PersonDbContextFactory : IDesignTimeDbContextFactory<PersonDbContext>
{
    public PersonDbContext CreateDbContext(string[] args)
    {
        var connectionString = EFUtils.GetConnectionString();
        var version = new MySqlServerVersion("8.0.32");

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
            connectionString,
            wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, version))
            .LogTo(Console.WriteLine)
            .Options;

        return new PersonDbContext(options);
    }
}
