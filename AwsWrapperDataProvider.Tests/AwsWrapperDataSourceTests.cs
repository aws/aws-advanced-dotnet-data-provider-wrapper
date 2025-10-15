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

using System.Data.Common;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperDataSourceTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithMySqlDataSource_CreatesWrappedConnection()
    {
        DbDataSource dataSource = new MySqlDataSource("Server=<insert_rds_instance_here>;User ID=admin;Password=my_password_2020;Initial Catalog=test;");
        DbDataSource wrapper = new AwsWrapperDataSource(dataSource);
        DbConnection connection = wrapper.CreateConnection();

        Assert.True(connection is AwsWrapperConnection);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithMySqlDataSource_CreatesWrappedBatch()
    {
        DbDataSource dataSource = new MySqlDataSource("Server=<insert_rds_instance_here>;User ID=admin;Password=my_password_2020;Initial Catalog=test;");
        DbDataSource wrapper = new AwsWrapperDataSource(dataSource);
        DbBatch batch = wrapper.CreateBatch();

        Assert.True(batch is AwsWrapperBatch);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithMySqlDataSource_CreatesWrappedCommand()
    {
        DbDataSource dataSource = new MySqlDataSource("Server=<insert_rds_instance_here>;User ID=admin;Password=my_password_2020;Initial Catalog=test;");
        DbDataSource wrapper = new AwsWrapperDataSource(dataSource);
        DbCommand batch = wrapper.CreateCommand();

        Assert.True(batch is AwsWrapperCommand);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithNpgsqlDataSource_CreatesWrappedConnection()
    {
        DbDataSource dataSource = NpgsqlDataSource.Create("Host=<insert_rds_instance_here>;Username=pgadmin;Password=my_password_2020;Database=postgres;");
        DbDataSource wrapper = new AwsWrapperDataSource(dataSource);
        DbConnection connection = wrapper.CreateConnection();

        Assert.True(connection is AwsWrapperConnection);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithNpgsqlDataSource_CreatesWrappedBatch()
    {
        DbDataSource dataSource = NpgsqlDataSource.Create("Host=<insert_rds_instance_here>;Username=pgadmin;Password=my_password_2020;Database=postgres;");
        DbDataSource wrapper = new AwsWrapperDataSource(dataSource);
        DbBatch batch = wrapper.CreateBatch();

        Assert.True(batch is AwsWrapperBatch);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_WithNpgsqlDataSource_CreatesWrappedCommand()
    {
        DbDataSource dataSource = NpgsqlDataSource.Create("Host=<insert_rds_instance_here>;Username=pgadmin;Password=my_password_2020;Database=postgres;");
        DbDataSource wrapper = new AwsWrapperDataSource(dataSource);
        DbCommand batch = wrapper.CreateCommand();

        Assert.True(batch is AwsWrapperCommand);
    }
}
