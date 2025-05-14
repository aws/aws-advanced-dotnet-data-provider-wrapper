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

using AwsWrapperDataProvider.Driver.Utils;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests;

public class AwsWrapperConnectionTest
{
    [Fact]
    public void Constructor_WithTypeArgument_SetsTargetConnectionTypeInProperties()
    {
        AwsWrapperConnection<MySqlConnection> connection =
            new("Server=<insert_rds_instance_here>;User ID=admin;Password=my_password_2020;Initial Catalog=test;");

        string typeString = PropertyDefinition.TargetConnectionType.GetString(connection.ConnectionProperties)!;
        Type? targetConnectionType = Type.GetType(typeString);

        Assert.Equal(typeof(MySqlConnection), targetConnectionType);
    }

    [Fact]
    public void Constructor_WithTargetConnectionTypeInConnectionString_SetsTargetConnectionTypeInProperties()
    {
        AwsWrapperConnection connection =
            new("Server=<insert_rds_instance_here>;User ID=admin;Password=my_password_2020;Initial Catalog=test;" +
                "TargetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;");

        string typeString = PropertyDefinition.TargetConnectionType.GetString(connection.ConnectionProperties)!;
        Type? targetConnectionType = Type.GetType(typeString)!;

        Assert.Equal(typeof(MySqlConnection), targetConnectionType);
    }
}
