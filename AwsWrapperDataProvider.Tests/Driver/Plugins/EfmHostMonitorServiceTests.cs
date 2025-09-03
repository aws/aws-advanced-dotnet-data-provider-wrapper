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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class EfmHostMonitorServiceTests
{
    private static readonly string TestHost = "<insert_host_here>";
    private static readonly string TestUser = "<insert_username_here>";
    private static readonly string TestPassword = "<insert_password_here>";
    private static readonly string TestDatabase = "<insert_database_here>";

    private readonly string connectionString;
    private readonly Dictionary<string, string> properties;
    private readonly HostSpec hostSpec;
    private readonly Mock<IPluginService> mockPluginService;
    private readonly IHostMonitorService monitorService;

    public EfmHostMonitorServiceTests()
    {
        this.connectionString = $"Host={TestHost};Username={TestUser};Password={TestPassword};Database={TestDatabase};";

        this.properties = new()
        {
            [PropertyDefinition.Host.Name] = $"{TestHost}",
            [PropertyDefinition.User.Name] = $"{TestUser}",
            [PropertyDefinition.Password.Name] = $"{TestPassword}",
            ["Database"] = $"{TestDatabase}",
        };

        this.hostSpec = new(TestHost, 5432, HostRole.Writer, HostAvailability.Available);

        this.mockPluginService = new Mock<IPluginService>();

        this.monitorService = new HostMonitorService(this.mockPluginService.Object, this.properties);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task StopMonitoring_DisposesConnection()
    {
        AwsWrapperConnection<NpgsqlConnection> connection = new(this.connectionString);

        // open connection to host
        connection.Open();

        HostMonitorConnectionContext context = this.monitorService.StartMonitoring(connection, this.hostSpec, this.properties, 500, 500, 10);
        Assert.True(context.IsActive());

        await Task.Delay(1000, TestContext.Current.CancellationToken);

        Assert.True(context.IsActive());
        this.monitorService.StopMonitoring(context, connection);

        Assert.False(context.IsActive());
        Assert.Equal(System.Data.ConnectionState.Closed, connection.State);

        if (connection.State != System.Data.ConnectionState.Closed)
        {
            connection.Close();
        }

        HostMonitorService.CloseAllMonitors();
    }
}
