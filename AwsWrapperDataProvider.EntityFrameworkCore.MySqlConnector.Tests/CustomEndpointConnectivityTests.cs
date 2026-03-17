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

using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.EntityFrameworkCore.MySQL.Tests;
using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;
using AwsWrapperDataProvider.Tests;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.Tests;

/// <summary>
/// Entity Framework MySQL integration tests for Custom Endpoint plugin with read-write splitting.
/// Runs only on Aurora MySQL with at least 3 instances; requires first instance to be writer.
/// </summary>
public class CustomEndpointConnectivityTests : IntegrationTestBase, IClassFixture<CustomEndpointTestFixture>
{
    private readonly ILogger logger;
    private readonly CustomEndpointTestFixture fixture;
    private readonly MySqlServerVersion version = new("8.0.32");

    protected override bool MakeSureFirstInstanceWriter => true;

    public CustomEndpointConnectivityTests(CustomEndpointTestFixture fixture)
    {
        this.fixture = fixture;
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Trace)
                .AddDebug()
                .AddConsole(options => options.FormatterName = "simple");

            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.UseUtcTimestamp = true;
                options.ColorBehavior = LoggerColorBehavior.Enabled;
            });
        });
        this.logger = loggerFactory.CreateLogger<CustomEndpointConnectivityTests>();
    }

    public override async ValueTask InitializeAsync()
    {
        ConnectionPluginChainBuilder.RegisterPluginFactory<CustomEndpointPluginFactory>(PluginCodes.CustomEndpoint);
        await base.InitializeAsync();
    }

    private async Task SetupCustomEndpointRoleAsync(HostRole hostRole)
    {
        this.logger.LogInformation($"Setting up custom endpoint instance with role: {hostRole}");
        var endpointMembers = this.fixture.EndpointInfo!.StaticMembers ?? new List<string>();
        var clusterId = TestEnvironment.Env.Info.RdsDbName!;
        var originalWriter = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);

        var connectionStringNoPlugins = ConnectionStringHelper.GetUrl(
            Engine,
            this.fixture.EndpointInfo.Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            10,
            10,
            string.Empty,
            false);

        using (var conn = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionStringNoPlugins))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            var originalInstanceId = await AuroraUtils.QueryInstanceId(conn, true);
            Assert.True(endpointMembers.Contains(originalInstanceId!), $"Instance {originalInstanceId} should be in endpoint members");

            string? failoverTarget = null;
            if (hostRole == HostRole.Writer)
            {
                if (originalInstanceId == originalWriter)
                {
                    this.logger.LogInformation($"Role is already {hostRole}, no failover needed.");
                    return;
                }

                failoverTarget = originalInstanceId;
                this.logger.LogInformation("Failing over to get writer role...");
            }
            else if (hostRole == HostRole.Reader)
            {
                if (originalInstanceId != originalWriter)
                {
                    this.logger.LogInformation($"Role is already {hostRole}, no failover needed.");
                    return;
                }

                this.logger.LogInformation("Failing over to get reader role...");
            }

            await AuroraUtils.FailoverClusterToATargetAndWaitUntilWriterChanged(clusterId, originalWriter, failoverTarget!);
        }

        this.logger.LogInformation($"Verifying that new connection has role: {hostRole}");
        using (var conn = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionStringNoPlugins))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            var currentInstanceId = await AuroraUtils.QueryInstanceId(conn, true);
            Assert.True(endpointMembers.Contains(currentInstanceId!), $"Instance {currentInstanceId} should be in endpoint members");

            var newRole = await AuroraUtils.QueryHostRoleAsync(conn, Engine, true);
            Assert.Equal(hostRole, newRole);
        }

        this.logger.LogInformation($"Custom endpoint instance successfully set to role: {hostRole}");
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EF_CustomEndpoint_ReadWriteSplitting_WithCustomEndpointChanges_WithReaderAsInitConn()
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        Assert.SkipWhen(this.fixture.EndpointInfo == null, "Custom endpoint fixture not created (not Aurora or < 3 instances).");

        await this.SetupCustomEndpointRoleAsync(HostRole.Reader);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            this.fixture.EndpointInfo!.Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            enablePooling: false);

        var wrapperConnectionString = connectionString + $";Plugins=customEndpoint,readWriteSplitting,failover;";
        wrapperConnectionString += $"; ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        wrapperConnectionString += $"; {PropertyDefinition.CustomEndpointMonitorIdleExpirationMs.Name}=30000";
        wrapperConnectionString += $"; {PropertyDefinition.WaitForCustomEndpointInfoTimeoutMs.Name}=30000";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, this.version))
            .Options;

        using var db = new PersonDbContext(options);
        await db.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
        var connection = db.Database.GetDbConnection();

        var endpointMembers = this.fixture.EndpointInfo.StaticMembers ?? new List<string>();
        var originalReaderId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.True(endpointMembers.Contains(originalReaderId!), $"Instance {originalReaderId} should be in endpoint members");

        this.logger.LogInformation("Initial connection is to a reader. Attempting to switch to writer...");
        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
        });

        var writerId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(TestEnvironment.Env.Info.RdsDbName!);
        await AuroraUtils.ModifyDBClusterEndpointAsync(this.fixture.EndpointId, new List<string> { originalReaderId!, writerId });

        try
        {
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this.fixture.EndpointId, new HashSet<string> { originalReaderId!, writerId });

            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
            var newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
            Assert.Equal(writerId, newInstanceId);

            await AuroraUtils.SetReadOnly(connection, Engine, true, true);
            newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
            Assert.Equal(originalReaderId, newInstanceId);
        }
        finally
        {
            await AuroraUtils.ModifyDBClusterEndpointAsync(this.fixture.EndpointId, new List<string> { originalReaderId! });
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this.fixture.EndpointId, new HashSet<string> { originalReaderId! });
        }

        this.logger.LogInformation("Writer removed from endpoint. Attempting to switch to writer should fail...");
        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
        });
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EF_CustomEndpoint_ReadWriteSplitting_WithCustomEndpointChanges_WithWriterAsInitConn()
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        Assert.SkipWhen(this.fixture.EndpointInfo == null, "Custom endpoint fixture not created (not Aurora or < 3 instances).");

        await this.SetupCustomEndpointRoleAsync(HostRole.Writer);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            this.fixture.EndpointInfo!.Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            enablePooling: false);

        var wrapperConnectionString = connectionString + $";Plugins=customEndpoint,readWriteSplitting,failover;";
        wrapperConnectionString += $"; ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        wrapperConnectionString += $"; {PropertyDefinition.CustomEndpointMonitorIdleExpirationMs.Name}=30000";
        wrapperConnectionString += $"; {PropertyDefinition.WaitForCustomEndpointInfoTimeoutMs.Name}=30000";

        var options = new DbContextOptionsBuilder<PersonDbContext>()
            .UseAwsWrapper(
                wrapperConnectionString,
                wrappedOptionBuilder => wrappedOptionBuilder.UseMySql(connectionString, this.version))
            .Options;

        using var db = new PersonDbContext(options);
        await db.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
        var connection = db.Database.GetDbConnection();

        var endpointMembers = this.fixture.EndpointInfo.StaticMembers ?? new List<string>();
        var originalWriterId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.True(endpointMembers.Contains(originalWriterId!), $"Instance {originalWriterId} should be in endpoint members");

        this.logger.LogInformation("Initial connection is to the writer. Attempting to switch to reader...");
        await AuroraUtils.SetReadOnly(connection, Engine, true, true);
        var newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.Equal(originalWriterId, newInstanceId);

        var writerId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(TestEnvironment.Env.Info.RdsDbName!);
        string readerIdToAdd = TestEnvironment.Env.Info.DatabaseInfo.Instances
            .First(i => i.InstanceId != writerId).InstanceId;

        await AuroraUtils.ModifyDBClusterEndpointAsync(this.fixture.EndpointId, new List<string> { originalWriterId!, readerIdToAdd });

        try
        {
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this.fixture.EndpointId, new HashSet<string> { originalWriterId!, readerIdToAdd });

            await AuroraUtils.SetReadOnly(connection, Engine, true, true);
            newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
            Assert.Equal(readerIdToAdd, newInstanceId);

            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
        }
        finally
        {
            await AuroraUtils.ModifyDBClusterEndpointAsync(this.fixture.EndpointId, new List<string> { originalWriterId! });
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this.fixture.EndpointId, new HashSet<string> { originalWriterId! });
        }

        this.logger.LogInformation("Reader removed from endpoint. Attempting to switch to reader should fallback to writer...");
        await AuroraUtils.SetReadOnly(connection, Engine, true, true);
        newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.Equal(originalWriterId, newInstanceId);
    }
}
