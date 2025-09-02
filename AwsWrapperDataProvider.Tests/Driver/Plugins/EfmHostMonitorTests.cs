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

namespace AwsWrapperDataProvider.Tests.Driver.Plugins
{
    public class EfmHostMonitorTests
    {
        private static readonly string TestHost = "<insert_host_here>";
        private static readonly string TestUser = "<insert_username_here>";
        private static readonly string TestPassword = "<insert_password_here>";
        private static readonly string TestDatabase = "<insert_database_here>";

        private readonly string connectionString;
        private readonly Mock<IPluginService> mockPluginService;
        private readonly IHostMonitor monitor;

        public EfmHostMonitorTests()
        {
            Dictionary<string, string> props = new();
            props[PropertyDefinition.Host.Name] = $"{TestHost}";
            props[PropertyDefinition.User.Name] = $"{TestUser}";
            props[PropertyDefinition.Password.Name] = $"{TestPassword}";
            props["Database"] = $"{TestDatabase}";

            HostSpec hostSpec = new(TestHost, 5432, HostRole.Writer, HostAvailability.Available);

            this.connectionString = $"Host={TestHost};Username={TestUser};Password={TestPassword};Database={TestDatabase};";

            this.mockPluginService = new Mock<IPluginService>();

            this.monitor = new HostMonitor(this.mockPluginService.Object, hostSpec, props, 500, 500, 10);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async Task StartMonitoring_MonitorsContext_WhileActive()
        {
            AwsWrapperConnection<NpgsqlConnection> connection = new(this.connectionString);

            // open connection to host
            connection.Open();

            Assert.True(this.monitor.CanDispose());

            // enqueue connection context in monitor
            HostMonitorConnectionContext context = new(connection);
            this.monitor.StartMonitoring(context);

            // monitor should not be disposable, as it is (or will) monitor the new context
            Assert.False(this.monitor.CanDispose());

            // wait for new context to enter active contexts; let the monitor go a few cycles
            await Task.Delay(2000, TestContext.Current.CancellationToken);

            Assert.True(context.IsActive());
            Assert.False(context.NodeUnhealthy);
            Assert.False(this.monitor.CanDispose());

            // stop monitoring this context
            context.SetInactive();
            connection.Close();

            await Task.Delay(200, TestContext.Current.CancellationToken);

            Assert.True(this.monitor.CanDispose());
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async Task StartMonitoring_MonitorsContext_WhileNotGarbageCollected()
        {
            AwsWrapperConnection<NpgsqlConnection>? connection = new(this.connectionString);

            // open connection to host
            connection.Open();

            Assert.True(this.monitor.CanDispose());

            // enqueue connection context in monitor
            HostMonitorConnectionContext? context = new(connection);
            this.monitor.StartMonitoring(context);

            // monitor should not be disposable, as it is (or will) monitor the new context
            Assert.False(this.monitor.CanDispose());

            // wait for new context to enter active contexts; let the monitor go a few cycles
            await Task.Delay(2000, TestContext.Current.CancellationToken);

            Assert.True(context.IsActive());
            Assert.False(context.NodeUnhealthy);
            Assert.False(this.monitor.CanDispose());

            connection.Close();
            connection = null;
            context = null;

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            await Task.Delay(2000, TestContext.Current.CancellationToken);

            // the internal weak reference to this context should have been set to null, and the context be disposed
            Assert.True(this.monitor.CanDispose());
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async Task Close_DoesNotDisposeContexts()
        {
            AwsWrapperConnection<NpgsqlConnection> connection = new(this.connectionString);

            // open connection to host
            connection.Open();

            Assert.True(this.monitor.CanDispose());

            // enqueue connection context in monitor
            HostMonitorConnectionContext context = new(connection);
            this.monitor.StartMonitoring(context);

            // monitor should not be disposable, as it is (or will) monitor the new context
            Assert.False(this.monitor.CanDispose());

            // wait for new context to enter active contexts; let the monitor go a few cycles
            await Task.Delay(2000, TestContext.Current.CancellationToken);

            Assert.True(context.IsActive());
            Assert.False(context.NodeUnhealthy);
            Assert.False(this.monitor.CanDispose());

            this.monitor.Dispose();

            Assert.True(context.IsActive());
            Assert.False(context.NodeUnhealthy);
            Assert.True(this.monitor.CanDispose());

            connection.Close();
        }
    }
}
