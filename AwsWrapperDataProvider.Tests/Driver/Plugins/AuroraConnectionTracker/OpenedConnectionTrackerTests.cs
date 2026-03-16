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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.AuroraConnectionTracker;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.AuroraConnectionTracker;

public class OpenedConnectionTrackerTests
{
    private readonly Mock<IPluginService> mockPluginService;
    private readonly OpenedConnectionTracker tracker;

    public OpenedConnectionTrackerTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        OpenedConnectionTracker.ClearCache();
        this.tracker = new OpenedConnectionTracker(this.mockPluginService.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PopulateOpenedConnectionQueue_RdsInstanceHost_TracksUnderHostPort()
    {
        var mockConnection = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "test-instance.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection.Object);

        Assert.Single(OpenedConnectionTracker.OpenedConnections);
        Assert.True(OpenedConnectionTracker.OpenedConnections.ContainsKey("test-instance.xyz.us-east-1.rds.amazonaws.com:5432"));

        var queue = OpenedConnectionTracker.OpenedConnections["test-instance.xyz.us-east-1.rds.amazonaws.com:5432"];
        Assert.False(queue.IsEmpty);
        Assert.True(queue.TryPeek(out var weakRef));
        Assert.True(weakRef.TryGetTarget(out var conn));
        Assert.Equal(mockConnection.Object, conn);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PopulateOpenedConnectionQueue_NonRdsHostWithRdsAlias_TracksUnderAliasKey()
    {
        var mockConnection = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "cluster-endpoint.cluster-xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);
        hostSpec.AddAlias("test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432");

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection.Object);

        Assert.Single(OpenedConnectionTracker.OpenedConnections);
        Assert.True(OpenedConnectionTracker.OpenedConnections.ContainsKey("test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432"));
        Assert.False(OpenedConnectionTracker.OpenedConnections.ContainsKey("cluster-endpoint.cluster-xyz.us-east-1.rds.amazonaws.com:5432"));
    }
}
