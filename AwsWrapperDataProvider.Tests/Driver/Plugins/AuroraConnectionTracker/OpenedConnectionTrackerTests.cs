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

    [Fact]
    [Trait("Category", "Unit")]
    public void InvalidateAllConnections_ByHostSpec_ClosesTrackedConnections()
    {
        var mockConnection = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection.Object);
        Assert.Single(OpenedConnectionTracker.OpenedConnections);

        this.tracker.InvalidateAllConnections(hostSpec);

        mockConnection.Verify(x => x.Close(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void InvalidateAllConnections_CloseThrows_SwallowsException()
    {
        var mockConnectionThatThrows = new Mock<DbConnection>();
        mockConnectionThatThrows.Setup(x => x.Close()).Throws(new InvalidOperationException("connection broken"));

        var mockConnectionThatSucceeds = new Mock<DbConnection>();

        var hostSpec = new HostSpec(
            "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnectionThatThrows.Object);
        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnectionThatSucceeds.Object);

        var exception = Record.Exception(() => this.tracker.InvalidateAllConnections(hostSpec));

        Assert.Null(exception);
        mockConnectionThatThrows.Verify(x => x.Close(), Times.Once);
        mockConnectionThatSucceeds.Verify(x => x.Close(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoveConnectionTracking_RemovesCorrectConnection()
    {
        var mockConnection1 = new Mock<DbConnection>();
        var mockConnection2 = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection1.Object);
        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection2.Object);

        this.tracker.RemoveConnectionTracking(hostSpec, mockConnection1.Object);

        var key = "test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432";
        Assert.True(OpenedConnectionTracker.OpenedConnections.ContainsKey(key));
        var queue = OpenedConnectionTracker.OpenedConnections[key];
        Assert.Single(queue);
        Assert.True(queue.TryPeek(out var weakRef));
        Assert.True(weakRef.TryGetTarget(out var remaining));
        Assert.Equal(mockConnection2.Object, remaining);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PruneNullConnections_KeepsLiveReferences()
    {
        var mockConnection = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection.Object);

        OpenedConnectionTracker.PruneNullConnections();

        Assert.Single(OpenedConnectionTracker.OpenedConnections);
        var key = "test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432";
        Assert.True(OpenedConnectionTracker.OpenedConnections.ContainsKey(key));
        var queue = OpenedConnectionTracker.OpenedConnections[key];
        Assert.Single(queue);
        Assert.True(queue.TryPeek(out var weakRef));
        Assert.True(weakRef.TryGetTarget(out var conn));
        Assert.Equal(mockConnection.Object, conn);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ClearCache_EmptiesTrackingMap()
    {
        var mockConnection = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection.Object);
        Assert.NotEmpty(OpenedConnectionTracker.OpenedConnections);

        OpenedConnectionTracker.ClearCache();

        Assert.Empty(OpenedConnectionTracker.OpenedConnections);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SharedStaticMap_TwoTrackerInstances_SeeEachOthersConnections()
    {
        var mockPluginService2 = new Mock<IPluginService>();
        var trackerB = new OpenedConnectionTracker(mockPluginService2.Object);

        var mockConnection = new Mock<DbConnection>();
        var hostSpec = new HostSpec(
            "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
            5432,
            HostRole.Writer,
            HostAvailability.Available);

        // Track through tracker A (this.tracker)
        this.tracker.PopulateOpenedConnectionQueue(hostSpec, mockConnection.Object);

        // Verify visible through the static map (shared by both instances)
        Assert.Single(OpenedConnectionTracker.OpenedConnections);
        var key = "test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432";
        Assert.True(OpenedConnectionTracker.OpenedConnections.ContainsKey(key));

        // Tracker B can invalidate what tracker A tracked
        trackerB.InvalidateAllConnections(hostSpec);
        mockConnection.Verify(x => x.Close(), Times.Once);
    }
}
