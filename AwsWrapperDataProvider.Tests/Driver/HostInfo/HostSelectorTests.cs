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
using AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;

namespace AwsWrapperDataProvider.Tests.Driver.HostInfo;

public class HostSelectorTests
{
    private readonly List<HostSpec> _testHosts;
    private readonly Dictionary<string, string> _emptyProps;

    public HostSelectorTests()
    {
        this._emptyProps = new Dictionary<string, string>();

        // Create test hosts with different roles and availability
        this._testHosts = new List<HostSpec>
        {
            new("writer-host.example.com", 3306, "writer-1", HostRole.Writer, HostAvailability.Available, 100, DateTime.UtcNow),
            new("reader-host-1.example.com", 3306, "reader-1", HostRole.Reader, HostAvailability.Available, 80, DateTime.UtcNow),
            new("reader-host-2.example.com", 3306, "reader-2", HostRole.Reader, HostAvailability.Available, 60, DateTime.UtcNow),
            new("reader-host-3.example.com", 3306, "reader-3", HostRole.Reader, HostAvailability.Available, 40, DateTime.UtcNow),
            new("unavailable-host.example.com", 3306, "unavailable-1", HostRole.Reader, HostAvailability.Unavailable, 90, DateTime.UtcNow),
        };
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldSelectAvailableWriterHost()
    {
        var selector = new RandomHostSelector();

        var selectedHost = selector.GetHost(this._testHosts, HostRole.Writer, this._emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal(HostRole.Writer, selectedHost.Role);
        Assert.Equal(HostAvailability.Available, selectedHost.Availability);
        Assert.Equal("writer-host.example.com", selectedHost.Host);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldSelectAvailableReaderHost()
    {
        var selector = new RandomHostSelector();

        var selectedHost = selector.GetHost(this._testHosts, HostRole.Reader, this._emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal(HostRole.Reader, selectedHost.Role);
        Assert.Equal(HostAvailability.Available, selectedHost.Availability);
        Assert.Contains(selectedHost.Host, new[] { "reader-host-1.example.com", "reader-host-2.example.com", "reader-host-3.example.com" });
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldThrowWhenNoHostsMatchRole()
    {
        var selector = new RandomHostSelector();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this._testHosts, HostRole.Unknown, this._emptyProps));

        Assert.Contains("No hosts found matching role: Unknown", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldDistributeSelections()
    {
        var selector = new RandomHostSelector();
        var selectionCounts = new Dictionary<string, int>();
        const int iterations = 1000;

        for (int i = 0; i < iterations; i++)
        {
            var selectedHost = selector.GetHost(this._testHosts, HostRole.Reader, this._emptyProps);
            selectionCounts[selectedHost.Host] = selectionCounts.GetValueOrDefault(selectedHost.Host, 0) + 1;
        }

        Assert.True(selectionCounts.ContainsKey("reader-host-1.example.com"));
        Assert.True(selectionCounts.ContainsKey("reader-host-2.example.com"));
        Assert.True(selectionCounts.ContainsKey("reader-host-3.example.com"));

        Assert.False(selectionCounts.ContainsKey("unavailable-host.example.com"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void HighestWeightHostSelector_ShouldSelectHighestWeightHost()
    {
        var selector = new HighestWeightHostSelector();

        var selectedHost = selector.GetHost(this._testHosts, HostRole.Reader, this._emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal("reader-host-1.example.com", selectedHost.Host);
        Assert.Equal(80, selectedHost.Weight);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void HighestWeightHostSelector_ShouldSelectWriterHost()
    {
        var selector = new HighestWeightHostSelector();

        var selectedHost = selector.GetHost(this._testHosts, HostRole.Writer, this._emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal("writer-host.example.com", selectedHost.Host);
        Assert.Equal(100, selectedHost.Weight);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void HighestWeightHostSelector_ShouldThrowWhenNoHostsMatchRole()
    {
        var selector = new HighestWeightHostSelector();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this._testHosts, HostRole.Unknown, this._emptyProps));

        Assert.Contains("No hosts found matching role: Unknown", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobinHostSelector_ShouldSelectHostsInOrder()
    {
        var selector = new RoundRobinHostSelector();
        RoundRobinHostSelector.ClearCache();

        var selectedHosts = new List<string>();

        for (int i = 0; i < 6; i++)
        {
            var selectedHost = selector.GetHost(this._testHosts, HostRole.Reader, this._emptyProps);
            selectedHosts.Add(selectedHost.Host);
        }

        Assert.Contains("reader-host-1.example.com", selectedHosts);
        Assert.Contains("reader-host-2.example.com", selectedHosts);
        Assert.Contains("reader-host-3.example.com", selectedHosts);

        Assert.DoesNotContain("unavailable-host.example.com", selectedHosts);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobinHostSelector_ShouldRespectWeights()
    {
        var selector = new RoundRobinHostSelector();
        RoundRobinHostSelector.ClearCache();

        var props = new Dictionary<string, string>
        {
            ["roundRobinHostWeightPairs"] = "reader-host-1.example.com:3,reader-host-2.example.com:1,reader-host-3.example.com:1",
        };

        var selectedHosts = new List<string>();

        for (int i = 0; i < 10; i++)
        {
            var selectedHost = selector.GetHost(this._testHosts, HostRole.Reader, props);
            selectedHosts.Add(selectedHost.Host);
        }

        var host1Count = selectedHosts.Count(h => h == "reader-host-1.example.com");
        var host2Count = selectedHosts.Count(h => h == "reader-host-2.example.com");
        var host3Count = selectedHosts.Count(h => h == "reader-host-3.example.com");

        Assert.True(host1Count > host2Count);
        Assert.True(host1Count > host3Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobinHostSelector_ShouldHandleInvalidWeightConfiguration()
    {
        var selector = new RoundRobinHostSelector();
        RoundRobinHostSelector.ClearCache();

        var props = new Dictionary<string, string>
        {
            ["roundRobinHostWeightPairs"] = "invalid-format",
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this._testHosts, HostRole.Reader, props));

        Assert.Contains("Invalid round robin host weight pairs format", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobinHostSelector_ShouldUseDefaultWeight()
    {
        var selector = new RoundRobinHostSelector();
        RoundRobinHostSelector.ClearCache();

        var props = new Dictionary<string, string>
        {
            ["roundRobinDefaultWeight"] = "2",
        };

        // Should work without throwing exceptions
        var selectedHost = selector.GetHost(this._testHosts, HostRole.Reader, props);
        Assert.NotNull(selectedHost);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobinHostSelector_ShouldThrowOnInvalidDefaultWeight()
    {
        var selector = new RoundRobinHostSelector();
        RoundRobinHostSelector.ClearCache();

        var props = new Dictionary<string, string>
        {
            ["roundRobinDefaultWeight"] = "0",
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this._testHosts, HostRole.Reader, props));

        Assert.Contains("Invalid round robin default weight", exception.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void AllSelectors_ShouldIgnoreUnavailableHosts()
    {
        var selectors = new IHostSelector[]
        {
            new RandomHostSelector(),
            new HighestWeightHostSelector(),
            new RoundRobinHostSelector(),
        };

        // Create a list with only unavailable hosts
        var unavailableHosts = new List<HostSpec>
        {
            new("unavailable-1.example.com", 3306, "unavailable-1", HostRole.Reader, HostAvailability.Unavailable),
            new("unavailable-2.example.com", 3306, "unavailable-2", HostRole.Reader, HostAvailability.Unavailable),
        };

        foreach (var selector in selectors)
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                selector.GetHost(unavailableHosts, HostRole.Reader, this._emptyProps));

            Assert.Contains("No hosts found matching role", exception.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void AllSelectors_ShouldHandleEmptyHostList()
    {
        var selectors = new IHostSelector[]
        {
            new RandomHostSelector(),
            new HighestWeightHostSelector(),
            new RoundRobinHostSelector(),
        };

        var emptyHosts = new List<HostSpec>();

        foreach (var selector in selectors)
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                selector.GetHost(emptyHosts, HostRole.Reader, this._emptyProps));

            Assert.Contains("No hosts found matching role", exception.Message);
        }
    }
}
