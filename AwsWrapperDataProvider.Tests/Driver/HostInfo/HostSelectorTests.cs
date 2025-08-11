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
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.HostInfo;

public class HostSelectorTests
{
    private const string Reader1 = "reader-host-1.example.com";
    private const string Reader2 = "reader-host-2.example.com";
    private const string Reader3 = "reader-host-3.example.com";
    private const string Writer1 = "writer-host.example.com";
    private const string Unavailable1 = "unavailable-host-1.example.com";
    private const string Unavailable2 = "unavailable-host-2.example.com";
    private readonly List<HostSpec> testHosts;
    private readonly Dictionary<string, string> emptyProps;

    public HostSelectorTests()
    {
        this.emptyProps = [];

        // Create test hosts with different roles and availability
        this.testHosts =
        [
            new(Writer1, 3306, "writer-1", HostRole.Writer, HostAvailability.Available, 100, DateTime.UtcNow),
            new(Reader1, 3306, "reader-1", HostRole.Reader, HostAvailability.Available, 80, DateTime.UtcNow),
            new(Reader2, 3306, "reader-2", HostRole.Reader, HostAvailability.Available, 60, DateTime.UtcNow),
            new(Reader3, 3306, "reader-3", HostRole.Reader, HostAvailability.Available, 40, DateTime.UtcNow),
            new(Unavailable1, 3306, "unavailable-1", HostRole.Reader, HostAvailability.Unavailable, 90, DateTime.UtcNow),
        ];
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldSelectAvailableWriterHost()
    {
        var selector = new RandomHostSelector();

        var selectedHost = selector.GetHost(this.testHosts, HostRole.Writer, this.emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal(HostRole.Writer, selectedHost.Role);
        Assert.Equal(HostAvailability.Available, selectedHost.Availability);
        Assert.Equal(Writer1, selectedHost.Host);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldSelectAvailableReaderHost()
    {
        var selector = new RandomHostSelector();

        var selectedHost = selector.GetHost(this.testHosts, HostRole.Reader, this.emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal(HostRole.Reader, selectedHost.Role);
        Assert.Equal(HostAvailability.Available, selectedHost.Availability);
        Assert.Contains(selectedHost.Host, new[] { Reader1, Reader2, Reader3 });
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RandomHostSelector_ShouldThrowWhenNoHostsMatchRole()
    {
        var selector = new RandomHostSelector();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this.testHosts, HostRole.Unknown, this.emptyProps));

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
            var selectedHost = selector.GetHost(this.testHosts, HostRole.Reader, this.emptyProps);
            selectionCounts[selectedHost.Host] = selectionCounts.GetValueOrDefault(selectedHost.Host, 0) + 1;
        }

        Assert.True(selectionCounts.ContainsKey(Reader1));
        Assert.True(selectionCounts.ContainsKey(Reader2));
        Assert.True(selectionCounts.ContainsKey(Reader3));

        Assert.False(selectionCounts.ContainsKey(Unavailable1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void HighestWeightHostSelector_ShouldSelectHighestWeightHost()
    {
        var selector = new HighestWeightHostSelector();

        var selectedHost = selector.GetHost(this.testHosts, HostRole.Reader, this.emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal(Reader1, selectedHost.Host);
        Assert.Equal(80, selectedHost.Weight);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void HighestWeightHostSelector_ShouldSelectWriterHost()
    {
        var selector = new HighestWeightHostSelector();

        var selectedHost = selector.GetHost(this.testHosts, HostRole.Writer, this.emptyProps);

        Assert.NotNull(selectedHost);
        Assert.Equal(Writer1, selectedHost.Host);
        Assert.Equal(100, selectedHost.Weight);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void HighestWeightHostSelector_ShouldThrowWhenNoHostsMatchRole()
    {
        var selector = new HighestWeightHostSelector();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this.testHosts, HostRole.Unknown, this.emptyProps));

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
            var selectedHost = selector.GetHost(this.testHosts, HostRole.Reader, this.emptyProps);
            selectedHosts.Add(selectedHost.Host);
        }

        Assert.Contains(Reader1, selectedHosts);
        Assert.Contains(Reader2, selectedHosts);
        Assert.Contains(Reader3, selectedHosts);

        Assert.DoesNotContain(Unavailable1, selectedHosts);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobinHostSelector_ShouldRespectWeights()
    {
        var selector = new RoundRobinHostSelector();
        RoundRobinHostSelector.ClearCache();

        var props = new Dictionary<string, string>
        {
            [PropertyDefinition.RoundRobinHostWeightPairs.Name] = $"{Reader1}:3,{Reader2}:1,{Reader3}:1",
        };

        var selectedHosts = new List<string>();

        for (int i = 0; i < 10; i++)
        {
            var selectedHost = selector.GetHost(this.testHosts, HostRole.Reader, props);
            selectedHosts.Add(selectedHost.Host);
        }

        var host1Count = selectedHosts.Count(h => h == Reader1);
        var host2Count = selectedHosts.Count(h => h == Reader2);
        var host3Count = selectedHosts.Count(h => h == Reader3);

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
            [PropertyDefinition.RoundRobinHostWeightPairs.Name] = "invalid-format",
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this.testHosts, HostRole.Reader, props));

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
            [PropertyDefinition.RoundRobinDefaultWeight.Name] = "2",
        };

        // Should work without throwing exceptions
        var selectedHost = selector.GetHost(this.testHosts, HostRole.Reader, props);
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
            [PropertyDefinition.RoundRobinDefaultWeight.Name] = "0",
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            selector.GetHost(this.testHosts, HostRole.Reader, props));

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
            new(Unavailable1, 3306, "unavailable-1", HostRole.Reader, HostAvailability.Unavailable),
            new(Unavailable2, 3306, "unavailable-2", HostRole.Reader, HostAvailability.Unavailable),
        };

        foreach (var selector in selectors)
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                selector.GetHost(unavailableHosts, HostRole.Reader, this.emptyProps));

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
                selector.GetHost(emptyHosts, HostRole.Reader, this.emptyProps));

            Assert.Contains("No hosts found matching role", exception.Message);
        }
    }
}
