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
using System.Reflection;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Driver.Plugins;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver;

public class ConnectionPluginManagerTests
{
    private readonly IConnectionProvider mockConnectionProvider;
    private readonly AwsWrapperConnection mockWrapperConnection;

    public ConnectionPluginManagerTests()
    {
        this.mockConnectionProvider = Mock.Of<IConnectionProvider>();
        this.mockWrapperConnection = Mock.Of<AwsWrapperConnection>();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestExecuteCallA()
    {
        List<string> calls = [];
        List<IConnectionPlugin> testPlugins =
        [
            new TestPluginOne(calls),
            new TestPluginTwo(calls),
            new TestPluginThree(calls),
        ];
        ConnectionPluginManager connectionPluginManager = new(
            this.mockConnectionProvider,
            null,
            [],
            testPlugins,
            this.mockWrapperConnection);

        string result = connectionPluginManager.Execute(
            this.mockWrapperConnection,
            "testADONetCall_A",
            () =>
            {
                calls.Add("targetCall");
                return "resulTestValue";
            },
            []);

        Assert.Equal("resulTestValue", result);
        Assert.Equal(7, calls.Count);
        Assert.Equal("TestPluginOne:before", calls[0]);
        Assert.Equal("TestPluginTwo:before", calls[1]);
        Assert.Equal("TestPluginThree:before", calls[2]);
        Assert.Equal("targetCall", calls[3]);
        Assert.Equal("TestPluginThree:after", calls[4]);
        Assert.Equal("TestPluginTwo:after", calls[5]);
        Assert.Equal("TestPluginOne:after", calls[6]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestExecuteCallB()
    {
        List<string> calls = [];
        List<IConnectionPlugin> testPlugins =
        [
            new TestPluginOne(calls),
            new TestPluginTwo(calls),
            new TestPluginThree(calls),
        ];

        ConnectionPluginManager connectionPluginManager = new(
            this.mockConnectionProvider,
            null,
            [],
            testPlugins,
            this.mockWrapperConnection);

        string result = connectionPluginManager.Execute(
            this.mockWrapperConnection,
            "testADONetCall_B",
            () =>
            {
                calls.Add("targetCall");
                return "resulTestValue";
            },
            []);

        Assert.Equal("resulTestValue", result);
        Assert.Equal(5, calls.Count);
        Assert.Equal("TestPluginOne:before", calls[0]);
        Assert.Equal("TestPluginTwo:before", calls[1]);
        Assert.Equal("targetCall", calls[2]);
        Assert.Equal("TestPluginTwo:after", calls[3]);
        Assert.Equal("TestPluginOne:after", calls[4]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestExecuteCallC()
    {
        List<string> calls = [];
        List<IConnectionPlugin> testPlugins =
        [
            new TestPluginOne(calls),
            new TestPluginTwo(calls),
            new TestPluginThree(calls),
        ];

        ConnectionPluginManager connectionPluginManager = new(
            this.mockConnectionProvider,
            null,
            [],
            testPlugins,
            this.mockWrapperConnection);

        string result = connectionPluginManager.Execute(
            this.mockWrapperConnection,
            "testADONetCall_C",
            () =>
            {
                calls.Add("targetCall");
                return "resulTestValue";
            },
            []);

        Assert.Equal("resulTestValue", result);
        Assert.Equal(3, calls.Count);
        Assert.Equal("TestPluginOne:before", calls[0]);
        Assert.Equal("targetCall", calls[1]);
        Assert.Equal("TestPluginOne:after", calls[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestOpen()
    {
        DbConnection expectedConnection = Mock.Of<DbConnection>();
        List<string> calls = [];
        List<IConnectionPlugin> testPlugins =
        [
            new TestPluginOne(calls),
            new TestPluginTwo(calls),
            new TestPluginThree(calls),
        ];

        ConnectionPluginManager connectionPluginManager = new(
            this.mockConnectionProvider,
            null,
            [],
            testPlugins,
            this.mockWrapperConnection);

        connectionPluginManager.Open(
            new HostSpecBuilder().WithHost("anyHost").Build(),
            [],
            true,
            null,
            () =>
            {
                calls.Add("open connection");
            });

        Assert.Equal(5, calls.Count);
        Assert.Equal("TestPluginOne:before open", calls[0]);
        Assert.Equal("TestPluginThree:before open", calls[1]);
        Assert.Equal("open connection", calls[2]);
        Assert.Equal("TestPluginThree:after open", calls[3]);
        Assert.Equal("TestPluginOne:after open", calls[4]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestOpenWithSkipPlugin()
    {
        DbConnection expectedConnection = Mock.Of<DbConnection>();
        List<string> calls = [];
        IConnectionPlugin pluginOne = new TestPluginOne(calls);
        List<IConnectionPlugin> testPlugins =
        [
            pluginOne,
            new TestPluginTwo(calls),
            new TestPluginThree(calls),
        ];

        ConnectionPluginManager connectionPluginManager = new(
            this.mockConnectionProvider,
            null,
            [],
            testPlugins,
            this.mockWrapperConnection);

        connectionPluginManager.Open(
            new HostSpecBuilder().WithHost("anyHost").Build(),
            [],
            true,
            pluginOne,
            () =>
            {
                calls.Add("open connection");
            });

        Assert.Equal(3, calls.Count);
        Assert.Equal("TestPluginThree:before open", calls[0]);
        Assert.Equal("open connection", calls[1]);
        Assert.Equal("TestPluginThree:after open", calls[2]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestNoWrapperPlugins()
    {
        Dictionary<string, string> props = new()
        {
            { PropertyDefinition.Plugins.Name, string.Empty },
        };

        ConnectionPluginManager pluginManager = new(
            this.mockConnectionProvider,
            null,
            this.mockWrapperConnection);

        pluginManager.InitConnectionPluginChain(
            Mock.Of<IPluginService>(),
            props);

        // To get the protected field plugins from ConnectionPluginManager
        FieldInfo? fieldInfo = typeof(ConnectionPluginManager).GetField("plugins", BindingFlags.NonPublic | BindingFlags.Instance);
        IList<IConnectionPlugin> plugins = (IList<IConnectionPlugin>)fieldInfo?.GetValue(pluginManager)!;

        Assert.Single(plugins);
    }
}
