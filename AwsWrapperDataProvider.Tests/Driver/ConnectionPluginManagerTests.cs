//// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
////
//// Licensed under the Apache License, Version 2.0 (the "License").
//// You may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//using System.Data.Common;
//using AwsWrapperDataProvider.Driver;
//using AwsWrapperDataProvider.Driver.ConnectionProviders;
//using AwsWrapperDataProvider.Driver.HostInfo;
//using AwsWrapperDataProvider.Driver.Plugins;
//using AwsWrapperDataProvider.Driver.Utils;
//using AwsWrapperDataProvider.Tests.Driver.Plugins;
//using Moq;
//using MySqlConnector;

//namespace AwsWrapperDataProvider.Tests.Driver;

//public class ConnectionPluginManagerTests
//{
//    private readonly IConnectionProvider mockConnectionProvider;
//    private readonly AwsWrapperConnection mockWrapperConnection;

//    public ConnectionPluginManagerTests()
//    {
//        this.mockConnectionProvider = Mock.Of<IConnectionProvider>();
//        this.mockWrapperConnection = new Mock<AwsWrapperConnection<MySqlConnection>>(
//            "Server=<insert_rds_instance_here>;User ID=admin;Password=my_password_2020;Initial Catalog=test;").Object;
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void TestExecuteCallA()
//    {
//        List<string> calls = [];
//        List<IConnectionPlugin> testPlugins =
//        [
//            new TestPluginOne(calls),
//            new TestPluginTwo(calls),
//            new TestPluginThree(calls),
//        ];
//        ConnectionPluginManager connectionPluginManager = new(
//            this.mockConnectionProvider,
//            null,
//            testPlugins,
//            this.mockWrapperConnection);

//        string result = connectionPluginManager.Execute(
//            this.mockWrapperConnection,
//            "testADONetCall_A",
//            () =>
//            {
//                calls.Add("targetCall");
//                return "resulTestValue";
//            },
//            []);

//        Assert.Equal("resulTestValue", result);
//        Assert.Equal(7, calls.Count);
//        Assert.Equal("TestPluginOne:before", calls[0]);
//        Assert.Equal("TestPluginTwo:before", calls[1]);
//        Assert.Equal("TestPluginThree:before", calls[2]);
//        Assert.Equal("targetCall", calls[3]);
//        Assert.Equal("TestPluginThree:after", calls[4]);
//        Assert.Equal("TestPluginTwo:after", calls[5]);
//        Assert.Equal("TestPluginOne:after", calls[6]);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void TestExecuteCallB()
//    {
//        List<string> calls = [];
//        List<IConnectionPlugin> testPlugins =
//        [
//            new TestPluginOne(calls),
//            new TestPluginTwo(calls),
//            new TestPluginThree(calls),
//        ];

//        ConnectionPluginManager connectionPluginManager = new(
//            this.mockConnectionProvider,
//            null,
//            testPlugins,
//            this.mockWrapperConnection);

//        string result = connectionPluginManager.Execute(
//            this.mockWrapperConnection,
//            "testADONetCall_B",
//            () =>
//            {
//                calls.Add("targetCall");
//                return "resulTestValue";
//            },
//            []);

//        Assert.Equal("resulTestValue", result);
//        Assert.Equal(5, calls.Count);
//        Assert.Equal("TestPluginOne:before", calls[0]);
//        Assert.Equal("TestPluginTwo:before", calls[1]);
//        Assert.Equal("targetCall", calls[2]);
//        Assert.Equal("TestPluginTwo:after", calls[3]);
//        Assert.Equal("TestPluginOne:after", calls[4]);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void TestExecuteCallC()
//    {
//        List<string> calls = [];
//        List<IConnectionPlugin> testPlugins =
//        [
//            new TestPluginOne(calls),
//            new TestPluginTwo(calls),
//            new TestPluginThree(calls),
//        ];

//        ConnectionPluginManager connectionPluginManager = new(
//            this.mockConnectionProvider,
//            null,
//            testPlugins,
//            this.mockWrapperConnection);

//        string result = connectionPluginManager.Execute(
//            this.mockWrapperConnection,
//            "testADONetCall_C",
//            () =>
//            {
//                calls.Add("targetCall");
//                return "resulTestValue";
//            },
//            []);

//        Assert.Equal("resulTestValue", result);
//        Assert.Equal(3, calls.Count);
//        Assert.Equal("TestPluginOne:before", calls[0]);
//        Assert.Equal("targetCall", calls[1]);
//        Assert.Equal("TestPluginOne:after", calls[2]);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void TestOpen()
//    {
//        DbConnection expectedConnection = Mock.Of<DbConnection>();
//        List<string> calls = [];
//        List<IConnectionPlugin> testPlugins =
//        [
//            new TestPluginOne(calls),
//            new TestPluginTwo(calls),
//            new TestPluginThree(calls),
//        ];

//        ConnectionPluginManager connectionPluginManager = new(
//            this.mockConnectionProvider,
//            null,
//            testPlugins,
//            this.mockWrapperConnection);

//        try
//        {
//            connectionPluginManager.Open(
//                new HostSpecBuilder().WithHost("anyHost").Build(),
//                [],
//                true,
//                null);
//        }
//        catch
//        {
//            // Ignore as Open should throw error on invoking methodFunc();
//        }

//        Assert.Equal(2, calls.Count);
//        Assert.Equal("TestPluginOne:before open", calls[0]);
//        Assert.Equal("TestPluginThree:before open", calls[1]);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void TestOpenWithSkipPlugin()
//    {
//        DbConnection expectedConnection = Mock.Of<DbConnection>();
//        List<string> calls = [];
//        IConnectionPlugin pluginOne = new TestPluginOne(calls);
//        List<IConnectionPlugin> testPlugins =
//        [
//            pluginOne,
//            new TestPluginTwo(calls),
//            new TestPluginThree(calls),
//        ];

//        ConnectionPluginManager connectionPluginManager = new(
//            this.mockConnectionProvider,
//            null,
//            testPlugins,
//            this.mockWrapperConnection);

//        try
//        {
//            connectionPluginManager.Open(
//                new HostSpecBuilder().WithHost("anyHost").Build(),
//                [],
//                true,
//                pluginOne);
//        }
//        catch
//        {
//            // Ignore as Open should throw error on invoking methodFunc();
//        }

//        Assert.Single(calls);
//        Assert.Equal("TestPluginThree:before open", calls[0]);
//    }

//    [Fact]
//    [Trait("Category", "Unit")]
//    public void TestNoWrapperPlugins()
//    {
//        Dictionary<string, string> props = new()
//        {
//            { PropertyDefinition.Plugins.Name, string.Empty },
//        };

//        ConnectionPluginManager pluginManager = new(
//            this.mockConnectionProvider,
//            null,
//            this.mockWrapperConnection,
//            null);

//        pluginManager.InitConnectionPluginChain(
//            Mock.Of<IPluginService>(),
//            props);

//        IList<IConnectionPlugin> plugins = TestUtils.GetNonPublicInstanceField<IList<IConnectionPlugin>>(pluginManager, "plugins")!;

//        Assert.Single(plugins);
//    }
//}
