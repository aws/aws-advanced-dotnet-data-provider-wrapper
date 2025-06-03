using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Benchmarks.TestPlugins;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.HostInfo;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Moq;

namespace AwsWrapperDataProvider.Benchmarks;

[SimpleJob(RunStrategy.Monitoring, warmupCount: 3)]
[MemoryDiagnoser]
public class PluginManagerBenchmarks
{
    private const string ConnectionString = "Host=localhost;Database=test;Username=user;Password=password";
    
    private Mock<DbConnection> _mockConnection;
    private Mock<DbCommand> _mockCommand;
    private Mock<DbDataReader> _mockReader;
    private Mock<IConnectionProvider> _mockConnectionProvider;
    private Mock<AwsWrapperConnection> _mockConnectionWrapper;
    private ConnectionPluginManager _pluginManagerWithNoPlugins;
    private ConnectionPluginManager _pluginManagerWithPlugins;
    private Dictionary<string, string> _props;

    [GlobalSetup]
    public void Setup()
    {
        _mockConnection = new Mock<DbConnection>();
        _mockCommand = new Mock<DbCommand>();
        _mockReader = new Mock<DbDataReader>();
        _mockConnectionProvider = new Mock<IConnectionProvider>();
        _mockConnectionWrapper = new Mock<AwsWrapperConnection>(MockBehavior.Loose, null!, null!, null!);
        
        // Setup mock behaviors
        _mockConnection.Setup(c => c.CreateCommand()).Returns(_mockCommand.Object);
        _mockCommand.Setup(c => c.ExecuteReader(It.IsAny<CommandBehavior>())).Returns(_mockReader.Object);
        _mockCommand.Setup(c => c.ExecuteScalar()).Returns("result");
        _mockCommand.Setup(c => c.ExecuteNonQuery()).Returns(1);
        
        _props = new Dictionary<string, string>();
        
        // Create plugin managers
        _pluginManagerWithNoPlugins = new ConnectionPluginManager(
            _mockConnectionProvider.Object,
            null,
            _props,
            new List<Driver.Plugins.IConnectionPlugin>(),
            _mockConnectionWrapper.Object);
        
        var plugins = new List<Driver.Plugins.IConnectionPlugin>();
        // Add 10 benchmark plugins to simulate the Java test
        for (int i = 0; i < 10; i++)
        {
            plugins.Add(new BenchmarkPlugin($"BenchmarkPlugin{i}"));
        }
        
        _pluginManagerWithPlugins = new ConnectionPluginManager(
            _mockConnectionProvider.Object,
            null,
            _props,
            plugins,
            _mockConnectionWrapper.Object);
    }

    [Benchmark]
    public object ExecuteWithNoPlugins()
    {
        return _pluginManagerWithNoPlugins.Execute<object>(
            _mockCommand.Object,
            "DbCommand.ExecuteScalar",
            () => _mockCommand.Object.ExecuteScalar());
    }

    [Benchmark]
    public object ExecuteWithPlugins()
    {
        return _pluginManagerWithPlugins.Execute<object>(
            _mockCommand.Object,
            "DbCommand.ExecuteScalar",
            () => _mockCommand.Object.ExecuteScalar());
    }

    [Benchmark]
    public void OpenWithNoPlugins()
    {
        var hostSpec = new HostSpecBuilder().WithHost("localhost").Build();
        _pluginManagerWithNoPlugins.Open(
            hostSpec,
            _props,
            true,
            null,
            () => _mockConnection.Object.Open());
    }

    [Benchmark]
    public void OpenWithPlugins()
    {
        var hostSpec = new HostSpecBuilder().WithHost("localhost").Build();
        _pluginManagerWithPlugins.Open(
            hostSpec,
            _props,
            true,
            null,
            () => _mockConnection.Object.Open());
    }

    [Benchmark]
    public void InitHostProviderWithNoPlugins()
    {
        var mockHostListProviderService = new Mock<IHostListProviderService>();
        _pluginManagerWithNoPlugins.InitHostProvider(
            ConnectionString,
            _props,
            mockHostListProviderService.Object);
    }

    [Benchmark]
    public void InitHostProviderWithPlugins()
    {
        var mockHostListProviderService = new Mock<IHostListProviderService>();
        _pluginManagerWithPlugins.InitHostProvider(
            ConnectionString,
            _props,
            mockHostListProviderService.Object);
    }
}
