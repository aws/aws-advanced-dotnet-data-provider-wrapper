using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Benchmarks.TestPlugins;
using AwsWrapperDataProvider.Driver;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Moq;

namespace AwsWrapperDataProvider.Benchmarks;

[SimpleJob(RunStrategy.Monitoring, warmupCount: 3)]
[MemoryDiagnoser]
public class PluginBenchmarks
{
    private const string ConnectionString = "Host=localhost;Database=test;Username=user;Password=password";
    private const string AwsConnectionString = "aws-wrapper:Host=localhost;Database=test;Username=user;Password=password";

    private Mock<DbConnection> _mockConnection;
    private Mock<DbCommand> _mockCommand;
    private Mock<DbDataReader> _mockReader;
    private TestWrapperConnection _connectionWithExecutionTimePlugin;
    private TestWrapperConnection _connectionWithAuroraHostListPlugin;
    private TestWrapperConnection _connectionWithBothPlugins;
    private TestWrapperConnection _connectionWithNoPlugins;

    [GlobalSetup]
    public void Setup()
    {
        _mockConnection = new Mock<DbConnection>();
        _mockCommand = new Mock<DbCommand>();
        _mockReader = new Mock<DbDataReader>();
        
        // Setup mock behaviors
        _mockConnection.Setup(c => c.CreateCommand()).Returns(_mockCommand.Object);
        _mockCommand.Setup(c => c.ExecuteReader(It.IsAny<CommandBehavior>())).Returns(_mockReader.Object);
        _mockCommand.Setup(c => c.ExecuteScalar()).Returns("result");
        _mockCommand.Setup(c => c.ExecuteNonQuery()).Returns(1);

        // Create plugin managers with different configurations
        var noPluginsManager = new ConnectionPluginManager();

        var executionTimePluginManager = new ConnectionPluginManager();
        executionTimePluginManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "ExecutionTimePlugin"));

        var auroraHostListPluginManager = new ConnectionPluginManager();
        auroraHostListPluginManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "AuroraHostListPlugin"));

        var bothPluginsManager = new ConnectionPluginManager();
        bothPluginsManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "ExecutionTimePlugin"));
        bothPluginsManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "AuroraHostListPlugin"));

        // Create test connection wrappers
        _connectionWithNoPlugins = new TestWrapperConnection(_mockConnection.Object, noPluginsManager, ConnectionString);
        _connectionWithExecutionTimePlugin = new TestWrapperConnection(_mockConnection.Object, executionTimePluginManager, ConnectionString);
        _connectionWithAuroraHostListPlugin = new TestWrapperConnection(_mockConnection.Object, auroraHostListPluginManager, ConnectionString);
        _connectionWithBothPlugins = new TestWrapperConnection(_mockConnection.Object, bothPluginsManager, AwsConnectionString);
    }

    [Benchmark]
    public void InitAndReleaseBaseLine()
    {
        // Empty baseline for comparison
    }

    [Benchmark]
    public void InitAndReleaseWithNoPlugins()
    {
        using var connection = new TestWrapperConnection(
            _mockConnection.Object,
            new ConnectionPluginManager(),
            ConnectionString);
    }

    [Benchmark]
    public void InitAndReleaseWithExecutionTimePlugin()
    {
        var pluginManager = new ConnectionPluginManager();
        pluginManager.;
        
        using var connection = new TestWrapperConnection(
            _mockConnection.Object,
            pluginManager,
            ConnectionString);
    }

    [Benchmark]
    public void InitAndReleaseWithAuroraHostListPlugin()
    {
        var pluginManager = new ConnectionPluginManager();
        pluginManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "AuroraHostListPlugin"));
        
        using var connection = new TestWrapperConnection(
            _mockConnection.Object,
            pluginManager,
            ConnectionString);
    }

    [Benchmark]
    public void InitAndReleaseWithBothPlugins()
    {
        var pluginManager = new ConnectionPluginManager();
        pluginManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "ExecutionTimePlugin"));
        pluginManager.RegisterPlugin(new BenchmarkPlugin(_mockPluginContext.Object, "AuroraHostListPlugin"));

        using var connection = new TestWrapperConnection(
            _mockConnection.Object,
            pluginManager,
            AwsConnectionString);
    }

    [Benchmark]
    public object ExecuteScalarWithNoPlugins()
    {
        using var command = _connectionWithNoPlugins.CreateCommand();
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object ExecuteScalarWithExecutionTimePlugin()
    {
        using var command = _connectionWithExecutionTimePlugin.CreateCommand();
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object ExecuteScalarWithAuroraHostListPlugin()
    {
        using var command = _connectionWithAuroraHostListPlugin.CreateCommand();
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object ExecuteScalarWithBothPlugins()
    {
        using var command = _connectionWithBothPlugins.CreateCommand();
        return command.ExecuteScalar();
    }

    [Benchmark]
    public int ExecuteNonQueryWithNoPlugins()
    {
        using var command = _connectionWithNoPlugins.CreateCommand();
        return command.ExecuteNonQuery();
    }

    [Benchmark]
    public int ExecuteNonQueryWithExecutionTimePlugin()
    {
        using var command = _connectionWithExecutionTimePlugin.CreateCommand();
        return command.ExecuteNonQuery();
    }

    [Benchmark]
    public DbDataReader ExecuteReaderWithNoPlugins()
    {
        using var command = _connectionWithNoPlugins.CreateCommand();
        return command.ExecuteReader();
    }

    [Benchmark]
    public DbDataReader ExecuteReaderWithExecutionTimePlugin()
    {
        using var command = _connectionWithExecutionTimePlugin.CreateCommand();
        return command.ExecuteReader();
    }
    
    [Benchmark]
    public 
}
