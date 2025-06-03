using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Benchmarks.TestPlugins;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Plugins;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Moq;

namespace AwsWrapperDataProvider.Benchmarks;

//TODO
// - Mock connection

[SimpleJob(RunStrategy.Monitoring, warmupCount: 3)]
[MemoryDiagnoser]
public class ConnectionBenchmarks
{
    private const string PostgresConnectionString = "Host=localhost;Database=test;Username=user;Password=password";
    
    private Mock<DbConnection> _mockConnection;
    private Mock<DbCommand> _mockCommand;
    private Mock<DbDataReader> _mockReader;
    private Mock<IConnectionProvider> _mockConnectionProvider;
    private Mock<AwsWrapperConnection> _mockConnectionWrapper;
    private Mock<DbProviderFactory> _mockFactory;
    private Dictionary<string, string> _props;
    private ConnectionPluginManager _pluginManager;
    private ConnectionPluginManager _pluginManagerWithNoPlugins;

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

        // Create plugin managers
        var plugins = new List<IConnectionPlugin>
        {
            new BenchmarkPlugin("ExecutionTimePlugin"),
            new BenchmarkPlugin("AuroraHostListPlugin")
        };
        
        _pluginManager = new ConnectionPluginManager(
            _mockConnectionProvider.Object,
            null,
            _props,
            plugins,
            _mockConnectionWrapper.Object);
            
        _pluginManagerWithNoPlugins = new ConnectionPluginManager(
            _mockConnectionProvider.Object,
            null,
            _props,
            new List<IConnectionPlugin>(),
            _mockConnectionWrapper.Object);
    }

    [Benchmark]
    public void ExecuteScalarBaseline()
    {
        using var connection = _mockFactory.Object.CreateConnection();
        connection.ConnectionString = PostgresConnectionString;
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        var result = command.ExecuteScalar();
        connection.Close();
    }

    [Benchmark]
    public void ExecuteScalarWithPlugins()
    {
        _pluginManager.Execute<object>(
            _mockCommand.Object,
            "DbCommand.ExecuteScalar",
            () => _mockCommand.Object.ExecuteScalar());
    }

    [Benchmark]
    public void ExecuteNonQueryBaseline()
    {
        using var connection = _mockFactory.Object.CreateConnection();
        connection.ConnectionString = PostgresConnectionString;
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE table SET column = value";
        var result = command.ExecuteNonQuery();
        connection.Close();
    }

    [Benchmark]
    public void ExecuteNonQueryWithPlugins()
    {
        _pluginManager.Execute<int>(
            _mockCommand.Object,
            "DbCommand.ExecuteNonQuery",
            () => _mockCommand.Object.ExecuteNonQuery());
    }

    [Benchmark]
    public void ExecuteReaderBaseline()
    {
        using var connection = _mockFactory.Object.CreateConnection();
        connection.ConnectionString = PostgresConnectionString;
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM table";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            // Process row
        }
        connection.Close();
    }

    [Benchmark]
    public void ExecuteReaderWithPlugins()
    {
        _pluginManager.Execute<DbDataReader>(
            _mockCommand.Object,
            "DbCommand.ExecuteReader",
            () => _mockCommand.Object.ExecuteReader());
    }
}
