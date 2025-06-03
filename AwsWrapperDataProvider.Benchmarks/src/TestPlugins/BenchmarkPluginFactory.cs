using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Benchmarks.TestPlugins;

/// <summary>
/// Factory for creating benchmark plugins.
/// </summary>
public class BenchmarkPluginFactory : IConnectionPluginFactory
{
    public IConnectionPlugin GetInstance(IPluginService pluginService, Dictionary<string, string> props)
    {
        return new BenchmarkPlugin();
    }
}
