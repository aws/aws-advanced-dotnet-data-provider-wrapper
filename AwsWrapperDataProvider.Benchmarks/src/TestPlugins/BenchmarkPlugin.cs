using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Benchmarks.TestPlugins;

/// <summary>
/// A simple plugin implementation for benchmarking purposes.
/// This plugin is designed to have minimal overhead to measure the plugin infrastructure itself.
/// </summary>
public class BenchmarkPlugin : IConnectionPlugin
{
    private readonly string _name;
    private readonly HashSet<string> _subscribedMethods = new() { "*" };

    public BenchmarkPlugin(string name = "BenchmarkPlugin")
    {
        _name = name;
    }

    public ISet<string> GetSubscribeMethods() => _subscribedMethods;

    public T Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, params object[] methodArgs)
    {
        // Pass through to the next plugin
        return methodFunc();
    }

    public void OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate methodFunc)
    {
        // Pass through to the next plugin
        methodFunc();
    }

    public void InitHostProvider(string initialUrl, Dictionary<string, string> props, IHostListProviderService hostListProviderService, ADONetDelegate<Action<object[]>> initHostProviderFunc)
    {
        // Pass through to the next plugin
        initHostProviderFunc();
    }
}
