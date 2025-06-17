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
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Benchmarks.Mocks;

/// <summary>
/// A simple plugin implementation for benchmarking purposes.
/// This plugin is designed to have minimal overhead to measure the plugin infrastructure itself.
/// </summary>
public class BenchmarkPlugin : IConnectionPlugin
{
    private readonly string _name;

    public BenchmarkPlugin(string name = "BenchmarkPlugin")
    {
        this._name = name;
    }

    public ISet<string> SubscribedMethods { get; } = new HashSet<string> { "*" };

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

    public void InitHostProvider(string initialUrl, Dictionary<string, string> props, IHostListProviderService hostListProviderService, ADONetDelegate initHostProviderFunc)
    {
        // Pass through to the next plugin
        initHostProviderFunc();
    }
}
