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

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class TestPluginOne : IConnectionPlugin
{
    protected List<string> calls;

    public virtual IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "*" };

    public TestPluginOne(List<string> calls)
    {
        this.calls = calls;
    }

    public async Task<T> Execute<T>(object methodInvokedOn, string methodName, ADONetDelegate<T> methodFunc, object[] methodArgs)
    {
        this.calls.Add(this.GetType().Name + ":before");

        T result;
        try
        {
            result = await methodFunc();
        }
        catch (Exception e)
        {
            Console.WriteLine($"Exception: {e.Message}");
            Console.WriteLine($"Stack Trace: {e.StackTrace}");
            throw;
        }

        this.calls.Add(this.GetType().Name + ":after");

        return result;
    }

    public void Execute(object methodInvokedOn, string methodName, ADONetDelegate methodFunc, object[] methodArgs)
    {
        throw new NotImplementedException();
    }

    public virtual async Task<DbConnection> OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        this.calls.Add(this.GetType().Name + ":before open");
        DbConnection connection = await methodFunc();
        this.calls.Add(this.GetType().Name + ":after open");
        return connection;
    }

    public virtual async Task<DbConnection> ForceOpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        this.calls.Add(this.GetType().Name + ":before open");
        DbConnection connection = await methodFunc();
        this.calls.Add(this.GetType().Name + ":after open");
        return connection;
    }

    public async Task<DbConnection> ForceConnect(HostSpec hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> forceConnectmethodFunc)
    {
        this.calls.Add(this.GetType().Name + ":before forceConnect");
        DbConnection result = await forceConnectmethodFunc();
        this.calls.Add(this.GetType().Name + ":after forceConnect");
        return result;
    }

    public Task InitHostProvider(string initialUrl, Dictionary<string, string> props, IHostListProviderService hostListProviderService, ADONetDelegate initHostProviderFunc)
    {
        return Task.CompletedTask;
    }
}
