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
using AwsWrapperDataProvider.Driver.Plugins;

namespace AwsWrapperDataProvider.Driver.Utils;

public class WrapperUtils
{
    public static T ExecuteWithPlugins<T>(
        ConnectionPluginManager connectionPluginManager,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        return connectionPluginManager.Execute(
            methodInvokeOn,
            methodName,
            methodFunc,
            methodArgs);
    }

    public static void RunWithPlugins(
        ConnectionPluginManager connectionPluginManager,
        object methodInvokeOn,
        string methodName,
        ADONetDelegate methodFunc,
        params object[] methodArgs)
    {
        // Type object does not mean anything since it's void return type
        ExecuteWithPlugins<object>(
            connectionPluginManager,
            methodInvokeOn,
            methodName,
            () =>
            {
                methodFunc();
                return default!;
            },
            methodArgs);
    }

    public static void OpenWithPlugins(
        ConnectionPluginManager connectionPluginManager,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate openFunc)
    {
        connectionPluginManager.Open(hostSpec, props, isInitialConnection, null, openFunc);
    }

    public static void ForceOpenWithPlugins(
        ConnectionPluginManager connectionPluginManager,
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate openFunc)
    {
        connectionPluginManager.ForceOpen(hostSpec, props, isInitialConnection, null, openFunc);
    }
}
