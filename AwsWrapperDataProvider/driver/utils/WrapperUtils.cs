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


using AwsWrapperDataProvider.driver.plugins;

namespace AwsWrapperDataProvider.driver.utils;

public class WrapperUtils
{
    public static T ExecuteWithPlugins<T>(
        ConnectionPluginManager connectionPluginManager,
        object methodInvokeOn,
        string methodName,
        JdbcCallable<T> jdbcCallable,
        object[] jdbcMethodArgs)
    {
        // TODO: stub implementation, please replace.
        return jdbcCallable.Invoke(jdbcMethodArgs);
    }

    private static T WrapWithProxyIfNeeded<T>(T toProxy, ConnectionPluginManager connectionPluginManager)
    {
        // TODO: stub implementation, please replace.
        return toProxy;
    }
}