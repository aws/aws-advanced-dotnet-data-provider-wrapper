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
using AwsWrapperDataProvider.driver.hostInfo;

namespace AwsWrapperDataProvider.driver.plugins;

/// <summary>
/// Interface for connection plugins that can intercept and modify connection behavior.
/// </summary>
public interface IConnectionPlugin
{
    /// <summary>
    /// Gets the set of method names this plugin subscribes to.
    /// </summary>
    /// <returns>Set of method names</returns>
    ISet<string> GetSubscribeMethods();

    /// <summary>
    /// Executes a method with the given arguments and returns a result.
    /// </summary>
    /// <param name="methodInvokedOn">The object the method is invoked on</param>
    /// <param name="methodName">The name of the method being invoked</param>
    /// <param name="jdbcCallable">The callable that executes the actual method</param>
    /// <param name="jdbcMethodArgs">The arguments to pass to the method</param>
    /// <typeparam name="T">The return type of the method</typeparam>
    /// <returns>The result of the method execution</returns>
    T Execute<T>(
        object methodInvokedOn,
        string methodName,
        JdbcCallable<T> jdbcCallable,
        object[] jdbcMethodArgs
    );
    
    /// <summary>
    /// Executes a void method with the given arguments.
    /// </summary>
    /// <param name="methodInvokedOn">The object the method is invoked on</param>
    /// <param name="methodName">The name of the method being invoked</param>
    /// <param name="jdbcCallable">The callable that executes the actual method</param>
    /// <param name="jdbcMethodArgs">The arguments to pass to the method</param>
    void Execute(
        object methodInvokedOn,
        string methodName,
        JdbcCallable jdbcCallable,
        object[] jdbcMethodArgs
    );

    /// <summary>
    /// Establishes a connection to the given host using the given properties.
    /// </summary>
    /// <param name="hostSpec">The host specification to connect to</param>
    /// <param name="props">Connection properties</param>
    /// <param name="isInitialConnection">Whether this is the initial connection</param>
    /// <param name="jdbcCallable">The callable that executes the actual connection</param>
    /// <returns>The database connection</returns>
    DbConnection Connect(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        JdbcCallable<DbConnection> jdbcCallable
    );

    /// <summary>
    /// Forces a connection to the given host using the given properties.
    /// </summary>
    /// <param name="hostSpec">The host specification to connect to</param>
    /// <param name="props">Connection properties</param>
    /// <param name="isInitialConnection">Whether this is the initial connection</param>
    /// <param name="forceConnectJdbcCallable">The callable that executes the actual connection</param>
    /// <returns>The database connection</returns>
    DbConnection ForceConnect(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        JdbcCallable<DbConnection> forceConnectJdbcCallable
    );

    /// <summary>
    /// Initializes the host provider.
    /// </summary>
    /// <param name="initialUrl">The initial connection URL</param>
    /// <param name="props">Connection properties</param>
    /// <param name="hostListProviderService">The host list provider service</param>
    /// <param name="initHostProviderFunc">The function to initialize the host provider</param>
    void InitHostProvider(
        string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        JdbcCallable<Action<object[]>> initHostProviderFunc
    );
}

/// <summary>
/// Delegate for callable methods that return a value.
/// </summary>
/// <typeparam name="T">The return type</typeparam>
/// <param name="args">The method arguments</param>
/// <returns>The method result</returns>
public delegate T JdbcCallable<out T>(object[] args);

/// <summary>
/// Delegate for callable methods that don't return a value.
/// </summary>
/// <param name="args">The method arguments</param>
public delegate void JdbcCallable(object[] args);
