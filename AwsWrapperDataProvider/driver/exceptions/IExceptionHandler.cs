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


using AwsWrapperDataProvider.driver.targetDriverDialects;

namespace AwsWrapperDataProvider.driver.exceptions;

/// <summary>
/// Interface for handling database exceptions and categorizing them.
/// </summary>
public interface IExceptionHandler
{
    /// <summary>
    /// Determines if the given SQL state represents a network exception.
    /// </summary>
    /// <param name="sqlState">The SQL state code</param>
    /// <returns>True if it's a network exception, false otherwise</returns>
    bool IsNetworkException(string sqlState);

    /// <summary>
    /// Determines if the given exception is a network exception.
    /// </summary>
    /// <param name="exception">The exception to check</param>
    /// <param name="targetDriverDialect">The target driver dialect</param>
    /// <returns>True if it's a network exception, false otherwise</returns>
    bool IsNetworkException(Exception exception, ITargetDriverDialect? targetDriverDialect);

    /// <summary>
    /// Determines if the given SQL state represents a login exception.
    /// </summary>
    /// <param name="sqlState">The SQL state code</param>
    /// <returns>True if it's a login exception, false otherwise</returns>
    bool IsLoginException(string sqlState);

    /// <summary>
    /// Determines if the given exception is a login exception.
    /// </summary>
    /// <param name="exception">The exception to check</param>
    /// <param name="targetDriverDialect">The target driver dialect</param>
    /// <returns>True if it's a login exception, false otherwise</returns>
    bool IsLoginException(Exception exception, ITargetDriverDialect? targetDriverDialect);
}