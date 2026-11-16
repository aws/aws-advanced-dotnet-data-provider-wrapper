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

using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider.Driver.Plugins.ConnectionLifetime.Exceptions;

/// <summary>
/// Thrown when a replacement for an expired connection could not be opened or installed. The inner
/// exception holds the reconnect failure.
/// </summary>
public class ConnectionLifetimeFailedException : ConnectionLifetimeException
{
    public const string UnableToConnectSqlState = "08001";

    public ConnectionLifetimeFailedException(Exception innerException)
        : base(Resources.ConnectionLifetimeFailedException_UnableToReconnect, UnableToConnectSqlState, innerException)
    {
    }
}
