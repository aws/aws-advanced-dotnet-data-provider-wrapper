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
/// Thrown after an expired connection was replaced. The wrapper connection is usable, but the call that
/// raised this exception was not executed and session state set on the previous connection is gone.
/// </summary>
public class ConnectionLifetimeSuccessException : ConnectionLifetimeException
{
    public const string ConnectionChangedSqlState = "08S02";

    public ConnectionLifetimeSuccessException()
        : base(Resources.ConnectionLifetimeSuccessException_ConnectionChanged, ConnectionChangedSqlState)
    {
    }
}
