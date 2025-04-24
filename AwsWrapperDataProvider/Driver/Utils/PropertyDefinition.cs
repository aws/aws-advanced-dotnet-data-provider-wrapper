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

namespace AwsWrapperDataProvider.Driver.Utils;

public static class PropertyDefinition
{
    public static readonly AwsWrapperProperty Server =
        new AwsWrapperProperty("Server", null, "MySql connection url.");

    public static readonly AwsWrapperProperty Host =
        new AwsWrapperProperty("Host", null, "Postgres connection url.");

    public static readonly AwsWrapperProperty Port =
        new AwsWrapperProperty("Port", null, "Connection port.");

    public static readonly AwsWrapperProperty Database =
        new AwsWrapperProperty("Database", null, "Driver database name.");

    public static readonly AwsWrapperProperty TargetConnectionType =
        new AwsWrapperProperty("TargetConnectionType", null, "Driver target connection type");

    public static readonly AwsWrapperProperty TargetCommandType =
        new AwsWrapperProperty("TargetCommandType", null, "Driver target command type");

    public static readonly AwsWrapperProperty TargetParameterType =
        new AwsWrapperProperty("TargetParameterType", null, "Driver target parameter type");

    /// <summary>
    /// A set of AwsWrapperProperties that is used by the wrapper and should not be passed to the target driver.
    /// </summary>
    public static readonly HashSet<AwsWrapperProperty> InternalWrapperProperties = [
        TargetConnectionType,
        TargetCommandType,
        TargetParameterType
    ];
}
