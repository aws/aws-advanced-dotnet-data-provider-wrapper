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
        new("Server", null, "MySql connection url.");

    public static readonly AwsWrapperProperty Host =
        new("Host", null, "Postgres connection url.");

    public static readonly AwsWrapperProperty Port =
        new("Port", null, "Connection port.");

    public static readonly AwsWrapperProperty Database =
        new("Database", null, "Driver database name.");

    public static readonly AwsWrapperProperty TargetConnectionType =
        new("TargetConnectionType", null, "Driver target connection type.");

    public static readonly AwsWrapperProperty TargetCommandType =
        new("TargetCommandType", null, "Driver target command type.");

    public static readonly AwsWrapperProperty TargetParameterType =
        new("TargetParameterType", null, "Driver target parameter type.");

    public static readonly AwsWrapperProperty TargetDialect =
        new("CustomDialect", null, "Custom dialect type. Should be AssemblyQualifiedName of class implementing IDialect.");

    public static readonly AwsWrapperProperty CustomTargetConnectionDialect =
        new("CustomTargetConnectionDialect", null, "Custom target connection dialect type. Should be AssemblyQualifiedName of class implementing ITargetConnectionDialect.");

    public static readonly AwsWrapperProperty Plugins = new(
        "Plugins",
        "efm,failover",
        "Comma separated list of connection plugin codes");

    public static readonly AwsWrapperProperty AutoSortPluginOrder = new(
        "AutoSortPluginOrder",
        "true",
        "This flag is enabled by default, meaning that the plugins order will be automatically adjusted. Disable it at your own risk or if you really need plugins to be executed in a particular order.");

    /// <summary>
    /// A set of AwsWrapperProperties that is used by the wrapper and should not be passed to the target driver.
    /// </summary>
    public static readonly HashSet<AwsWrapperProperty> InternalWrapperProperties = [
        TargetConnectionType,
        TargetCommandType,
        TargetParameterType,
        CustomTargetConnectionDialect,
        TargetDialect,
        Plugins,
        AutoSortPluginOrder,
    ];

    public static string GetConnectionUrl(Dictionary<string, string> props)
    {
        return Server.GetString(props) ?? Host.GetString(props) ?? throw new ArgumentException("Connection url is missing");
    }
}
