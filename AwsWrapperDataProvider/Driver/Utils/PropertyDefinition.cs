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

public class PropertyDefinition
{
    public static readonly string DefaultPlugins = "efm,failover";
    public static readonly AwsWrapperProperty Database =
        new("database", null, "Driver database name");
    public static readonly AwsWrapperProperty TargetConnectionType =
        new("targetConnectionType", null, "Driver target connection type");
    public static readonly AwsWrapperProperty TargetCommandType =
        new("targetCommandType", null, "Driver target command type");
    public static readonly AwsWrapperProperty TargetParameterType =
        new("targetParameterType", null, "Driver target parameter type");
    public static readonly AwsWrapperProperty Plugins = new("Plugins", DefaultPlugins, "Comma separated list of connection plugin codes");
    public static readonly AwsWrapperProperty AutoSortPluginOrder = new(
        "AutoSortPluginOrder",
        "true",
        "This flag is enabled by default, meaning that the plugins order will be automatically adjusted. Disable it at your own risk or if you really need plugins to be executed in a particular order.");
}
