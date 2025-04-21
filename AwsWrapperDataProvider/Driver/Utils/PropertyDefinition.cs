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
    public static AwsWrapperProperty Database =
        new AwsWrapperProperty("database", null, "Driver database name");
    public static AwsWrapperProperty TargetConnectionType =
        new AwsWrapperProperty("targetConnectionType", null, "Driver target connection type");
    public static AwsWrapperProperty TargetCommandType =
        new AwsWrapperProperty("targetCommandType", null, "Driver target command type");
    public static AwsWrapperProperty TargetParameterType =
        new AwsWrapperProperty("targetParameterType", null, "Driver target parameter type");
}
