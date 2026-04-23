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

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;

public static class BlueGreenRole
{
    private static readonly Dictionary<string, BlueGreenRoleType> BlueGreenRoleMapping10 = new()
    {
        { "BLUE_GREEN_DEPLOYMENT_SOURCE", BlueGreenRoleType.SOURCE },
        { "BLUE_GREEN_DEPLOYMENT_TARGET", BlueGreenRoleType.TARGET },
    };

    public static BlueGreenRoleType ParseRole(string value, string version)
    {
        if (version != "1.0")
        {
            throw new ArgumentException(string.Format(Resources.BlueGreenRole_ParseRole_ExceptionVersion, version));
        }

        if (string.IsNullOrEmpty(value) || !BlueGreenRoleMapping10.TryGetValue(value.ToUpperInvariant(), out var role))
        {
            throw new ArgumentException(string.Format(Resources.BlueGreenRole_ParseRole_ExceptionValue, value));
        }

        return role;
    }
}
