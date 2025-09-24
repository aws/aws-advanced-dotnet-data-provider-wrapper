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

using System.Reflection;

namespace AwsWrapperDataProvider.Driver.Plugins;

public static class AwsAuthenticationPluginLoader
{
    private static readonly Dictionary<string, string> PluginTypeMap = new()
    {
        { PluginCodes.Iam, "AwsWrapperDataProvider.Driver.Plugins.Iam.IamAuthPluginFactory" },
        { PluginCodes.SecretsManager, "AwsWrapperDataProvider.Driver.Plugins.SecretsManager.SecretsManagerAuthPluginFactory" },
        { PluginCodes.FederatedAuth, "AwsWrapperDataProvider.Driver.Plugins.FederatedAuth.FederatedAuthPluginFactory" },
        { PluginCodes.Okta, "AwsWrapperDataProvider.Driver.Plugins.FederatedAuth.OktaAuthPluginFactory" },
    };

    private static Assembly? _assembly;

    public static IConnectionPluginFactory LoadAwsFactory(string pluginCode)
    {
        try
        {
            _assembly ??= Assembly.LoadFrom("AwsWrapperDataProvider.AwsAuthenticationPluginProvider.dll");

            if (!PluginTypeMap.TryGetValue(pluginCode, out string? typeName))
            {
                throw new Exception(string.Format(Properties.Resources.Error_UnableToRetrieveAwsPluginType, pluginCode));
            }

            Type? factoryType = _assembly.GetType(typeName);
            IConnectionPluginFactory? factory = (IConnectionPluginFactory?)Activator.CreateInstance(factoryType);
            return factory ?? throw new Exception(string.Format(Properties.Resources.Error_UnableToLoadAwsPluginFromAssembly, pluginCode));
        }
        catch
        {
            throw new Exception(string.Format(Properties.Resources.Error_UnableToLoadAwsPluginFromAssembly, pluginCode));
        }
    }
}
