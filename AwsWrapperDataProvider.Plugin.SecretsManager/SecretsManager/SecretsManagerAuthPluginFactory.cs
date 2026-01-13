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

using Amazon;
using Amazon.SecretsManager;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.SecretsManager.Utils;

namespace AwsWrapperDataProvider.Plugin.SecretsManager.SecretsManager;

public class SecretsManagerAuthPluginFactory : IConnectionPluginFactory
{
    private static readonly Dictionary<string, AmazonSecretsManagerClient> Clients = new();

    public IConnectionPlugin GetInstance(IPluginService pluginService, Dictionary<string, string> props)
    {
        string secretId = PropertyDefinition.SecretsManagerSecretId.GetString(props) ??
                          throw new Exception("Secret ID not provided.");
        string region = RegionUtils.GetRegionFromSecretId(secretId) ??
                        PropertyDefinition.SecretsManagerRegion.GetString(props) ??
                        throw new Exception("Can't determine secret region.");
        string endpoint = PropertyDefinition.SecretsManagerEndpoint.GetString(props) ?? string.Empty;
        string usernameKey = PropertyDefinition.SecretsManagerSecretUsernameProperty.GetString(props) ?? "username";
        string passwordKey = PropertyDefinition.SecretsManagerSecretPasswordProperty.GetString(props) ?? "password";

        AmazonSecretsManagerClient? client;

        try
        {
            if (!Clients.TryGetValue(region, out client))
            {
                RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(region);
                var config = new AmazonSecretsManagerConfig { RegionEndpoint = regionEndpoint };

                if (endpoint != string.Empty)
                {
                    config.ServiceURL = endpoint;
                }

                client = new(config);

                Clients[region] = client;
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Couldn't create AWS Secrets Manager client.", ex);
        }

        int secretValueExpirySecs = PropertyDefinition.SecretsManagerExpirationSecs.GetInt(props) ?? 870;

        return new SecretsManagerAuthPlugin(pluginService, props, secretId, region, secretValueExpirySecs, usernameKey, passwordKey, client);
    }
}
