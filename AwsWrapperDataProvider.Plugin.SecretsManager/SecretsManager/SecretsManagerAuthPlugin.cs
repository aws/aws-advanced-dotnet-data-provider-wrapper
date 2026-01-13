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

using System.Data.Common;
using Amazon;
using Amazon.SecretsManager;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.SecretsManager.Utils;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Plugin.SecretsManager.SecretsManager;

public class SecretsManagerAuthPlugin : AbstractConnectionPlugin
{
    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync", "DbConnection.ForceOpen" };

    private static readonly MemoryCache SecretValueCache = new(new MemoryCacheOptions());
    private static readonly Dictionary<string, AmazonSecretsManagerClient> Clients = new();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly string secretId;
    private readonly string region;
    private readonly string cacheKey;
    private readonly int secretValueExpirySecs;
    private readonly string usernameKey;
    private readonly string passwordKey;
    private readonly AmazonSecretsManagerClient client;

    public SecretsManagerAuthPlugin(IPluginService pluginService, Dictionary<string, string> props) : this(pluginService, props, null)
    { }

    public SecretsManagerAuthPlugin(IPluginService pluginService, Dictionary<string, string> props, AmazonSecretsManagerClient? client)
    {
        this.pluginService = pluginService;
        this.props = props;

        this.secretId = PropertyDefinition.SecretsManagerSecretId.GetString(props) ??
                        throw new ArgumentException("Secret ID not provided.");
        this.region = RegionUtils.GetRegionFromSecretId(this.secretId) ??
                      PropertyDefinition.SecretsManagerRegion.GetString(props) ??
                      throw new ArgumentException("Can't determine secret region.");
        this.usernameKey = PropertyDefinition.SecretsManagerSecretUsernameProperty.GetString(props) ?? "username";
        this.passwordKey = PropertyDefinition.SecretsManagerSecretPasswordProperty.GetString(props) ?? "password";
        this.secretValueExpirySecs = PropertyDefinition.SecretsManagerExpirationSecs.GetInt(props) ?? 870;
        this.cacheKey = GetCacheKey(this.secretId, this.region);

        this.client = client ?? CreateClient(this.region, PropertyDefinition.SecretsManagerEndpoint.GetString(props) ?? string.Empty);
    }

    private static AmazonSecretsManagerClient CreateClient(string region, string endpoint)
    {
        try
        {
            if (!Clients.TryGetValue(region, out var client))
            {
                RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(region);
                var config = new AmazonSecretsManagerConfig { RegionEndpoint = regionEndpoint };

                if (!string.IsNullOrEmpty(endpoint))
                {
                    config.ServiceURL = endpoint;
                }

                client = new AmazonSecretsManagerClient(config);
                Clients[region] = client;
            }

            return client;
        }
        catch (Exception ex)
        {
            throw new Exception("Couldn't create AWS Secrets Manager client.", ex);
        }
    }

    private SecretsManagerUtility.AwsRdsSecrets? secret;

    private static string GetCacheKey(string secretId, string region)
    {
        return secretId + ":" + region;
    }

    public static void ClearCache()
    {
        SecretValueCache.Clear();
    }

    public override async Task<DbConnection> OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        return await this.ConnectInternal(hostSpec, props, methodFunc);
    }

    public override async Task<DbConnection> ForceOpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        // For ForceOpenConnection, we can reuse the same logic as OpenConnection
        return await this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private async Task<DbConnection> ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate<DbConnection> methodFunc)
    {
        bool secretsWasFetched = await this.UpdateSecrets(false);
        this.ApplySecretToProperties(props);

        try
        {
            return await methodFunc();
        }
        catch (Exception ex)
        {
            if (!this.pluginService.IsLoginException(ex) || secretsWasFetched)
            {
                throw;
            }

            // should the token not work (login exception + is cached token), generate a new one and try again
            await this.UpdateSecrets(true);
            this.ApplySecretToProperties(props);
            return await methodFunc();
        }
    }

    private async Task<bool> UpdateSecrets(bool forceReFetch)
    {
        bool secretsWasFetched = false;

        if (forceReFetch || !SecretValueCache.TryGetValue(this.cacheKey, out SecretsManagerUtility.AwsRdsSecrets? secret))
        {
            secret = await SecretsManagerUtility.GetRdsSecretFromAwsSecretsManager(this.secretId, this.usernameKey, this.passwordKey, this.client);
            SecretValueCache.Set(this.cacheKey, secret, TimeSpan.FromSeconds(this.secretValueExpirySecs));
            secretsWasFetched = true;
        }

        this.secret = secret;
        return secretsWasFetched;
    }

    private void ApplySecretToProperties(Dictionary<string, string> props)
    {
        props[PropertyDefinition.User.Name] = this.secret?.Username ?? throw new Exception("Could not receive secrets from secrets manager.");
        props[PropertyDefinition.Password.Name] = this.secret?.Password ?? throw new Exception("Could not receive secrets from secrets manager.");
    }
}
