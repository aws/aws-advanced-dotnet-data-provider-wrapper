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
using Amazon.SecretsManager;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.Plugins.SecretsManager;

public class SecretsManagerAuthPlugin(IPluginService pluginService, Dictionary<string, string> props, string secretId, string region, int secretValueExpirySecs, AmazonSecretsManagerClient client) : AbstractConnectionPlugin
{
    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync", "DbConnection.ForceOpen" };

    private static readonly MemoryCache SecretValueCache = new(new MemoryCacheOptions());

    private readonly IPluginService pluginService = pluginService;

    private readonly Dictionary<string, string> props = props;

    private readonly string secretId = secretId;

    private readonly string region = region;

    private readonly string cacheKey = GetCacheKey(secretId, region);

    private readonly int secretValueExpirySecs = secretValueExpirySecs;

    private readonly AmazonSecretsManagerClient client = client;

    private SecretsManagerUtility.AwsRdsSecrets? secret;

    private static string GetCacheKey(string secretId, string region)
    {
        return secretId + ":" + region;
    }

    public override DbConnection OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc)
    {
        return this.ConnectInternal(hostSpec, props, methodFunc);
    }

    public override DbConnection ForceOpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc)
    {
        // For ForceOpenConnection, we can reuse the same logic as OpenConnection
        return this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private DbConnection ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate<DbConnection> methodFunc)
    {
        bool secretsWasFetched = this.UpdateSecrets(false);
        this.ApplySecretToProperties(props);

        try
        {
            return methodFunc();
        }
        catch
        {
            // TODO: check that this was due to a login error
            if (!secretsWasFetched)
            {
                this.UpdateSecrets(true);
                this.ApplySecretToProperties(props);
                return methodFunc();
            }
            throw;
        }
    }

    private bool UpdateSecrets(bool forceReFetch)
    {
        bool secretsWasFetched = false;

        if (forceReFetch || !SecretValueCache.TryGetValue(this.cacheKey, out SecretsManagerUtility.AwsRdsSecrets? secret))
        {
            secret = SecretsManagerUtility.GetRdsSecretFromAwsSecretsManager(this.secretId, this.client);
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
