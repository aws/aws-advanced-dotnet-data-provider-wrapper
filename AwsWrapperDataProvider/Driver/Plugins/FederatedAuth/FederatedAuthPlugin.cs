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

using System.Text.RegularExpressions;
using Amazon;
using Amazon.Runtime;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;

public partial class FederatedAuthPlugin(IPluginService pluginService, Dictionary<string, string> props, CredentialsProviderFactory credentialsFactory) : AbstractConnectionPlugin
{
    private static readonly ISet<string> SubscribeMethods = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync" };

    private static readonly MemoryCache IamTokenCache = new(new MemoryCacheOptions());

    private readonly IPluginService pluginService = pluginService;

    private readonly Dictionary<string, string> props = props;

    private readonly CredentialsProviderFactory credentialsFactory = credentialsFactory;

    public static readonly string SamlResponsePatternGroup = "saml";

    [GeneratedRegex("SAMLResponse\\W+value=\"(?<saml>[^\"]+)\"", RegexOptions.IgnoreCase, "en-CA")]
    public static partial Regex SamlResponsePattern();

    public override ISet<string> GetSubscribeMethods()
    {
        return SubscribeMethods;
    }

    public override void OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate methodFunc)
    {
        this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private void ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate methodFunc)
    {
        SamlUtils.CheckIdpCredentialsWithFallback(PropertyDefinition.IdpUsername, PropertyDefinition.IdpPassword, props);

        string host = PropertyDefinition.IamHost.GetString(props) ?? hostSpec?.Host ?? string.Empty;
        int port = PropertyDefinition.IamDefaultPort.GetInt(props) ?? hostSpec?.Port ?? this.pluginService.Dialect.DefaultPort;

        if (port <= 0)
        {
            port = this.pluginService.Dialect.DefaultPort;
        }

        string region = RegionUtils.GetRegion(host, props, PropertyDefinition.IamRegion) ?? throw new Exception("Failed to determine region");
        string dbUser = PropertyDefinition.DbUser.GetString(props) ?? throw new Exception("Failed not determine db user");

        string cacheKey = IamTokenUtility.GetCacheKey(dbUser, host, port, region);
        bool isCachedToken = true;

        if (IamTokenCache.TryGetValue(cacheKey, out string? token) && token != null)
        {
            props[PropertyDefinition.Password.Name] = token;
        }
        else
        {
            this.UpdateAuthenticationToken(hostSpec, props, host, port, region, cacheKey, dbUser);
            isCachedToken = false;
        }

        props[PropertyDefinition.User.Name] = dbUser;

        try
        {
            methodFunc();
        }
        catch (Exception ex)
        {
            // should the token not work (expired on the server), generate a new one and try again
            if (isCachedToken)
            {
                this.UpdateAuthenticationToken(hostSpec, props, host, port, region, cacheKey, dbUser);
            }
            else
            {
                throw new Exception("Failed to connect; token generation may be at fault.", ex);
            }

            methodFunc();
        }
    }

    private void UpdateAuthenticationToken(HostSpec? hostSpec, Dictionary<string, string> props, string host, int port, string region, string cacheKey, string dbUser)
    {
        int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? IamAuthPlugin.DefaultIamExpirationSeconds;
        RegionEndpoint regionEndpoint = RegionUtils.IsValidRegion(region) ? RegionEndpoint.GetBySystemName(region) : throw new Exception("Invalid region");

        AWSCredentialsProvider credentialsProvider = this.credentialsFactory.GetAwsCredentialsProvider(host, regionEndpoint, props);
        AWSCredentials credentials = credentialsProvider.GetAWSCredentials();

        string token = IamTokenUtility.GenerateAuthenticationToken(region, host, port, dbUser, credentials);
        props[PropertyDefinition.Password.Name] = token;
        IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
    }
}
