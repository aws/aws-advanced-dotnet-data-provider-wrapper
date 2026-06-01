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
using System.Text.RegularExpressions;
using Amazon;
using Amazon.Runtime;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Plugin.FederatedAuth.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

public partial class FederatedAuthPlugin(IPluginService pluginService, Dictionary<string, string> props, CredentialsProviderFactory credentialsFactory, ITokenUtility tokenUtility) : AbstractConnectionPlugin
{
    public static readonly int DefaultIamExpirationSeconds = 870;

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync", "DbConnection.ForceOpen" };

    public static readonly int DefaultHttpTimeoutMs = 60000;

    private static readonly MemoryCache IamTokenCache = new(new MemoryCacheOptions());

    private readonly IPluginService pluginService = pluginService;

    private readonly Dictionary<string, string> props = props;

    private readonly CredentialsProviderFactory credentialsFactory = credentialsFactory;

    private readonly ITokenUtility tokenUtility = tokenUtility;

    // Telemetry — counts each federated-auth token fetch
    // attempt (incremented at the top of UpdateAuthenticationTokenAsync so
    // both the initial cache-miss fetch and the login-exception retry-fetch
    // are captured). Uses the primary constructor's pluginService parameter;
    // TelemetryFactory returns a no-op NullTelemetryCounter when telemetry
    // is disabled.
    private readonly ITelemetryCounter fetchTokenCounter =
        pluginService.TelemetryFactory.CreateCounter("federatedAuth.fetchToken.count");

    /// <summary>
    /// Region resolver. Reassigned per call: a <see cref="GdbRegionUtils"/> for Global Aurora
    /// Database endpoints, otherwise the base <see cref="RegionUtils"/>.
    /// </summary>
    protected RegionUtils regionUtils = new();

    public static readonly string SamlResponsePatternGroup = "saml";

    [GeneratedRegex("SAMLResponse\\W+value=\"(?<saml>[^\"]+)\"", RegexOptions.IgnoreCase, "en-CA")]
    public static partial Regex SamlResponsePattern();

    public static void ClearCache()
    {
        IamTokenCache.Clear();
    }

    public override async Task<DbConnection> OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        return await this.ConnectInternal(hostSpec, props, methodFunc);
    }

    public override async Task<DbConnection> ForceOpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc, bool async)
    {
        return await this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private async Task<DbConnection> ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate<DbConnection> methodFunc)
    {
        SamlUtils.CheckIdpCredentialsWithFallback(PropertyDefinition.IdpUsername, PropertyDefinition.IdpPassword, props);

        // If an IamHost override is provided, build a new HostSpec by copying from the source HostSpec and overriding the host.
        // The HostSpec is required so global endpoints can be detected and resolved via the
        // RDS DescribeGlobalClusters API.
        string? iamHostOverride = PropertyDefinition.IamHost.GetString(props);
        HostSpec iamHostSpec;
        if (!string.IsNullOrEmpty(iamHostOverride))
        {
            iamHostSpec = hostSpec != null
                ? new HostSpecBuilder().CopyFrom(hostSpec).WithHost(iamHostOverride).Build()
                : new HostSpecBuilder().WithHost(iamHostOverride).Build();
        }
        else
        {
            iamHostSpec = hostSpec ?? throw new Exception(Resources.FederatedAuthPlugin_HostNotProvided);
        }

        string host = iamHostSpec.Host;
        int port = PropertyDefinition.IamDefaultPort.GetInt(props) ?? hostSpec?.Port ?? this.pluginService.Dialect.DefaultPort;

        if (port <= 0)
        {
            port = this.pluginService.Dialect.DefaultPort;
        }

        // Pick the right RegionUtils implementation based on the URL type so global endpoints
        // are resolved via the RDS DescribeGlobalClusters API.
        RdsUrlType urlType = RdsUtils.IdentifyRdsType(host);
        this.regionUtils = urlType == RdsUrlType.RdsGlobalWriterCluster ? new GdbRegionUtils() : new RegionUtils();
        string region = await this.regionUtils.GetRegionAsync(iamHostSpec, props, PropertyDefinition.IamRegion)
            ?? throw new Exception(Resources.FederatedAuthPlugin_CouldNotDetermineRegion);
        string dbUser = PropertyDefinition.DbUser.GetString(props) ?? throw new Exception(Resources.FederatedAuthPlugin_DbUserNotProvided);

        string cacheKey = this.tokenUtility.GetCacheKey(dbUser, host, port, region);
        bool isCachedToken = true;

        if (IamTokenCache.TryGetValue(cacheKey, out string? token) && token != null)
        {
            props[PropertyDefinition.Password.Name] = token;
        }
        else
        {
            await this.UpdateAuthenticationTokenAsync(hostSpec, props, host, port, region, cacheKey, dbUser);
            isCachedToken = false;
        }

        props[PropertyDefinition.User.Name] = dbUser;

        try
        {
            return await methodFunc();
        }
        catch (Exception ex)
        {
            if (!this.pluginService.IsLoginException(ex) || !isCachedToken)
            {
                throw;
            }

            // should the token not work (login exception + is cached token), generate a new one and try again
            await this.UpdateAuthenticationTokenAsync(hostSpec, props, host, port, region, cacheKey, dbUser);

            return await methodFunc();
        }
    }

    private async Task UpdateAuthenticationTokenAsync(HostSpec? hostSpec, Dictionary<string, string> props, string host, int port, string region, string cacheKey, string dbUser)
    {
        // Count every token-fetch attempt. Incremented before any
        // work so both the initial cache-miss fetch and the login-exception
        // retry-fetch are captured. Counts attempts (not successes); an
        // exception later in the method does not roll the counter back.
        this.fetchTokenCounter.Inc();

        int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
        RegionEndpoint regionEndpoint = RegionUtils.IsValidRegion(region) ? RegionEndpoint.GetBySystemName(region) : throw new Exception(Resources.FederatedAuthPlugin_InvalidRegion);

        AWSCredentialsProvider credentialsProvider = await this.credentialsFactory.GetAwsCredentialsProviderAsync(host, regionEndpoint, props);
        AWSCredentials credentials = credentialsProvider.GetAWSCredentials();

        string token = await this.tokenUtility.GenerateAuthenticationTokenAsync(region, host, port, dbUser, credentials);
        props[PropertyDefinition.Password.Name] = token;
        IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
    }
}
