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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

/// <summary>
/// Shared base for the SAML-based federated authentication plugins (ADFS via
/// <see cref="FederatedAuthPlugin"/> and Okta via <see cref="OktaAuthPlugin"/>). It owns the
/// connection flow: resolving the IAM host/port/region, fetching and caching the database
/// authentication token, and retrying once on a login failure with a freshly generated token.
/// <para>
/// Subclasses supply their own telemetry counter name and token cache so each IdP keeps its own
/// metric and cache, mirroring the JDBC wrapper's <c>BaseSamlAuthPlugin</c>.
/// </para>
/// </summary>
public abstract partial class BaseSamlAuthPlugin : AbstractConnectionPlugin
{
    public static readonly int DefaultIamExpirationSeconds = 870;

    public static readonly int DefaultHttpTimeoutMs = 60000;

    public static readonly string SamlResponsePatternGroup = "saml";

    private static readonly ILogger<BaseSamlAuthPlugin> Logger = LoggerUtils.GetLogger<BaseSamlAuthPlugin>();

    private readonly IPluginService pluginService;

    private readonly CredentialsProviderFactory credentialsFactory;

    private readonly ITokenUtility tokenUtility;

    private readonly MemoryCache tokenCache;

    // Telemetry — counts each token fetch attempt (incremented at the top of
    // UpdateAuthenticationTokenAsync so both the initial cache-miss fetch and the
    // login-exception retry-fetch are captured). Counts attempts, not successes.
    private readonly ITelemetryCounter fetchTokenCounter;

    // Telemetry — reports the current number of cached tokens. The backend samples
    // the supplied callback when it observes the gauge. Held so the registration is
    // not garbage collected.
    private readonly ITelemetryGauge tokenCacheSizeGauge;

    /// <summary>
    /// Region resolver. Reassigned per call: a <see cref="GdbRegionUtils"/> for Global Aurora
    /// Database endpoints, otherwise the base <see cref="RegionUtils"/>.
    /// </summary>
    protected RegionUtils regionUtils = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="BaseSamlAuthPlugin"/> class.
    /// </summary>
    /// <param name="pluginService">The plugin service.</param>
    /// <param name="credentialsFactory">Factory that produces the IdP-derived AWS credentials.</param>
    /// <param name="tokenUtility">Utility used to build cache keys and generate authentication tokens.</param>
    /// <param name="fetchTokenCounterName">Name of the telemetry counter incremented on each token fetch attempt.</param>
    /// <param name="tokenCacheSizeGaugeName">Name of the telemetry gauge reporting the current token cache size.</param>
    /// <param name="tokenCache">The token cache shared across instances of the concrete plugin.</param>
    protected BaseSamlAuthPlugin(
        IPluginService pluginService,
        CredentialsProviderFactory credentialsFactory,
        ITokenUtility tokenUtility,
        string fetchTokenCounterName,
        string tokenCacheSizeGaugeName,
        MemoryCache tokenCache)
    {
        this.pluginService = pluginService;
        this.credentialsFactory = credentialsFactory;
        this.tokenUtility = tokenUtility;
        this.tokenCache = tokenCache;
        this.fetchTokenCounter = pluginService.TelemetryFactory.CreateCounter(fetchTokenCounterName);
        this.tokenCacheSizeGauge = pluginService.TelemetryFactory.CreateGauge(
            tokenCacheSizeGaugeName,
            () => (long)tokenCache.Count);
    }

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync", "DbConnection.ForceOpen" };

    [GeneratedRegex("SAMLResponse\\W+value=\"(?<saml>[^\"]+)\"", RegexOptions.IgnoreCase, "en-CA")]
    public static partial Regex SamlResponsePattern();

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
        // are resolved via the RDS DescribeGlobalClusters API. For a Global Aurora Database
        // endpoint the region cannot be parsed from the hostname, so we fetch the federated
        // (SAML-derived) credentials up front and use them both to authenticate the
        // DescribeGlobalClusters call and, later, to generate the database authentication token.
        RdsUrlType urlType = RdsUtils.IdentifyRdsType(host);
        AWSCredentialsProvider? credentialsProvider = null;
        if (urlType == RdsUrlType.RdsGlobalWriterCluster)
        {
            credentialsProvider = await this.credentialsFactory.GetAwsCredentialsProviderAsync(iamHostSpec.Host, null, props);
            this.regionUtils = new GdbRegionUtils(credentialsProvider.GetAWSCredentials());
        }
        else
        {
            this.regionUtils = new RegionUtils();
        }

        string region = await this.regionUtils.GetRegionAsync(iamHostSpec, props, PropertyDefinition.IamRegion)
            ?? throw new Exception(Resources.FederatedAuthPlugin_CouldNotDetermineRegion);
        string dbUser = PropertyDefinition.DbUser.GetString(props) ?? throw new Exception(Resources.FederatedAuthPlugin_DbUserNotProvided);

        string cacheKey = this.tokenUtility.GetCacheKey(dbUser, host, port, region);
        bool isCachedToken;
        if (this.tokenCache.TryGetValue(cacheKey, out string? token) && token != null)
        {
            props[PropertyDefinition.Password.Name] = token;
            isCachedToken = true;
            Logger.LogTrace(Resources.FederatedAuthPlugin_UseCachedToken);
        }
        else
        {
            await this.UpdateAuthenticationTokenAsync(props, host, port, region, cacheKey, dbUser, credentialsProvider);
            isCachedToken = false;
            Logger.LogTrace(Resources.FederatedAuthPlugin_GeneratedNewToken);
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
            await this.UpdateAuthenticationTokenAsync(props, host, port, region, cacheKey, dbUser, credentialsProvider);

            return await methodFunc();
        }
    }

    private async Task UpdateAuthenticationTokenAsync(Dictionary<string, string> props, string host, int port, string region, string cacheKey, string dbUser, AWSCredentialsProvider? credentialsProvider = null)
    {
        // Count every token-fetch attempt. Incremented before any work so both the initial
        // cache-miss fetch and the login-exception retry-fetch are captured. Counts attempts
        // (not successes); an exception later in the method does not roll the counter back.
        this.fetchTokenCounter.Inc();

        int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
        RegionEndpoint regionEndpoint = RegionUtils.IsValidRegion(region) ? RegionEndpoint.GetBySystemName(region) : throw new Exception(Resources.FederatedAuthPlugin_InvalidRegion);

        // Reuse the credentials fetched earlier for global endpoints; otherwise fetch them now
        // using the resolved region.
        credentialsProvider ??= await this.credentialsFactory.GetAwsCredentialsProviderAsync(host, regionEndpoint, props);
        AWSCredentials credentials = credentialsProvider.GetAWSCredentials();

        string token = await this.tokenUtility.GenerateAuthenticationTokenAsync(region, host, port, dbUser, credentials);
        props[PropertyDefinition.Password.Name] = token;
        this.tokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
    }
}
