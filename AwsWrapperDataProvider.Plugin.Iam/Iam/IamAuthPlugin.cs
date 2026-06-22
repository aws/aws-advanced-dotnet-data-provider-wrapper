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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Auth;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Plugin.Iam.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.Iam.Iam;

public class IamAuthPlugin(IPluginService pluginService, Dictionary<string, string> props, IIamTokenUtility iamTokenUtility) : AbstractConnectionPlugin
{
    private static readonly ILogger<IamAuthPlugin> Logger = LoggerUtils.GetLogger<IamAuthPlugin>();

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync", "DbConnection.ForceOpen" };

    internal static readonly MemoryCache IamTokenCache = new(new MemoryCacheOptions());

    private readonly IPluginService pluginService = pluginService;

    private readonly Dictionary<string, string> props = props;

    private readonly IIamTokenUtility iamTokenUtility = iamTokenUtility;

    private readonly ITelemetryCounter fetchTokenCounter =
        pluginService.TelemetryFactory.CreateCounter("iam.fetchToken.count");

    private readonly ITelemetryGauge tokenCacheSizeGauge =
        pluginService.TelemetryFactory.CreateGauge(
            "iam.tokenCache.size",
            () => (long)IamTokenCache.Count);

    /// <summary>
    /// Region resolver. Reassigned per call: a <see cref="GdbRegionUtils"/> for Global Aurora
    /// Database endpoints, otherwise the base <see cref="RegionUtils"/>.
    /// </summary>
    protected RegionUtils regionUtils = new();

    public static readonly int DefaultIamExpirationSeconds = 870;

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
        // For ForceOpenConnection, we need to create a DbConnection-returning delegate from the void delegate
        return await this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private async Task<DbConnection> ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate<DbConnection> methodFunc)
    {
        string iamUser = PropertyDefinition.User.GetString(props) ??
            throw new Exception(Resources.IamAuthPlugin_CouldNotDetermineUser);

        // If an IamHost override is provided, build a new HostSpec by copying from the source HostSpec and overriding the host;
        // otherwise return the source HostSpec.
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
            iamHostSpec = hostSpec ?? throw new Exception(Resources.IamAuthPlugin_CouldNotDetermineHost);
        }

        string iamHost = iamHostSpec.Host;

        // the default value for IamDefaultPort is -1, which should default to the other port property (?)
        int iamPort = PropertyDefinition.IamDefaultPort.GetInt(props) ?? this.pluginService.Dialect.DefaultPort;

        if (iamPort <= 0)
        {
            iamPort = this.pluginService.Dialect.DefaultPort;
        }

        // Pick the right RegionUtils implementation based on the URL type so global endpoints
        // are resolved via the RDS DescribeGlobalClusters API.
        RdsUrlType urlType = RdsUtils.IdentifyRdsType(iamHost);
        this.regionUtils = urlType == RdsUrlType.RdsGlobalWriterCluster ? new GdbRegionUtils() : new RegionUtils();
        string iamRegion = await this.regionUtils.GetRegionAsync(iamHostSpec, props, PropertyDefinition.IamRegion)
            ?? throw new Exception(Resources.IamAuthPlugin_CouldNotDetermineRegion);

        string cacheKey = this.iamTokenUtility.GetCacheKey(iamUser, iamHost, iamPort, iamRegion);
        int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
        bool isCachedToken = true;
        if (!IamTokenCache.TryGetValue(cacheKey, out string? token))
        {
            token = await this.FetchTokenAsync(iamUser, iamHost, iamPort, iamRegion);
            IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
            isCachedToken = false;
            Logger.LogTrace(Resources.IamAuthPlugin_GeneratedNewToken);
        }
        else
        {
            Logger.LogTrace(Resources.IamAuthPlugin_UseCachedToken);
        }

        // When the target driver supports a native password provider, keep the token out of the
        // connection string (and therefore out of the driver's pool key) and instead register a
        // durable provider keyed by the endpoint cache key. The driver fetches the current token
        // from the shared IamTokenCache on each new physical connection, so token rotation no longer
        // fragments the connection pool.
        bool useProvider = this.pluginService.TargetConnectionDialect?.SupportsPasswordProvider == true;
        if (useProvider)
        {
            this.RegisterPasswordProvider(cacheKey, iamUser, iamHost, iamPort, iamRegion, tokenExpirationSeconds);
            props[PasswordProviderRegistry.ProviderKeyPropertyName] = cacheKey;
            PropertyDefinition.Password.Set(props, null);
        }
        else
        {
            // token is non-null here, as the above try-catch block must have succeeded
            PropertyDefinition.Password.Set(props, token);
        }

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
            token = await this.FetchTokenAsync(iamUser, iamHost, iamPort, iamRegion);
            IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
            Logger.LogTrace(Resources.IamAuthPlugin_GeneratedNewToken);

            // For the provider path the registered delegate reads the refreshed token from the cache
            // on the next physical open, so only the legacy path needs to re-set the password here.
            if (!useProvider)
            {
                // token is non-null here, as the above try-catch block must have succeeded
                PropertyDefinition.Password.Set(props, token);
            }

            return await methodFunc();
        }
    }

    /// <summary>
    /// Registers a durable <see cref="WrapperPasswordProvider"/> for the given endpoint in the
    /// <see cref="PasswordProviderRegistry"/>. The delegate serves the token from the shared
    /// <see cref="IamTokenCache"/> and only fetches a new token on a cache miss, so it is safe to be
    /// invoked by the target driver on any future physical connection open.
    /// </summary>
    private void RegisterPasswordProvider(string cacheKey, string iamUser, string iamHost, int iamPort, string iamRegion, int tokenExpirationSeconds)
    {
        PasswordProviderRegistry.Register(
            cacheKey,
            new PasswordProviderRegistration(
                async _ =>
                {
                    if (!IamTokenCache.TryGetValue(cacheKey, out string? token) || token == null)
                    {
                        token = await this.FetchTokenAsync(iamUser, iamHost, iamPort, iamRegion);
                        IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
                    }

                    return token!;
                },
                successRefreshInterval: TimeSpan.FromSeconds(Math.Max(tokenExpirationSeconds - 60, 60)),
                failureRefreshInterval: TimeSpan.FromSeconds(30)));
    }

    /// <summary>
    /// Wraps a single <see cref="IIamTokenUtility.GenerateAuthenticationTokenAsync"/>
    /// API call in the <c>"fetch IAM token"</c> nested telemetry span and
    /// increments the <c>iam.fetchToken.count</c> counter.
    /// </summary>
    private async Task<string> FetchTokenAsync(string iamUser, string iamHost, int iamPort, string iamRegion)
    {
        ITelemetryContext fetchContext = this.pluginService.TelemetryFactory
            .OpenTelemetryContext("fetch IAM token", TelemetryTraceLevel.Nested);
        this.fetchTokenCounter.Inc();
        try
        {
            string token = await this.iamTokenUtility.GenerateAuthenticationTokenAsync(iamRegion, iamHost, iamPort, iamUser);
            fetchContext.SetSuccess(true);
            return token;
        }
        catch (Exception ex)
        {
            // Record the underlying exception on the span for root-cause
            // visibility before we wrap it for the caller.
            fetchContext.SetException(ex);
            fetchContext.SetSuccess(false);
            throw new Exception(string.Format(Resources.IamAuthPlugin_CouldNotGenerateToken, iamUser), ex);
        }
        finally
        {
            fetchContext.CloseContext();
        }
    }
}
