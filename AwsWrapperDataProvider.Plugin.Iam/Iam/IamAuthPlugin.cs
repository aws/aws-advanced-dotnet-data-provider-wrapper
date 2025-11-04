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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.Iam.Utils;
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

    public static readonly int DefaultIamExpirationSeconds = 870;

    public static void ClearCache()
    {
        IamTokenCache.Clear();
    }

    public override DbConnection OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc)
    {
        return this.ConnectInternal(hostSpec, props, methodFunc);
    }

    public override DbConnection ForceOpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate<DbConnection> methodFunc)
    {
        // For ForceOpenConnection, we need to create a DbConnection-returning delegate from the void delegate
        return this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private DbConnection ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate<DbConnection> methodFunc)
    {
        string iamUser = PropertyDefinition.User.GetString(props) ?? PropertyDefinition.UserId.GetString(props) ??
            throw new Exception("Could not determine user for IAM authentication.");
        string iamHost = PropertyDefinition.IamHost.GetString(props) ?? hostSpec?.Host ?? throw new Exception("Could not determine host for IAM authentication provider.");

        // the default value for IamDefaultPort is -1, which should default to the other port property (?)
        int iamPort = PropertyDefinition.IamDefaultPort.GetInt(props) ?? this.pluginService.Dialect.DefaultPort;

        if (iamPort <= 0)
        {
            iamPort = this.pluginService.Dialect.DefaultPort;
        }

        string iamRegion = RegionUtils.GetRegion(iamHost, props, PropertyDefinition.IamRegion) ?? throw new Exception("Could not determine region for IAM authentication provider.");

        string cacheKey = this.iamTokenUtility.GetCacheKey(iamUser, iamHost, iamPort, iamRegion);
        bool isCachedToken = true;
        if (!IamTokenCache.TryGetValue(cacheKey, out string? token))
        {
            try
            {
                token = this.iamTokenUtility.GenerateAuthenticationToken(iamRegion, iamHost, iamPort, iamUser, null);
                int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
                IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
                isCachedToken = false;
                Logger.LogTrace("Generated new authentication token");
            }
            catch (Exception ex)
            {
                throw new Exception("Could not generate authentication token for IAM user " + iamUser + ".", ex);
            }
        }
        else
        {
            Logger.LogTrace("Use cached authentication token");
        }

        // token is non-null here, as the above try-catch block must have succeeded
        PropertyDefinition.Password.Set(props, token);

        try
        {
            return methodFunc();
        }
        catch (Exception ex)
        {
            if (!this.pluginService.IsLoginException(ex) || !isCachedToken)
            {
                throw;
            }

            // should the token not work (login exception + is cached token), generate a new one and try again
            try
            {
                token = this.iamTokenUtility.GenerateAuthenticationToken(iamRegion, iamHost, iamPort, iamUser, null);
                int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
                IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
                Logger.LogTrace("Generated new authentication token");
            }
            catch (Exception ex2)
            {
                throw new Exception("Could not generate authentication token for IAM user " + iamUser + ".", ex2);
            }

            // token is non-null here, as the above try-catch block must have succeeded
            PropertyDefinition.Password.Set(props, token);

            return methodFunc();
        }
    }
}
