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

using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using K4os.Compression.LZ4.Internal;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.Plugins.Iam;

public class IamAuthPlugin(IPluginService pluginService, Dictionary<string, string> props) : AbstractConnectionPlugin
{
    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync" };

    private static readonly MemoryCache IamTokenCache = new(new MemoryCacheOptions());

    private static readonly int DefaultIamExpirationSeconds = 870;

    private readonly IPluginService pluginService = pluginService;
    private readonly Dictionary<string, string> props = props;

    public override void OpenConnection(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, ADONetDelegate methodFunc)
    {
        this.ConnectInternal(hostSpec, props, methodFunc);
    }

    private void ConnectInternal(HostSpec? hostSpec, Dictionary<string, string> props, ADONetDelegate methodFunc)
    {
        string iamUser = PropertyDefinition.User.GetString(props) ?? throw new Exception(PropertyDefinition.User.Name + " is null or empty.");
        string iamHost = PropertyDefinition.IamHost.GetString(props) ?? hostSpec?.Host ?? throw new Exception("Could not determine host for IAM authentication provider.");

        // the default value for IamDefaultPort is -1, which should default to the other port property (?)
        int iamPort = PropertyDefinition.IamDefaultPort.GetInt(props) ?? this.pluginService.Dialect.DefaultPort;

        if (iamPort <= 0)
        {
            iamPort = this.pluginService.Dialect.DefaultPort;
        }

        string iamRegion = RegionUtils.GetRegion(iamHost, props, PropertyDefinition.IamRegion) ?? throw new Exception("Could not determine region for IAM authentication provider.");

        string cacheKey = IamTokenUtility.GetCacheKey(iamUser, iamHost, iamPort, iamRegion);
        if (!IamTokenCache.TryGetValue(cacheKey, out string? token))
        {
            try
            {
                token = IamTokenUtility.GenerateAuthenticationToken(iamRegion, iamHost, iamPort, iamUser);
                int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
                IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
            }
            catch (Exception ex)
            {
                throw new Exception("Could not generate authentication token for IAM user " + iamUser + ".", ex);
            }
        }

        // token is non-null here, as the above try-catch block must have succeeded
        PropertyDefinition.Password.Set(props, token);

        try
        {
            methodFunc();
        }
        catch
        {
            // should the token not work (expired on the server), generate a new one and try again
            try
            {
                token = IamTokenUtility.GenerateAuthenticationToken(iamRegion, iamHost, iamPort, iamUser);
                int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? DefaultIamExpirationSeconds;
                IamTokenCache.Set(cacheKey, token, TimeSpan.FromSeconds(tokenExpirationSeconds));
            }
            catch (Exception ex)
            {
                throw new Exception("Could not generate authentication token for IAM user " + iamUser + ".", ex);
            }

            // token is non-null here, as the above try-catch block must have succeeded
            PropertyDefinition.Password.Set(props, token);

            methodFunc();
        }
    }
}
