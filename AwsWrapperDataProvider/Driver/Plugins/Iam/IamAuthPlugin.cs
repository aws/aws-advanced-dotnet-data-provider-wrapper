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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.Plugins.Iam;

public class IamAuthPlugin(IPluginService pluginService, Dictionary<string, string> props) : AbstractConnectionPlugin
{
    private static readonly ISet<string> SubscribeMethods = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync" };

    private readonly IPluginService pluginService = pluginService;
    private readonly Dictionary<string, string> props = props;

    private readonly IamTokenCache iamTokenCache = new IamTokenCache();

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
        string iamHost = PropertyDefinition.IamHost.GetString(props) ?? hostSpec?.Host ?? throw new Exception("Could not determine host for IAM authentication provider.");

        // the default value for IamDefaultPort is -1, which should default to the other port property (?)
        int iamPort = PropertyDefinition.IamDefaultPort.GetInt(props) ?? this.pluginService.Dialect.DefaultPort;

        if (iamPort <= 0)
        {
            iamPort = this.pluginService.Dialect.DefaultPort;
        }

        string iamRegion = RegionUtils.GetRegion(iamHost, props, PropertyDefinition.IamRegion) ?? throw new Exception("Could not determine region for IAM authentication provider.");
        string? iamUser = PropertyDefinition.User.GetString(props) ?? string.Empty;

        string cacheKey = IamTokenCache.GetCacheKey(iamUser, iamHost, iamPort, iamRegion);
        string? token = this.iamTokenCache.GetToken(cacheKey);

        if (token == null)
        {
            try
            {
                token = IamTokenUtility.GenerateAuthenticationToken(iamRegion, iamHost, iamPort, iamUser);
                int tokenExpirationSeconds = PropertyDefinition.IamExpiration.GetInt(props) ?? 800; // TODO(micahdbak): default expiration
                this.iamTokenCache.SetToken(cacheKey, token, tokenExpirationSeconds);
            }
            catch (Exception ex)
            {
                throw new Exception("Could not generate authentication token for IAM user " + iamUser + ".", ex);
            }
        }

        // token is non-null here, as the above try-catch block must have succeeded
        PropertyDefinition.Password.Set(props, token);

        methodFunc();
    }
}
