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

using AwsWrapperDataProvider.Driver;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

/// <summary>
/// Federated authentication plugin for Okta. The connection flow is implemented by
/// <see cref="BaseSamlAuthPlugin"/>; this type only supplies the Okta-specific telemetry counter
/// name and token cache.
/// </summary>
public class OktaAuthPlugin(IPluginService pluginService, Dictionary<string, string> props, CredentialsProviderFactory credentialsFactory, ITokenUtility tokenUtility)
    : BaseSamlAuthPlugin(pluginService, props, credentialsFactory, tokenUtility, "oktaAuth.fetchToken.count", "oktaAuth.tokenCache.size", IamTokenCache)
{
    internal static readonly MemoryCache IamTokenCache = new(new MemoryCacheOptions());

    public static void ClearCache()
    {
        IamTokenCache.Clear();
    }
}
