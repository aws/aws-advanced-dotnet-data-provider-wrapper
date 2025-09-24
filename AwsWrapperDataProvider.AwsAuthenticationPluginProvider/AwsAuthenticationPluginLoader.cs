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

using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Plugins.SecretsManager;

namespace AwsWrapperDataProvider.AwsAuthenticationPluginProvider;

public class AwsAuthenticationPluginLoader
{
    internal static void Load()
    {
        ConnectionPluginChainBuilder.RegisterPluginFactory(PluginCodes.Iam, new Lazy<IConnectionPluginFactory>(() => new IamAuthPluginFactory()));
        ConnectionPluginChainBuilder.RegisterPluginFactory(PluginCodes.SecretsManager, new Lazy<IConnectionPluginFactory>(() => new SecretsManagerAuthPluginFactory()));
        ConnectionPluginChainBuilder.RegisterPluginFactory(PluginCodes.FederatedAuth, new Lazy<IConnectionPluginFactory>(() => new FederatedAuthPluginFactory()));
        ConnectionPluginChainBuilder.RegisterPluginFactory(PluginCodes.Okta, new Lazy<IConnectionPluginFactory>(() => new OktaAuthPluginFactory()));
    }
}
