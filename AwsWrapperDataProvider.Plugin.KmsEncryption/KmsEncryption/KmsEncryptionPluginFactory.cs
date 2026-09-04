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

using Amazon;
using Amazon.KeyManagementService;
using Amazon.Runtime;
using AwsWrapperDataProvider.Authentication;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Cache;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Key;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Metadata;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;

public class KmsEncryptionPluginFactory : IConnectionPluginFactory
{
    public IConnectionPlugin GetInstance(FullServicesContainer servicesContainer, Dictionary<string, string> props)
    {
        EncryptionConfig config = EncryptionConfig.FromProperties(props);
        MetadataManager.ValidateSchemaName(config.MetadataSchema);

        IPluginService pluginService = servicesContainer.PluginService;

        var plugin = new KmsEncryptionPlugin(pluginService, props);

        // The metadata manager needs the plugin so it can open a connection with this plugin skipped, and the
        // plugin needs the manager to plan statements, so the two are joined after construction.
        var metadataManager = new MetadataManager(pluginService, props, config, plugin);
        var keyManager = new KeyManager(
            () => CreateKmsClient(config, pluginService, props),
            new DataKeyCache(config),
            ownsKmsClient: true);

        plugin.Initialise(
            new StatementEncryptionPlanner(metadataManager),
            new ColumnEncryptor(keyManager, new EncryptionService()),
            metadataManager,
            keyManager);

        return plugin;
    }

    /// <summary>
    /// Builds the AWS Key Management Service client, honouring any credentials the application supplied
    /// through <see cref="AwsCredentialsManager"/> and falling back to the SDK's default chain.
    /// </summary>
    private static IAmazonKeyManagementService CreateKmsClient(
        EncryptionConfig config,
        IPluginService pluginService,
        Dictionary<string, string> props)
    {
        RegionEndpoint region = RegionEndpoint.GetBySystemName(config.Region);
        AWSCredentials? credentials =
            AwsCredentialsManager.GetCredentials(pluginService.CurrentHostSpec, props);

        return credentials is null
            ? new AmazonKeyManagementServiceClient(region)
            : new AmazonKeyManagementServiceClient(credentials, region);
    }
}
