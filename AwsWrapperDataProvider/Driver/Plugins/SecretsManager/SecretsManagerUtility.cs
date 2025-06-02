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

using System.Text.Json;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;

namespace AwsWrapperDataProvider.Driver.Plugins.SecretsManager;

public class SecretsManagerUtility
{
    public class AwsRdsSecrets
    {
        public required string Username;
        public required string Password;
    }

    public static AwsRdsSecrets GetRdsSecretFromAwsSecretsManager(string secretId, AmazonSecretsManagerClient client)
    {
        GetSecretValueResponse response = client.GetSecretValueAsync(new GetSecretValueRequest { SecretId = secretId })
            .GetAwaiter().GetResult();

        return JsonSerializer.Deserialize<AwsRdsSecrets>(response.SecretString) ?? throw new Exception("Could not get secrets from secrets manager.");
    }
}
