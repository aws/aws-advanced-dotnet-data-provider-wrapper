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

namespace AwsWrapperDataProvider.Plugin.SecretsManager.SecretsManager;

public static class SecretsManagerUtility
{
    public class AwsRdsSecrets
    {
        public string? Username { get; set; }

        public string? Password { get; set; }
    }

    public static async Task<AwsRdsSecrets> GetRdsSecretFromAwsSecretsManager(string secretId, string usernameKey, string passwordKey, AmazonSecretsManagerClient client)
    {
        try
        {
            GetSecretValueResponse response = await client.GetSecretValueAsync(new GetSecretValueRequest { SecretId = secretId });
            using JsonDocument doc = JsonDocument.Parse(response.SecretString);

            string? username = doc.RootElement.TryGetProperty(usernameKey, out JsonElement usernameElement) ? usernameElement.GetString() : null;
            string? password = doc.RootElement.TryGetProperty(passwordKey, out JsonElement passwordElement) ? passwordElement.GetString() : null;

            return username == null || password == null
                ? throw new Exception($"Username or password not found in RDS secret. Please ensure the values specified in SecretsManagerSecretUsernameProperty ({usernameKey}) and SecretsManagerSecretPasswordProperty ({passwordKey}) matches the content of the Secret value.")
                : new AwsRdsSecrets { Username = username, Password = password };
        }
        catch (Exception ex)
        {
            throw new Exception("Couldn't get RDS secret from AWS Secrets Manager.", ex);
        }
    }
}
