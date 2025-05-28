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

namespace AwsWrapperDataProvider.Driver.Plugins.Iam;

public class IamTokenCache
{
    private struct Token
    {
        public string Value;
        public DateTime ExpirationTime;
    }

    private readonly int cleanUpIntervalMinutes = 10; // clean up expired tokens every 10 minutes

    private Dictionary<string, Token> tokenCache = new Dictionary<string, Token>();

    private DateTime cleanUpTime = DateTime.Now;

    public static string GetCacheKey(string user, string hostname, int port, string region)
    {
        return user + ":" + hostname + ":" + port + ":" + region;
    }

    public string? GetToken(string cacheKey)
    {
        bool tokenExists = this.tokenCache.TryGetValue(cacheKey, out Token token);

        this.CleanUp();
        return tokenExists && DateTime.Now < token.ExpirationTime ? token.Value : null;
    }

    public void SetToken(string cacheKey, string tokenValue, int tokenExpirationSeconds)
    {
        Token newToken;
        newToken.Value = tokenValue;
        newToken.ExpirationTime = DateTime.Now.AddSeconds(tokenExpirationSeconds);

        this.tokenCache[cacheKey] = newToken;
        this.CleanUp();
    }

    private void CleanUp()
    {
        if (DateTime.Now > this.cleanUpTime)
        {
            foreach (var (cacheKey, token) in this.tokenCache)
            {
                // remove expired tokens
                if (DateTime.Now > token.ExpirationTime)
                {
                    this.tokenCache.Remove(cacheKey);
                }
            }

            this.cleanUpTime = DateTime.Now.AddMinutes(this.cleanUpIntervalMinutes);
        }
    }
}
