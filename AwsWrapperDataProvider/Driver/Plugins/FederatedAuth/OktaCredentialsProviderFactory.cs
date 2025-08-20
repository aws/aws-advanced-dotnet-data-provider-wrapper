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

using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;

public partial class OktaCredentialsProviderFactory(IPluginService pluginService) : SamlCredentialsProviderFactory
{
    private static readonly string SessionTokenGroup = "token";
    private static readonly string AttributesGroup = "attributes";
    private static readonly string ValueGroup = "value";

    [GeneratedRegex(@".*""sessionToken""\s*:\s*""(?<token>[^""]+)"".*", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-CA")]
    private static partial Regex SessionTokenPattern();

    [GeneratedRegex(@"<(?<attributes>[^>]*\s+name\s*=\s*[""']SAMLResponse[""'][^>]*)>", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-CA")]
    private static partial Regex SamlResponseTagPattern();

    [GeneratedRegex(@".*\s+value\s*=\s*[""'](?<value>[^""']+)[""'].*", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-CA")]
    private static partial Regex ValueAttributePattern();

    private static readonly string OktaAwsAppName = "amazon_aws";
    private static readonly string OneTimeToken = "onetimetoken";
    private static readonly ILogger<OktaCredentialsProviderFactory> Logger = LoggerUtils.GetLogger<OktaCredentialsProviderFactory>();
    private readonly IPluginService pluginService = pluginService;

    private static string GetSessionToken(HttpClient httpClient, Dictionary<string, string> props)
    {
        string idpHost = PropertyDefinition.IdpEndpoint.GetString(props) ?? throw new Exception("IDP Endpoint not provided.");
        string idpUser = PropertyDefinition.IdpUsername.GetString(props) ?? throw new Exception("IDP Username not provided.");
        string idpPassword = PropertyDefinition.IdpPassword.GetString(props) ?? throw new Exception("IDP Password not provided.");

        string sessionTokenEndpoint = "https://" + idpHost + "/api/v1/authn";

        try
        {
            string requestBody = $"{{\"username\":\"{idpUser}\",\"password\":\"{idpPassword}\"}}";
            HttpContent content = new StringContent(requestBody, Encoding.UTF8, "application/json");

            HttpResponseMessage resp = httpClient.PostAsync(sessionTokenEndpoint, content).GetAwaiter().GetResult();

            if (!resp.IsSuccessStatusCode)
            {
                throw new Exception("OKTA session token request failed.");
            }

            string responseJson = resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();

            Match match = SessionTokenPattern().Match(responseJson);

            if (match.Success)
            {
                return match.Groups[SessionTokenGroup].Value;
            }

            throw new Exception("OKTA session token not found in response.");
        }
        catch (Exception ex)
        {
            throw new Exception("Failed to get session token.", ex);
        }
    }

    private static string GetSamlUrl(Dictionary<string, string> props)
    {
        string idpHost = PropertyDefinition.IdpEndpoint.GetString(props) ?? throw new Exception("IDP Endpoint not provided.");
        string appId = PropertyDefinition.AppId.GetString(props) ?? throw new Exception("App ID not provided.");
        string baseUri = "https://" + idpHost + "/app/" + OktaAwsAppName + "/" + appId + "/sso/saml";
        SamlUtils.ValidateUrl(baseUri);
        return baseUri;
    }

    public override string GetSamlAssertion(Dictionary<string, string> props)
    {
        try
        {
            HttpClient httpClient = HttpClientFactory.GetDisposableHttpClient(10000);
            string sessionToken = GetSessionToken(httpClient, props);
            string baseUri = GetSamlUrl(props);

            // construct a new URI using the base URI and the session token
            UriBuilder uriBuilder = new(baseUri);
            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            query[OneTimeToken] = sessionToken;
            uriBuilder.Query = query.ToString();

            string samlRequestUri = uriBuilder.ToString();

            HttpResponseMessage resp = httpClient.GetAsync(samlRequestUri).GetAwaiter().GetResult();

            if (!resp.IsSuccessStatusCode)
            {
                throw new Exception("OKTA SAML request failed.");
            }

            string responseHtml = resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();

            MatchCollection matches = SamlResponseTagPattern().Matches(responseHtml);

            foreach (Match match in matches)
            {
                Match samlResponseValue = ValueAttributePattern().Match(match.Groups[AttributesGroup].Value);

                if (samlResponseValue.Success)
                {
                    // WebUtility.HtmlDecode will convert entities such as &#x3d; to =, which is necessary for the base 64 SAML assertion
                    return WebUtility.HtmlDecode(samlResponseValue.Groups[ValueGroup].Value);
                }
            }

            throw new Exception("OKTA SAML response value not found in response HTML.");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Failed to get SAML assertion: {ex.Message}");
            throw new Exception("Failed to get SAML assertion.", ex);
        }
    }
}
