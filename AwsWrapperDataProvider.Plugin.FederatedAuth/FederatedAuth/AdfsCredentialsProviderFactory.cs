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

using System.Text;
using System.Text.RegularExpressions;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

public partial class AdfsCredentialsProviderFactory(IPluginService pluginService) : SamlCredentialsProviderFactory
{
    [GeneratedRegex("<input(.+?)/>", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-CA")]
    private static partial Regex InputTagPattern();

    [GeneratedRegex("<form.*?action=\"([^\"]+)\"", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-CA")]
    private static partial Regex FormActionPattern();

    private readonly IPluginService pluginService = pluginService;

    public static readonly string IdpName = "adfs";

    private static string GetSignInPageBody(HttpClient httpClient, string uri)
    {
        SamlUtils.ValidateUrl(uri);
        try
        {
            HttpResponseMessage resp = httpClient.GetAsync(uri).GetAwaiter().GetResult();

            return !resp.IsSuccessStatusCode
                ? throw new Exception("Sign on page request failed.")
                : resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            throw new Exception("Sign on page request failed.", ex);
        }
    }

    private static string GetFormActionBody(HttpClient httpClient, string uri, Dictionary<string, string> parameters)
    {
        SamlUtils.ValidateUrl(uri);
        try
        {
            HttpResponseMessage resp = httpClient.PostAsync(uri, new FormUrlEncodedContent(parameters))
                .GetAwaiter().GetResult();

            return !resp.IsSuccessStatusCode
                ? throw new Exception("Sign on page request failed.")
                : resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            throw new Exception("Sign on page request failed.", ex);
        }
    }

    private static string GetSignInPageUrl(Dictionary<string, string> props)
    {
        return "https://" + PropertyDefinition.IdpEndpoint.GetString(props) + ':'
            + PropertyDefinition.IdpPort.GetString(props) + "/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp="
            + PropertyDefinition.RelayingPartyId.GetString(props);
    }

    private static string GetFormActionUrl(Dictionary<string, string> props, string action)
    {
        return "https://" + PropertyDefinition.IdpEndpoint.GetString(props) + ':'
            + PropertyDefinition.IdpPort.GetString(props) + action;
    }

    private static List<string> GetInputTagsFromHTML(string body)
    {
        var matches = InputTagPattern().Matches(body);
        HashSet<string> distinctInputTags = [];
        List<string> inputTags = [];

        foreach (Match match in matches)
        {
            string tag = match.Groups[0].Value;
            string tagNameLower = GetValueByKey(tag, "name").ToLower();
            if (tagNameLower.Length > 0 && distinctInputTags.Add(tagNameLower))
            {
                inputTags.Add(tag);
            }
        }

        return inputTags;
    }

    private static string GetValueByKey(string input, string key)
    {
        Regex keyValuePattern = new("(" + Regex.Escape(key) + ")\\s*=\\s*\"(.*?)\"");
        Match? match = keyValuePattern.Match(input);
        return match == null ? string.Empty : match.Groups[2].Value;
    }

    private static string EscapeHtmlEntity(string html)
    {
        StringBuilder sb = new(html.Length);
        int i = 0;
        int length = html.Length;
        while (i < length)
        {
            char c = html[i];
            if (c != '&')
            {
                sb.Append(c);
                i++;
                continue;
            }

            if (html[i..].StartsWith("&amp;"))
            {
                sb.Append('&');
                i += 5;
            }
            else if (html[i..].StartsWith("&apos;"))
            {
                sb.Append('\'');
                i += 6;
            }
            else if (html[i..].StartsWith("&quot;"))
            {
                sb.Append('"');
                i += 6;
            }
            else if (html[i..].StartsWith("&lt;"))
            {
                sb.Append('<');
                i += 4;
            }
            else if (html[i..].StartsWith("&gt;"))
            {
                sb.Append('>');
                i += 4;
            }
            else
            {
                sb.Append(c);
                ++i;
            }
        }

        return sb.ToString();
    }

    private static Dictionary<string, string> GetParametersFromHtmlBody(string body, Dictionary<string, string> props)
    {
        Dictionary<string, string> parameters = [];
        foreach (string inputTag in GetInputTagsFromHTML(body))
        {
            string name = GetValueByKey(inputTag, "name");
            string value = GetValueByKey(inputTag, "value");
            string nameLower = name.ToLower();

            if (nameLower.Contains("username"))
            {
                parameters[name] = PropertyDefinition.IdpUsername.GetString(props) ?? string.Empty;
            }
            else if (nameLower.Contains("authmethod"))
            {
                parameters[name] = value;
            }
            else if (nameLower.Contains("password"))
            {
                parameters[name] = PropertyDefinition.IdpPassword.GetString(props) ?? string.Empty;
            }
            else if (name.Length > 0)
            {
                parameters[name] = value;
            }
        }

        return parameters;
    }

    private static string GetFormActionFromHtmlBody(string body)
    {
        Match? match = FormActionPattern().Match(body);
        return match == null ? string.Empty : EscapeHtmlEntity(match.Groups[1].Value);
    }

    public override string GetSamlAssertion(Dictionary<string, string> props)
    {
        int connectionTimeoutMs = PropertyDefinition.HttpClientConnectTimeout.GetInt(props) ?? FederatedAuthPlugin.DefaultHttpTimeoutMs;
        HttpClient httpClient = HttpClientFactory.GetDisposableHttpClient(connectionTimeoutMs);

        string uri = GetSignInPageUrl(props);
        string signInPageBody = GetSignInPageBody(httpClient, uri);
        string action = GetFormActionFromHtmlBody(signInPageBody);

        if (action.Length > 0 && action.StartsWith('/'))
        {
            uri = GetFormActionUrl(props, action);
        }

        Dictionary<string, string> parameters = GetParametersFromHtmlBody(signInPageBody, props);
        string content = GetFormActionBody(httpClient, uri, parameters);

        Match samlMatch = FederatedAuthPlugin.SamlResponsePattern().Match(content) ?? throw new Exception("Failed login");

        httpClient.Dispose();
        return samlMatch.Groups[FederatedAuthPlugin.SamlResponsePatternGroup].Value;
    }
}
