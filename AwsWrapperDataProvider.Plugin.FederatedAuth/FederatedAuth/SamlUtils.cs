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

using System.Text.RegularExpressions;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

public partial class SamlUtils
{
    [GeneratedRegex("^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex HttpsUrlPattern();

    public static void CheckIdpCredentialsWithFallback(AwsWrapperProperty idpUserNameProperty, AwsWrapperProperty idpPasswordProperty, Dictionary<string, string> props)
    {
        if (idpUserNameProperty.GetString(props) == null)
        {
            props[idpUserNameProperty.Name] = PropertyDefinition.User.GetString(props) ?? throw new Exception("No IDP user name provided.");
        }

        if (idpPasswordProperty.GetString(props) == null)
        {
            props[idpPasswordProperty.Name] = PropertyDefinition.Password.GetString(props) ?? throw new Exception("No IDP password provided.");
        }
    }

    public static void ValidateUrl(string paramString)
    {
        Uri.TryCreate(paramString, UriKind.RelativeOrAbsolute, out Uri? authorizeRequestUrl);

        if (authorizeRequestUrl == null || !authorizeRequestUrl.Scheme.Equals("https", StringComparison.CurrentCultureIgnoreCase))
        {
            throw new Exception("Invalid url");
        }

        _ = HttpsUrlPattern().Match(paramString) ?? throw new Exception("Invalid url");
    }
}
