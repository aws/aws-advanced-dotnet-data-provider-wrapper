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
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;

public abstract class SamlCredentialsProviderFactory : CredentialsProviderFactory
{
    private class SamlAWSCredentialsProvider(AWSCredentials credentials) : AWSCredentialsProvider
    {
        private readonly AWSCredentials credentials = credentials;

        public override AWSCredentials GetAWSCredentials()
        {
            return this.credentials;
        }
    }

    public override AWSCredentialsProvider GetAwsCredentialsProvider(string host, RegionEndpoint region, Dictionary<string, string> props)
    {
        string samlAssertion = this.GetSamlAssertion(props);
        string roleArn = PropertyDefinition.IamRoleArn.GetString(props) ?? throw new Exception("Missing IAM role ARN");
        string principalArn = PropertyDefinition.IamIdpArn.GetString(props) ?? throw new Exception("Missing IAM IDP ARN");

        AmazonSecurityTokenServiceClient client = new(region);

        AssumeRoleWithSAMLResponse result = client.AssumeRoleWithSAMLAsync(
            new AssumeRoleWithSAMLRequest { SAMLAssertion = samlAssertion, RoleArn = roleArn, PrincipalArn = principalArn })
            .GetAwaiter().GetResult();

        return new SamlAWSCredentialsProvider(result.Credentials);
    }

    public abstract string GetSamlAssertion(Dictionary<string, string> props);
}
