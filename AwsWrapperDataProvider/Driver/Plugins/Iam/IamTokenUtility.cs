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
using Amazon.RDS.Util;
using Amazon.Runtime;
using AwsWrapperDataProvider.Driver.Plugins.Efm;

namespace AwsWrapperDataProvider.Driver.Plugins.Iam;

public class IamTokenUtility : IIamTokenUtility
{
    public string GetCacheKey(string user, string hostname, int port, string region)
    {
        return user + ":" + hostname + ":" + port + ":" + region;
    }

    public string GenerateAuthenticationToken(string region, string hostname, int port, string user, AWSCredentials? credentials)
    {
        try
        {
            RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(region);

            if (credentials != null)
            {
                return RDSAuthTokenGenerator.GenerateAuthToken(credentials, regionEndpoint, hostname, port, user);
            }
            else
            {
                // load aws credentials from system if possible
                return RDSAuthTokenGenerator.GenerateAuthToken(regionEndpoint, hostname, port, user);
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Couldn't generate token for IAM authentication.", ex);
        }
    }
}
