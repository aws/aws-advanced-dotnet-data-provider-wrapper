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


using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.driver.hostInfo;

namespace AwsWrapperDataProvider.driver.targetDriverDialects;

public class PgTargetDriverDialect : ITargetDriverDialect
{
    protected static HashSet<string> wrapperParameterNames = new(["targetConnectionType", "targetCommandType", "targetParameterType"]);
    
    public bool IsDialect(Type connectionType)
    {
        throw new NotImplementedException();
    }
    
    public string PrepareConnectionString(
        HostSpec hostSpec,
        Dictionary<string, string> props)
    {
        // TODO: proper 
        Dictionary<string, string> targetConnectionParameters = props.Where(x => !wrapperParameterNames.Contains(x.Key)).ToDictionary();
        return string.Join("; ", targetConnectionParameters.Select(x => $"{x.Key}={x.Value}"));
    }
}