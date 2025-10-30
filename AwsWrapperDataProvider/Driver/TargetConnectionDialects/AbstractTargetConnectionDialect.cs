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

using System.Data.Common;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.TargetConnectionDialects;

public abstract class AbstractTargetConnectionDialect : ITargetConnectionDialect
{
    private const string DefaultPluginCode = "initialConnection, efm,failover";

    public abstract Type DriverConnectionType { get; }

    public bool IsDialect(Type connectionType)
    {
        return connectionType == this.DriverConnectionType;
    }

    public abstract string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props);

    public ISet<string> GetAllowedOnConnectionMethodNames()
    {
        throw new NotImplementedException("Will implement in Milestone 5, as feature is only relevant to Failover.");
    }

    public virtual string GetPluginCodesOrDefault(Dictionary<string, string> props)
    {
        return PropertyDefinition.Plugins.GetString(props) ?? DefaultPluginCode;
    }

    protected string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props, AwsWrapperProperty hostProperty)
    {
        Dictionary<string, string> targetConnectionParameters = props.Where(x =>
            !PropertyDefinition.InternalWrapperProperties
                .Select(prop => prop.Name)
                .Contains(x.Key)).ToDictionary();

        if (hostSpec != null)
        {
            dialect.PrepareConnectionProperties(targetConnectionParameters, hostSpec);
            hostProperty.Set(targetConnectionParameters, hostSpec.Host);
            if (hostSpec.IsPortSpecified)
            {
                PropertyDefinition.Port.Set(targetConnectionParameters, hostSpec.Port.ToString());
            }
        }

        return string.Join("; ", targetConnectionParameters.Select(x => $"{x.Key}={x.Value}"));
    }
}
