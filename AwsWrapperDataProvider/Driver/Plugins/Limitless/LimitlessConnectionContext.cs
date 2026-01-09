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
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.Plugins.Limitless;

public class LimitlessConnectionContext
{
    public HostSpec HostSpec { get; set; }
    public Dictionary<string, string> Props { get; set; }
    public DbConnection? Connection { get; set; }
    public ADONetDelegate<DbConnection> ConnectFunc { get; set; }
    public IList<HostSpec>? LimitlessRouters { get; set; }
    public IConnectionPlugin Plugin { get; set; }

    public LimitlessConnectionContext(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        DbConnection? connection,
        ADONetDelegate<DbConnection> connectFunc,
        IList<HostSpec>? limitlessRouters,
        IConnectionPlugin plugin)
    {
        this.HostSpec = hostSpec;
        this.Props = props;
        this.Connection = connection;
        this.ConnectFunc = connectFunc;
        this.LimitlessRouters = limitlessRouters;
        this.Plugin = plugin;
    }

    public void SetConnection(DbConnection? connection)
    {
        if (this.Connection != null && this.Connection != connection)
        {
            try
            {
                this.Connection.Close();
            }
            catch
            {
                // ignore
            }
        }

        this.Connection = connection;
    }
}
