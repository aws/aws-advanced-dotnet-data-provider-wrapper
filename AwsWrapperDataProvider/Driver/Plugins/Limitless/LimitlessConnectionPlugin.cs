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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Limitless;

public class LimitlessConnectionPlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<LimitlessConnectionPlugin> Logger = LoggerUtils.GetLogger<LimitlessConnectionPlugin>();

    private static readonly IReadOnlySet<string> SubscribedMethodsSet = new HashSet<string>
    {
        "DbConnection.Open",
        "DbConnection.OpenAsync",

    };

    private readonly IPluginService _pluginService;
    private readonly Dictionary<string, string> _properties;
    private readonly Func<ILimitlessRouterService> _limitlessRouterServiceSupplier;
    private ILimitlessRouterService? _limitlessRouterService;

    public override IReadOnlySet<string> SubscribedMethods => SubscribedMethodsSet;

    public LimitlessConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> properties)
        : this(pluginService, properties, () => new LimitlessRouterService(pluginService))
    {
    }

    public LimitlessConnectionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> properties,
        Func<ILimitlessRouterService> limitlessRouterServiceSupplier)
    {
        this._pluginService = pluginService;
        this._properties = properties;
        this._limitlessRouterServiceSupplier = limitlessRouterServiceSupplier;
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        if (hostSpec == null)
        {
            throw new ArgumentNullException(nameof(hostSpec));
        }

        DbConnection? conn = null;

        IDialect dialect = this._pluginService.Dialect;
        if (dialect is not IAuroraLimitlessDialect)
        {
            conn = await methodFunc();
            var refreshedDialect = this._pluginService.Dialect;
            if (refreshedDialect is not IAuroraLimitlessDialect)
            {
                throw new NotSupportedException(string.Format(Resources.Error_UnsupportedDialectOrDatabase, refreshedDialect?.GetType().Name));
            }
        }

        this.InitLimitlessRouterMonitorService();
        if (isInitialConnection)
        {
            int intervalMs = PropertyDefinition.LimitlessIntervalMs.GetInt(props) ?? 7500;
            this._limitlessRouterService!.StartMonitoring(hostSpec, props, intervalMs);
        }

        var context = new LimitlessConnectionContext(
            hostSpec,
            props,
            conn,
            methodFunc,
            null,
            this);

        await this._limitlessRouterService!.EstablishConnection(context);

        if (context.Connection != null)
        {
            return context.Connection;
        }

        throw new AwsWrapperDbException(string.Format(Resources.Error_FailedToConnectToHost, hostSpec.Host));
    }

    private void InitLimitlessRouterMonitorService()
    {
        this._limitlessRouterService ??= this._limitlessRouterServiceSupplier();
    }
}
