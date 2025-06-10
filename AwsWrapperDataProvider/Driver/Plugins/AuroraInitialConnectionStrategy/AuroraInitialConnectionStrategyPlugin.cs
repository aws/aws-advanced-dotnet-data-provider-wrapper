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
using AwsWrapperDataProvider.Driver.Utils;
using Org.BouncyCastle.Crypto;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraInitialConnectionStrategy;

public class AuroraInitialConnectionStrategyPlugin : AbstractConnectionPlugin
{
    //TODO: Add logger

    private IPluginService pluginService;
    private IHostListProviderService _hostListProviderService;
    private VerifyOpenedConnectionType? verifyOpenedConnectionType;

    public AuroraInitialConnectionStrategyPlugin(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        string? verifyOpenedConnectionTypeStr = PropertyDefinition.VerifyOpenedConnectionType.GetString(props);
        this.verifyOpenedConnectionType = verifyOpenedConnectionTypeStr != null ? this.verifyOpenedConnectionTypeMap[verifyOpenedConnectionTypeStr] : null;
    }

    public override ISet<string> SubscribedMethods { get; } =
        new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync" };

    public override void InitHostProvider(string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        ADONetDelegate<Action<object[]>> initHostProviderFunc)
    {
        this._hostListProviderService = hostListProviderService;

        if (hostListProviderService.IsStaticHostListProvider())
        {
            throw new Exception("AuroraInitialConnectionStrategyPlugin requires dynamic provider.");
        }

        initHostProviderFunc.Invoke();
    }

    public override void OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate methodFunc)
    {
        RdsUrlType urlType = RdsUtils.IdentifyRdsType(hostSpec?.Host);
        DbConnection? connectionCandidate = null;

        if (urlType == RdsUrlType.RdsWriterCluster ||
            (isInitialConnection && this.verifyOpenedConnectionType == VerifyOpenedConnectionType.Writer))
        {
            connectionCandidate = this.GetVerifiedWriterConnection(
                props,
                isInitialConnection,
                methodFunc);
        }
        else if (urlType == RdsUrlType.RdsReaderCluster ||
            (isInitialConnection && this.verifyOpenedConnectionType == VerifyOpenedConnectionType.Reader))
        {
            connectionCandidate = this.GetVerifiedReaderConnection(
                props,
                isInitialConnection,
                methodFunc);
        }

        if (connectionCandidate == null)
        {
            methodFunc();
            return;
        }

        this.pluginService.CurrentConnection = connectionCandidate;
    }

    private DbConnection? GetVerifiedWriterConnection(
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate methodFunc)
    {
        int retryDelay = (int)PropertyDefinition.OpenConnectionRetryIntervalMs.GetInt(props)!;
        long endTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                         + (long)PropertyDefinition.OpenConnectionRetryTimeoutMs.GetLong(props)!;

        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < endTimeMs)
        {
            DbConnection? writerConnectionCandidate = null;
            HostSpec? writerCandidate = null;

            try
            {
                writerCandidate = this.GetWriter();

                if (writerCandidate == null || RdsUtils.IsRdsClusterDns(writerCandidate.Host))
                {
                    methodFunc();
                    writerConnectionCandidate = this.pluginService.CurrentConnection ?? throw new Exception("Could not find connection.");
                    this.pluginService.ForceRefreshHostList(writerConnectionCandidate);
                    writerCandidate = this.pluginService.IdentifyConnection(writerConnectionCandidate);

                    if (writerCandidate == null || writerCandidate.Role != HostRole.Writer) {
                        // Shouldn't be here. But let's try again.
                        this.CloseConnection(writerConnectionCandidate);
                        Task.Delay(retryDelay);
                        continue;
                    }

                    if (isInitialConnection)
                    {
                        this._hostListProviderService.InitialConnectionHostSpec = writerCandidate;
                    }

                    return writerConnectionCandidate;
                }

                this.pluginService.OpenConnection(writerCandidate, props, null);
                writerConnectionCandidate = this.pluginService.CurrentConnection;

                if (this.pluginService.GetHostRole(writerConnectionCandidate) != HostRole.Writer)
                {
                    this.pluginService.ForceRefreshHostList(writerConnectionCandidate);
                    this.CloseConnection(writerConnectionCandidate);
                    Task.Delay(retryDelay);
                }

                if (isInitialConnection)
                {
                    this._hostListProviderService.InitialConnectionHostSpec = writerCandidate;
                }

                return writerConnectionCandidate;
            }
            catch (Exception exception)
            {
                this.CloseConnection(writerConnectionCandidate);
                throw;
            }
        }

        return null;
    }

    private DbConnection? GetVerifiedReaderConnection(
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate methodFunc)
    {
        throw new NotImplementedException();
    }

    private HostSpec? GetWriter()
    {
        return this.pluginService.AllHosts.FirstOrDefault(host => host.Role == HostRole.Writer);
    }

    private void CloseConnection(DbConnection? connection)
    {
        try
        {
            connection?.Close();
        }
        catch
        {
            // do nothing.
        }
    }

    private enum VerifyOpenedConnectionType
    {
        Writer,
        Reader,
    }

    private Dictionary<string, VerifyOpenedConnectionType> verifyOpenedConnectionTypeMap = new(StringComparer.OrdinalIgnoreCase)
    {
        { "writer", VerifyOpenedConnectionType.Writer },
        { "reader", VerifyOpenedConnectionType.Reader },
    };
}
