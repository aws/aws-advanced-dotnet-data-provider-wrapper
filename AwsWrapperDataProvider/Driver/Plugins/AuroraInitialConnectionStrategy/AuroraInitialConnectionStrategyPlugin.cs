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
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using Org.BouncyCastle.Crypto;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraInitialConnectionStrategy;

public class AuroraInitialConnectionStrategyPlugin : AbstractConnectionPlugin
{
    private readonly IPluginService pluginService;
    private readonly VerifyOpenedConnectionType? verifyOpenedConnectionType;

    private IHostListProviderService? hostListProviderService;
    public AuroraInitialConnectionStrategyPlugin(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        string? verifyOpenedConnectionTypeStr = PropertyDefinition.VerifyOpenedConnectionType.GetString(props);
        this.verifyOpenedConnectionType = verifyOpenedConnectionTypeStr != null ? this.verifyOpenedConnectionTypeMap[verifyOpenedConnectionTypeStr] : null;
    }

    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string> { "DbConnection.Open", "DbConnection.OpenAsync", "initHostProvider" };

    public override void InitHostProvider(
        string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        ADONetDelegate initHostProviderFunc)
    {
        this.hostListProviderService = hostListProviderService;

        if (this.hostListProviderService.IsStaticHostListProvider())
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
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);

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
                    writerConnectionCandidate = this.pluginService.CurrentConnection ??
                                                throw new Exception("Could not find connection.");
                    this.pluginService.ForceRefreshHostList(writerConnectionCandidate);
                    writerCandidate = this.pluginService.IdentifyConnection(writerConnectionCandidate);

                    if (writerCandidate == null || writerCandidate.Role != HostRole.Writer)
                    {
                        // Shouldn't be here. But let's try again.
                        this.CloseConnection(writerConnectionCandidate);
                        Task.Delay(retryDelay);
                        continue;
                    }

                    if (isInitialConnection)
                    {
                        this.hostListProviderService.InitialConnectionHostSpec = writerCandidate;
                    }

                    return writerConnectionCandidate;
                }

                this.pluginService.OpenConnection(writerCandidate, props, null);
                writerConnectionCandidate = this.pluginService.CurrentConnection
                    ?? throw new Exception("Could not find connection.");

                if (this.pluginService.GetHostRole(writerConnectionCandidate) != HostRole.Writer)
                {
                    this.pluginService.ForceRefreshHostList(writerConnectionCandidate);
                    this.CloseConnection(writerConnectionCandidate);
                    Task.Delay(retryDelay);
                }

                if (isInitialConnection)
                {
                    this.hostListProviderService.InitialConnectionHostSpec = writerCandidate;
                }

                return writerConnectionCandidate;
            }
            catch (DbException dbException)
            {
                this.CloseConnection(writerConnectionCandidate);
                if (this.pluginService.IsLoginException(dbException))
                {
                    throw;
                }

                if (writerCandidate != null)
                {
                    this.pluginService.SetAvailability(writerCandidate.AsAliases(), HostAvailability.Unavailable);
                }
            }
            catch (Exception e)
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
        ArgumentNullException.ThrowIfNull(this.hostListProviderService);

        int retryDelay = (int)PropertyDefinition.OpenConnectionRetryIntervalMs.GetInt(props)!;
        long endTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                         + (long)PropertyDefinition.OpenConnectionRetryTimeoutMs.GetLong(props)!;

        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < endTimeMs)
        {
            DbConnection? readerConnectionCandidate = null;
            HostSpec? readerCandidate = null;

            try
            {
                readerCandidate = this.GetReader(props);

                if (readerCandidate == null || RdsUtils.IsRdsClusterDns(readerCandidate.Host))
                {
                    methodFunc();
                    readerConnectionCandidate = this.pluginService.CurrentConnection ??
                                                throw new Exception("Could not find connection.");
                    this.pluginService.ForceRefreshHostList(readerConnectionCandidate);
                    readerCandidate = this.pluginService.IdentifyConnection(readerConnectionCandidate);

                    if (readerCandidate == null)
                    {
                        this.CloseConnection(readerConnectionCandidate);
                        Task.Delay(retryDelay);
                        continue;
                    }

                    if (readerCandidate.Role != HostRole.Reader)
                    {
                        if (this.HasNoReader())
                        {
                            if (isInitialConnection)
                            {
                                this.hostListProviderService.InitialConnectionHostSpec = readerCandidate;
                            }

                            return readerConnectionCandidate;
                        }

                        this.CloseConnection(readerConnectionCandidate);
                        Task.Delay(retryDelay);
                        continue;
                    }

                    if (isInitialConnection)
                    {
                        this.hostListProviderService.InitialConnectionHostSpec = readerCandidate;
                    }

                    return readerConnectionCandidate;
                }

                this.pluginService.OpenConnection(readerCandidate, props, null);
                readerConnectionCandidate = this.pluginService.CurrentConnection!;

                if (this.pluginService.GetHostRole(readerConnectionCandidate) != HostRole.Reader)
                {
                    this.pluginService.ForceRefreshHostList(readerConnectionCandidate);

                    if (this.HasNoReader())
                    {
                        if (isInitialConnection)
                        {
                            this.hostListProviderService.InitialConnectionHostSpec = readerCandidate;
                        }

                        return readerConnectionCandidate;
                    }

                    this.CloseConnection(readerConnectionCandidate);
                    Task.Delay(retryDelay);
                    continue;
                }

                if (isInitialConnection)
                {
                    this.hostListProviderService.InitialConnectionHostSpec = readerCandidate;
                }

                return readerConnectionCandidate;
            }
            catch (DbException dbException)
            {
                this.CloseConnection(readerConnectionCandidate);
                if (this.pluginService.IsLoginException(dbException))
                {
                    throw;
                }

                if (readerCandidate != null)
                {
                    this.pluginService.SetAvailability(readerCandidate.AsAliases(), HostAvailability.Unavailable);
                }
            }
            catch
            {
                this.CloseConnection(readerConnectionCandidate);
                throw;
            }
        }

        return null;
    }

    private HostSpec? GetWriter()
    {
        return this.pluginService.AllHosts.FirstOrDefault(host => host.Role == HostRole.Writer);
    }

    private HostSpec? GetReader(Dictionary<string, string> props)
    {
        string strategy = PropertyDefinition.ReaderHostSelectionStrategy.GetString(props)!;
        if (this.pluginService.AcceptsStrategy(HostRole.Reader, strategy))
        {
            try
            {
                return this.pluginService.GetHostSpecByStrategy(HostRole.Reader, strategy);
            }
            catch (DbException dbException)
            {
                return null;
            }
        }

        throw new InvalidOperationException($"Invalid host selection strategy: {strategy}");
    }

    private bool HasNoReader()
    {
        if (this.pluginService.AllHosts.Count == 0)
        {
            return false;
        }

        return this.pluginService.AllHosts
            .All(host => host.Role == HostRole.Writer);
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

    private readonly Dictionary<string, VerifyOpenedConnectionType> verifyOpenedConnectionTypeMap = new(StringComparer.OrdinalIgnoreCase)
    {
        { "writer", VerifyOpenedConnectionType.Writer },
        { "reader", VerifyOpenedConnectionType.Reader },
    };
}
