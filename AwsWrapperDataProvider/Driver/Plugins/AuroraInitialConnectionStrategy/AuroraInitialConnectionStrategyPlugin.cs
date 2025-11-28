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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider.Driver.Plugins.AuroraInitialConnectionStrategy;

public class AuroraInitialConnectionStrategyPlugin : AbstractConnectionPlugin
{
    private const int DnsRetries = 3;

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

    public override async Task InitHostProvider(
        string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        ADONetDelegate initHostProviderFunc)
    {
        this.hostListProviderService = hostListProviderService;

        if (this.hostListProviderService.IsStaticHostListProvider())
        {
            throw new Exception(Resources.AuroraInitialConnectionStrategyPlugin_InitHostProvider_RequiresDynamicProvider);
        }

        await initHostProviderFunc.Invoke();
    }

    public override async Task<DbConnection> OpenConnection(
        HostSpec? hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
    {
        RdsUrlType urlType = RdsUtils.IdentifyRdsType(hostSpec?.Host);
        DbConnection? connectionCandidate = null;

        if (urlType == RdsUrlType.RdsWriterCluster ||
            (isInitialConnection && this.verifyOpenedConnectionType == VerifyOpenedConnectionType.Writer))
        {
            connectionCandidate = await this.GetVerifiedWriterConnection(
                props,
                isInitialConnection,
                methodFunc,
                async);
        }
        else if (urlType == RdsUrlType.RdsReaderCluster ||
            (isInitialConnection && this.verifyOpenedConnectionType == VerifyOpenedConnectionType.Reader))
        {
            connectionCandidate = await this.GetVerifiedReaderConnection(
                props,
                isInitialConnection,
                methodFunc,
                async);
        }

        if (connectionCandidate == null)
        {
            return await methodFunc();
        }

        return connectionCandidate;
    }

    private async Task<DbConnection?> GetVerifiedWriterConnection(
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
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
                    try
                    {
                        writerConnectionCandidate = await methodFunc();
                    }
                    catch (InvalidOpenConnectionException)
                    {
                        writerConnectionCandidate?.Dispose();
                        writerConnectionCandidate = await this.RetryInvalidOpenConnection(props);

                        if (writerConnectionCandidate == null)
                        {
                            throw;
                        }
                    }

                    await this.pluginService.ForceRefreshHostListAsync(writerConnectionCandidate);
                    writerCandidate = await this.pluginService.IdentifyConnectionAsync(writerConnectionCandidate);

                    if (writerCandidate == null || writerCandidate.Role != HostRole.Writer)
                    {
                        // Shouldn't be here. But let's try again.
                        this.DisposeConnection(writerConnectionCandidate);
                        await Task.Delay(retryDelay);
                        continue;
                    }

                    if (isInitialConnection)
                    {
                        this.hostListProviderService.InitialConnectionHostSpec = writerCandidate;
                    }

                    return writerConnectionCandidate;
                }

                writerConnectionCandidate = await this.pluginService.OpenConnection(writerCandidate, props, this, async);

                if ((await this.pluginService.GetHostRole(writerConnectionCandidate)) != HostRole.Writer)
                {
                    await this.pluginService.ForceRefreshHostListAsync(writerConnectionCandidate);
                    this.DisposeConnection(writerConnectionCandidate);
                    await Task.Delay(retryDelay);
                    continue;
                }

                if (isInitialConnection)
                {
                    this.hostListProviderService.InitialConnectionHostSpec = writerCandidate;
                }

                return writerConnectionCandidate;
            }
            catch (DbException dbException)
            {
                this.DisposeConnection(writerConnectionCandidate);
                if (this.pluginService.IsLoginException(dbException))
                {
                    throw;
                }

                if (writerCandidate != null)
                {
                    this.pluginService.SetAvailability(writerCandidate.AsAliases(), HostAvailability.Unavailable);
                }
            }
            catch
            {
                this.DisposeConnection(writerConnectionCandidate);
                throw;
            }
        }

        return null;
    }

    private async Task<DbConnection?> GetVerifiedReaderConnection(
        Dictionary<string, string> props,
        bool isInitialConnection,
        ADONetDelegate<DbConnection> methodFunc,
        bool async)
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
                    try
                    {
                        readerConnectionCandidate = await methodFunc();
                    }
                    catch (InvalidOpenConnectionException)
                    {
                        readerConnectionCandidate?.Dispose();
                        readerConnectionCandidate = await this.RetryInvalidOpenConnection(props);

                        if (readerConnectionCandidate == null)
                        {
                            throw;
                        }
                    }

                    await this.pluginService.ForceRefreshHostListAsync(readerConnectionCandidate);
                    readerCandidate = await this.pluginService.IdentifyConnectionAsync(readerConnectionCandidate);

                    if (readerCandidate == null)
                    {
                        this.DisposeConnection(readerConnectionCandidate);
                        await Task.Delay(retryDelay);
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

                        this.DisposeConnection(readerConnectionCandidate);
                        await Task.Delay(retryDelay);
                        continue;
                    }

                    if (isInitialConnection)
                    {
                        this.hostListProviderService.InitialConnectionHostSpec = readerCandidate;
                    }

                    return readerConnectionCandidate;
                }

                readerConnectionCandidate = await this.pluginService.OpenConnection(readerCandidate, props, this, async);

                if ((await this.pluginService.GetHostRole(readerConnectionCandidate)) != HostRole.Reader)
                {
                    await this.pluginService.ForceRefreshHostListAsync(readerConnectionCandidate);

                    if (this.HasNoReader())
                    {
                        if (isInitialConnection)
                        {
                            this.hostListProviderService.InitialConnectionHostSpec = readerCandidate;
                        }

                        return readerConnectionCandidate;
                    }

                    this.DisposeConnection(readerConnectionCandidate);
                    await Task.Delay(retryDelay);
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
                this.DisposeConnection(readerConnectionCandidate);
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
                this.DisposeConnection(readerConnectionCandidate);
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
        string strategy = PropertyDefinition.ReaderHostSelectorStrategy.GetString(props)!;
        if (this.pluginService.AcceptsStrategy(strategy))
        {
            try
            {
                return this.pluginService.GetHostSpecByStrategy(HostRole.Reader, strategy);
            }
            catch (DbException)
            {
                return null;
            }
        }

        throw new InvalidOperationException(string.Format(Resources.AuroraInitialConnectionStrategyPlugin_GetReader_InvalidHostSelectionStrategy, strategy));
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

    private void DisposeConnection(DbConnection? connection)
    {
        try
        {
            connection?.Dispose();
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

    private async Task<DbConnection?> RetryInvalidOpenConnection(Dictionary<string, string> props)
    {
        DbConnection? connection = null;

        for (int attempt = 1; attempt <= DnsRetries; attempt++)
        {
            connection?.Dispose();
            connection = await this.pluginService.ForceOpenConnection(this.pluginService.CurrentHostSpec!, props, this, true);

            if (this.pluginService.TargetConnectionDialect.Ping(connection).ConnectionAlive)
            {
                return connection;
            }

            if (attempt == DnsRetries)
            {
                break;
            }

            Thread.Sleep(100);
        }

        return null;
    }
}
