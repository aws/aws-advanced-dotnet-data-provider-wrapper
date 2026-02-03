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
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider;

public class AwsWrapperConnectionStringBuilder : DbConnectionStringBuilder
{
    public string? Host
    {
        get => this.GetValue(PropertyDefinition.Host.Name);
        set => this.SetValue(PropertyDefinition.Host.Name, value);
    }

    public int? Port
    {
        get => this.GetIntValue(PropertyDefinition.Port.Name);
        set => this.SetValue(PropertyDefinition.Port.Name, value?.ToString());
    }

    public string? Username
    {
        get => this.GetValue(PropertyDefinition.User.Name);
        set => this.SetValue(PropertyDefinition.User.Name, value);
    }

    public string? Password
    {
        get => this.GetValue(PropertyDefinition.Password.Name);
        set => this.SetValue(PropertyDefinition.Password.Name, value);
    }

    public string? TargetConnectionType
    {
        get => this.GetValue(PropertyDefinition.TargetConnectionType.Name);
        set => this.SetValue(PropertyDefinition.TargetConnectionType.Name, value);
    }

    public string? TargetCommandType
    {
        get => this.GetValue(PropertyDefinition.TargetCommandType.Name);
        set => this.SetValue(PropertyDefinition.TargetCommandType.Name, value);
    }

    public string? TargetDialect
    {
        get => this.GetValue(PropertyDefinition.TargetDialect.Name);
        set => this.SetValue(PropertyDefinition.TargetDialect.Name, value);
    }

    public string? CustomTargetConnectionDialect
    {
        get => this.GetValue(PropertyDefinition.CustomTargetConnectionDialect.Name);
        set => this.SetValue(PropertyDefinition.CustomTargetConnectionDialect.Name, value);
    }

    public string? Plugins
    {
        get => this.GetValue(PropertyDefinition.Plugins.Name);
        set => this.SetValue(PropertyDefinition.Plugins.Name, value);
    }

    public bool? AutoSortPluginOrder
    {
        get => this.GetBoolValue(PropertyDefinition.AutoSortPluginOrder.Name);
        set => this.SetValue(PropertyDefinition.AutoSortPluginOrder.Name, value?.ToString());
    }

    public bool? SingleWriterConnectionString
    {
        get => this.GetBoolValue(PropertyDefinition.SingleWriterConnectionString.Name);
        set => this.SetValue(PropertyDefinition.SingleWriterConnectionString.Name, value?.ToString());
    }

    public string? IamHost
    {
        get => this.GetValue(PropertyDefinition.IamHost.Name);
        set => this.SetValue(PropertyDefinition.IamHost.Name, value);
    }

    public int? IamDefaultPort
    {
        get => this.GetIntValue(PropertyDefinition.IamDefaultPort.Name);
        set => this.SetValue(PropertyDefinition.IamDefaultPort.Name, value?.ToString());
    }

    public string? IamRegion
    {
        get => this.GetValue(PropertyDefinition.IamRegion.Name);
        set => this.SetValue(PropertyDefinition.IamRegion.Name, value);
    }

    public int? IamExpiration
    {
        get => this.GetIntValue(PropertyDefinition.IamExpiration.Name);
        set => this.SetValue(PropertyDefinition.IamExpiration.Name, value?.ToString());
    }

    public string? IamRoleArn
    {
        get => this.GetValue(PropertyDefinition.IamRoleArn.Name);
        set => this.SetValue(PropertyDefinition.IamRoleArn.Name, value);
    }

    public string? IamIdpArn
    {
        get => this.GetValue(PropertyDefinition.IamIdpArn.Name);
        set => this.SetValue(PropertyDefinition.IamIdpArn.Name, value);
    }

    public int? ClusterTopologyRefreshRateMs
    {
        get => this.GetIntValue(PropertyDefinition.ClusterTopologyRefreshRateMs.Name);
        set => this.SetValue(PropertyDefinition.ClusterTopologyRefreshRateMs.Name, value?.ToString());
    }

    public string? ClusterInstanceHostPattern
    {
        get => this.GetValue(PropertyDefinition.ClusterInstanceHostPattern.Name);
        set => this.SetValue(PropertyDefinition.ClusterInstanceHostPattern.Name, value);
    }

    public string? ClusterId
    {
        get => this.GetValue(PropertyDefinition.ClusterId.Name);
        set => this.SetValue(PropertyDefinition.ClusterId.Name, value);
    }

    public string? SecretsManagerSecretId
    {
        get => this.GetValue(PropertyDefinition.SecretsManagerSecretId.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerSecretId.Name, value);
    }

    public string? SecretsManagerRegion
    {
        get => this.GetValue(PropertyDefinition.SecretsManagerRegion.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerRegion.Name, value);
    }

    public int? SecretsManagerExpirationSecs
    {
        get => this.GetIntValue(PropertyDefinition.SecretsManagerExpirationSecs.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerExpirationSecs.Name, value?.ToString());
    }

    public string? SecretsManagerEndpoint
    {
        get => this.GetValue(PropertyDefinition.SecretsManagerEndpoint.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerEndpoint.Name, value);
    }

    public string? SecretsManagerSecretUsernameProperty
    {
        get => this.GetValue(PropertyDefinition.SecretsManagerSecretUsernameProperty.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerSecretUsernameProperty.Name, value);
    }

    public string? SecretsManagerSecretPasswordProperty
    {
        get => this.GetValue(PropertyDefinition.SecretsManagerSecretPasswordProperty.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerSecretPasswordProperty.Name, value);
    }

    public int? OpenConnectionRetryTimeoutMs
    {
        get => this.GetIntValue(PropertyDefinition.OpenConnectionRetryTimeoutMs.Name);
        set => this.SetValue(PropertyDefinition.OpenConnectionRetryTimeoutMs.Name, value?.ToString());
    }

    public int? OpenConnectionRetryIntervalMs
    {
        get => this.GetIntValue(PropertyDefinition.OpenConnectionRetryIntervalMs.Name);
        set => this.SetValue(PropertyDefinition.OpenConnectionRetryIntervalMs.Name, value?.ToString());
    }

    public string? VerifyOpenedConnectionType
    {
        get => this.GetValue(PropertyDefinition.VerifyOpenedConnectionType.Name);
        set => this.SetValue(PropertyDefinition.VerifyOpenedConnectionType.Name, value);
    }

    public string? IdpEndpoint
    {
        get => this.GetValue(PropertyDefinition.IdpEndpoint.Name);
        set => this.SetValue(PropertyDefinition.IdpEndpoint.Name, value);
    }

    public int? IdpPort
    {
        get => this.GetIntValue(PropertyDefinition.IdpPort.Name);
        set => this.SetValue(PropertyDefinition.IdpPort.Name, value?.ToString());
    }

    public string? IdpUsername
    {
        get => this.GetValue(PropertyDefinition.IdpUsername.Name);
        set => this.SetValue(PropertyDefinition.IdpUsername.Name, value);
    }

    public string? IdpPassword
    {
        get => this.GetValue(PropertyDefinition.IdpPassword.Name);
        set => this.SetValue(PropertyDefinition.IdpPassword.Name, value);
    }

    public string? RelayingPartyId
    {
        get => this.GetValue(PropertyDefinition.RelayingPartyId.Name);
        set => this.SetValue(PropertyDefinition.RelayingPartyId.Name, value);
    }

    public string? DbUser
    {
        get => this.GetValue(PropertyDefinition.DbUser.Name);
        set => this.SetValue(PropertyDefinition.DbUser.Name, value);
    }

    public int? HttpClientConnectTimeout
    {
        get => this.GetIntValue(PropertyDefinition.HttpClientConnectTimeout.Name);
        set => this.SetValue(PropertyDefinition.HttpClientConnectTimeout.Name, value?.ToString());
    }

    public int? FailoverTimeoutMs
    {
        get => this.GetIntValue(PropertyDefinition.FailoverTimeoutMs.Name);
        set => this.SetValue(PropertyDefinition.FailoverTimeoutMs.Name, value?.ToString());
    }

    public string? FailoverMode
    {
        get => this.GetValue(PropertyDefinition.FailoverMode.Name);
        set => this.SetValue(PropertyDefinition.FailoverMode.Name, value);
    }

    public string? FailoverReaderHostSelectorStrategy
    {
        get => this.GetValue(PropertyDefinition.FailoverReaderHostSelectorStrategy.Name);
        set => this.SetValue(PropertyDefinition.FailoverReaderHostSelectorStrategy.Name, value);
    }

    public string? InitialConnectionReaderHostSelectorStrategy
    {
        get => this.GetValue(PropertyDefinition.InitialConnectionReaderHostSelectorStrategy.Name);
        set => this.SetValue(PropertyDefinition.InitialConnectionReaderHostSelectorStrategy.Name, value);
    }

    public string? RWSplittingReaderHostSelectorStrategy
    {
        get => this.GetValue(PropertyDefinition.RWSplittingReaderHostSelectorStrategy.Name);
        set => this.SetValue(PropertyDefinition.RWSplittingReaderHostSelectorStrategy.Name, value);
    }

    public bool? EnableConnectFailover
    {
        get => this.GetBoolValue(PropertyDefinition.EnableConnectFailover.Name);
        set => this.SetValue(PropertyDefinition.EnableConnectFailover.Name, value?.ToString());
    }

    public bool? SkipFailoverOnInterruptedThread
    {
        get => this.GetBoolValue(PropertyDefinition.SkipFailoverOnInterruptedThread.Name);
        set => this.SetValue(PropertyDefinition.SkipFailoverOnInterruptedThread.Name, value?.ToString());
    }

    public int? ClusterTopologyHighRefreshRateMs
    {
        get => this.GetIntValue(PropertyDefinition.ClusterTopologyHighRefreshRateMs.Name);
        set => this.SetValue(PropertyDefinition.ClusterTopologyHighRefreshRateMs.Name, value?.ToString());
    }

    public string? RoundRobinHostWeightPairs
    {
        get => this.GetValue(PropertyDefinition.RoundRobinHostWeightPairs.Name);
        set => this.SetValue(PropertyDefinition.RoundRobinHostWeightPairs.Name, value);
    }

    public string? WeightedRandomHostWeightPairs
    {
        get => this.GetValue(PropertyDefinition.WeightedRandomHostWeightPairs.Name);
        set => this.SetValue(PropertyDefinition.WeightedRandomHostWeightPairs.Name, value);
    }

    public bool? LimitlessWaitForTransactionRouterInfo
    {
        get => this.GetBoolValue(PropertyDefinition.LimitlessWaitForRouterInfo.Name);
        set => this.SetValue(PropertyDefinition.LimitlessWaitForRouterInfo.Name, value?.ToString());
    }

    public int? LimitlessGetTransactionRouterInfoRetryIntervalMs
    {
        get => this.GetIntValue(PropertyDefinition.LimitlessGetRouterRetryIntervalMs.Name);
        set => this.SetValue(PropertyDefinition.LimitlessGetRouterRetryIntervalMs.Name, value?.ToString());
    }

    public int? LimitlessGetTransactionRouterInfoMaxRetries
    {
        get => this.GetIntValue(PropertyDefinition.LimitlessGetRouterMaxRetries.Name);
        set => this.SetValue(PropertyDefinition.LimitlessGetRouterMaxRetries.Name, value?.ToString());
    }

    public int? LimitlessTransactionRouterMonitorIntervalMs
    {
        get => this.GetIntValue(PropertyDefinition.LimitlessIntervalMs.Name);
        set => this.SetValue(PropertyDefinition.LimitlessIntervalMs.Name, value?.ToString());
    }

    public int? LimitlessConnectMaxRetries
    {
        get => this.GetIntValue(PropertyDefinition.LimitlessMaxRetries.Name);
        set => this.SetValue(PropertyDefinition.LimitlessMaxRetries.Name, value?.ToString());
    }

    public int? LimitlessTransactionRouterMonitorDisposalTimeMs
    {
        get => this.GetIntValue(PropertyDefinition.LimitlessMonitorDisposalTimeMs.Name);
        set => this.SetValue(PropertyDefinition.LimitlessMonitorDisposalTimeMs.Name, value?.ToString());
    }

    public int? RoundRobinDefaultWeight
    {
        get => this.GetIntValue(PropertyDefinition.RoundRobinDefaultWeight.Name);
        set => this.SetValue(PropertyDefinition.RoundRobinDefaultWeight.Name, value?.ToString());
    }

    public int? MonitorDisposalTimeMs
    {
        get => this.GetIntValue(PropertyDefinition.MonitorDisposalTimeMs.Name);
        set => this.SetValue(PropertyDefinition.MonitorDisposalTimeMs.Name, value?.ToString());
    }

    public bool? FailureDetectionEnabled
    {
        get => this.GetBoolValue(PropertyDefinition.FailureDetectionEnabled.Name);
        set => this.SetValue(PropertyDefinition.FailureDetectionEnabled.Name, value?.ToString());
    }

    public int? FailureDetectionTime
    {
        get => this.GetIntValue(PropertyDefinition.FailureDetectionTime.Name);
        set => this.SetValue(PropertyDefinition.FailureDetectionTime.Name, value?.ToString());
    }

    public int? FailureDetectionInterval
    {
        get => this.GetIntValue(PropertyDefinition.FailureDetectionInterval.Name);
        set => this.SetValue(PropertyDefinition.FailureDetectionInterval.Name, value?.ToString());
    }

    public int? FailureDetectionCount
    {
        get => this.GetIntValue(PropertyDefinition.FailureDetectionCount.Name);
        set => this.SetValue(PropertyDefinition.FailureDetectionCount.Name, value?.ToString());
    }

    public string? AppId
    {
        get => this.GetValue(PropertyDefinition.AppId.Name);
        set => this.SetValue(PropertyDefinition.AppId.Name, value);
    }

    public string? CustomDialect
    {
        get => this.GetValue(PropertyDefinition.TargetDialect.Name);
        set => this.SetValue(PropertyDefinition.TargetDialect.Name, value);
    }

    public int? SecretsManagerExpirationSec
    {
        get => this.GetIntValue(PropertyDefinition.SecretsManagerExpirationSecs.Name);
        set => this.SetValue(PropertyDefinition.SecretsManagerExpirationSecs.Name, value?.ToString());
    }

    public string? RpIdentifier
    {
        get => this.GetValue(PropertyDefinition.RelayingPartyId.Name);
        set => this.SetValue(PropertyDefinition.RelayingPartyId.Name, value);
    }

    public int? MonitorDisposalTime
    {
        get => this.GetIntValue(PropertyDefinition.MonitorDisposalTimeMs.Name);
        set => this.SetValue(PropertyDefinition.MonitorDisposalTimeMs.Name, value?.ToString());
    }

    private string? GetValue(string key)
    {
        return this.TryGetValue(key, out object? value) ? value?.ToString() : null;
    }

    private int? GetIntValue(string key)
    {
        var stringValue = this.GetValue(key);
        return int.TryParse(stringValue, out int result) ? result : null;
    }

    private bool? GetBoolValue(string key)
    {
        var stringValue = this.GetValue(key);
        return bool.TryParse(stringValue, out bool result) ? result : null;
    }

    private void SetValue(string key, string? value)
    {
        if (value == null)
        {
            this.Remove(key);
        }
        else
        {
            this[key] = value;
        }
    }
}
