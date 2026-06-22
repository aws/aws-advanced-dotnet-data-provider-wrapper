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

namespace AwsWrapperDataProvider.Driver.Auth;

/// <summary>
/// Describes a dynamic password provider together with the cadence at which a target driver should
/// refresh it. Registered by token-based auth plugins (e.g. IAM, SAML, Secrets Manager) and consumed
/// by the target dialects so a rotating token can be supplied to the driver without changing the
/// connection string (and therefore without fragmenting the driver's internal connection pool).
/// </summary>
public sealed class PasswordProviderRegistration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PasswordProviderRegistration"/> class.
    /// </summary>
    /// <param name="provider">The delegate that returns the current password/token.</param>
    /// <param name="successRefreshInterval">
    /// How long a successfully fetched password may be cached before <paramref name="provider"/> is
    /// invoked again on a background timer.
    /// </param>
    /// <param name="failureRefreshInterval">
    /// How soon <paramref name="provider"/> is retried after a failed fetch. Should typically be much
    /// shorter than <paramref name="successRefreshInterval"/>.
    /// </param>
    public PasswordProviderRegistration(
        WrapperPasswordProvider provider,
        TimeSpan successRefreshInterval,
        TimeSpan failureRefreshInterval)
    {
        this.Provider = provider;
        this.SuccessRefreshInterval = successRefreshInterval;
        this.FailureRefreshInterval = failureRefreshInterval;
    }

    /// <summary>
    /// Gets the delegate that returns the current password/token.
    /// </summary>
    public WrapperPasswordProvider Provider { get; }

    /// <summary>
    /// Gets the interval at which a successfully fetched password is refreshed.
    /// </summary>
    public TimeSpan SuccessRefreshInterval { get; }

    /// <summary>
    /// Gets the interval at which the provider is retried after a failed fetch.
    /// </summary>
    public TimeSpan FailureRefreshInterval { get; }
}
