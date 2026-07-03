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
/// Describes a dynamic password provider. Registered by token-based auth plugins (e.g. IAM, SAML,
/// Secrets Manager) and consumed by the target dialects so a rotating token can be supplied to the
/// driver without changing the connection string (and therefore without fragmenting the driver's
/// internal connection pool). The dialect invokes <see cref="Provider"/> when opening a new physical
/// connection, so it is expected to satisfy the common path from an in-memory cache.
/// </summary>
public sealed class PasswordProviderRegistration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PasswordProviderRegistration"/> class.
    /// </summary>
    /// <param name="provider">The delegate that returns the current password/token.</param>
    public PasswordProviderRegistration(WrapperPasswordProvider provider)
    {
        this.Provider = provider;
    }

    /// <summary>
    /// Gets the delegate that returns the current password/token.
    /// </summary>
    public WrapperPasswordProvider Provider { get; }
}
