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

using Amazon.Runtime;
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Authentication;

/// <summary>
/// Process-global registry that lets consumers supply and manage their own AWS credentials for the
/// AWS API calls the wrapper makes on their behalf (RDS IAM token generation, Secrets Manager, and
/// RDS control-plane calls).
/// <para>
/// Register a handler once, typically at application startup:
/// <code>
/// AwsCredentialsManager.SetCustomHandler((hostSpec, props) =>
///     new AssumeRoleAWSCredentials(
///         FallbackCredentialsFactory.GetCredentials(),
///         "arn:aws:iam::123456789012:role/MyRole",
///         "my-session"));
/// </code>
/// The registration is process-global, so the most recent handler wins. Because the handler receives
/// the <see cref="HostSpec"/> and connection properties, a single handler can still return different
/// credentials per endpoint.
/// </para>
/// <para>
/// When no handler is registered — or the registered handler returns <c>null</c> —
/// <see cref="GetCredentials"/> returns <c>null</c>, which every caller treats as "use the AWS SDK
/// default credentials chain". This preserves the wrapper's original behavior for consumers that do
/// not register a handler.
/// </para>
/// </summary>
public static class AwsCredentialsManager
{
    private static readonly object SyncRoot = new();

    private static AwsCredentialsProviderHandler? handler;

    /// <summary>
    /// Registers the handler used to select AWS credentials. The registration is process-global; the
    /// most recent call wins.
    /// </summary>
    /// <param name="customHandler">The handler to register.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="customHandler"/> is <c>null</c>.</exception>
    public static void SetCustomHandler(AwsCredentialsProviderHandler customHandler)
    {
        ArgumentNullException.ThrowIfNull(customHandler);
        lock (SyncRoot)
        {
            handler = customHandler;
        }
    }

    /// <summary>
    /// Clears any registered handler, reverting to the AWS SDK default credentials chain.
    /// </summary>
    public static void ResetCustomHandler()
    {
        lock (SyncRoot)
        {
            handler = null;
        }
    }

    /// <summary>
    /// Resolves the credentials for the given endpoint. Returns the value produced by the registered
    /// handler, or <c>null</c> when no handler is registered or the handler returns <c>null</c>.
    /// A <c>null</c> result means "use the AWS SDK default credentials chain"; callers must fall back
    /// to their default-chain code path in that case.
    /// </summary>
    /// <param name="hostSpec">The endpoint the credentials will be used against; may be <c>null</c>.</param>
    /// <param name="props">A read-only view of the connection properties.</param>
    /// <returns>The credentials to use, or <c>null</c> to use the AWS SDK default credentials chain.</returns>
    public static AWSCredentials? GetCredentials(HostSpec? hostSpec, IReadOnlyDictionary<string, string> props)
    {
        AwsCredentialsProviderHandler? current;
        lock (SyncRoot)
        {
            current = handler;
        }

        // Invoke the handler outside the lock: handlers are expected to be cheap, but releasing the
        // lock first ensures a consumer-supplied handler can never serialize concurrent connects.
        return current?.Invoke(hostSpec, props);
    }
}
