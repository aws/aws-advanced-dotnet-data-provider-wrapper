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

using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Encrypts a single value for a single column, resolving that column's data key as needed.
/// </summary>
internal interface IColumnEncryptor
{
    /// <summary>Encrypts <paramref name="value"/> for <paramref name="column"/>.</summary>
    Task<byte[]> EncryptAsync(object value, ColumnEncryptionConfig column, CancellationToken cancellationToken);
}
