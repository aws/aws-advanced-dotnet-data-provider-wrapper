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

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

/// <summary>
/// Something about a statement that this plugin cannot handle correctly, together with the reason.
/// <para>
/// The subject is a parameter name, a column identifier, or the word "statement", depending on what the
/// limitation is about. Each of these is logged as a warning and the statement then runs unchanged, so
/// every one of them is a case where the driver alone does not guarantee the column holds ciphertext.
/// </para>
/// </summary>
internal class EncryptionLimitation
{
    internal EncryptionLimitation(string subject, string reason)
    {
        this.Subject = subject;
        this.Reason = reason;
    }

    /// <summary>Gets what the limitation is about: a parameter, a column, or the statement itself.</summary>
    internal string Subject { get; }

    /// <summary>Gets the reason it cannot be handled, for the message shown to the caller.</summary>
    internal string Reason { get; }
}
