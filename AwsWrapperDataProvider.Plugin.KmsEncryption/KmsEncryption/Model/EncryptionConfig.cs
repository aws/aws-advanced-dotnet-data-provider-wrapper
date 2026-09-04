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

using System.Globalization;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

/// <summary>
/// Resolved configuration for the KMS Encryption Plugin, read once from the connection properties.
/// </summary>
internal class EncryptionConfig
{
    private EncryptionConfig(
        string region,
        string metadataSchema,
        bool metadataCacheEnabled,
        TimeSpan metadataCacheExpiration,
        bool dataKeyCacheEnabled,
        int dataKeyCacheMaxSize,
        TimeSpan dataKeyCacheExpiration)
    {
        this.Region = region;
        this.MetadataSchema = metadataSchema;
        this.MetadataCacheEnabled = metadataCacheEnabled;
        this.MetadataCacheExpiration = metadataCacheExpiration;
        this.DataKeyCacheEnabled = dataKeyCacheEnabled;
        this.DataKeyCacheMaxSize = dataKeyCacheMaxSize;
        this.DataKeyCacheExpiration = dataKeyCacheExpiration;
    }

    /// <summary>Gets the AWS region used for KMS operations.</summary>
    internal string Region { get; }

    /// <summary>Gets the schema containing the encryption metadata tables.</summary>
    internal string MetadataSchema { get; }

    /// <summary>
    /// Gets a value indicating whether encryption metadata is cached. When <see langword="false"/>,
    /// metadata is read from the database on every lookup, so changes to the metadata tables (for
    /// example, encrypting an additional column) take effect immediately. Useful during setup and
    /// migration at the cost of one extra query per lookup.
    /// </summary>
    internal bool MetadataCacheEnabled { get; }

    /// <summary>Gets how long encryption metadata is cached.</summary>
    internal TimeSpan MetadataCacheExpiration { get; }

    /// <summary>
    /// Gets a value indicating whether data keys are cached in memory. When <see langword="false"/>,
    /// every encrypt or decrypt operation calls AWS KMS.
    /// </summary>
    internal bool DataKeyCacheEnabled { get; }

    /// <summary>Gets the maximum number of entries held in the data key cache.</summary>
    internal int DataKeyCacheMaxSize { get; }

    /// <summary>Gets how long a data key is cached.</summary>
    internal TimeSpan DataKeyCacheExpiration { get; }

    /// <summary>
    /// Builds the configuration from the given connection properties, applying the documented
    /// defaults for any property that was not supplied.
    /// </summary>
    /// <param name="props">The connection properties.</param>
    /// <returns>The resolved configuration.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when a required property is missing, or a supplied value is not usable.
    /// </exception>
    internal static EncryptionConfig FromProperties(Dictionary<string, string> props)
    {
        string? region = PropertyDefinition.KmsRegion.GetString(props);
        if (string.IsNullOrWhiteSpace(region))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.EncryptionConfig_FromProperties_RegionRequired,
                    PropertyDefinition.KmsRegion.Name,
                    PluginCodesText),
                nameof(props));
        }

        string metadataSchema = PropertyDefinition.KmsEncryptionMetadataSchema.GetString(props)
            ?? throw new ArgumentException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.EncryptionConfig_FromProperties_SchemaEmpty,
                    PropertyDefinition.KmsEncryptionMetadataSchema.Name),
                nameof(props));

        return new EncryptionConfig(
            region,
            metadataSchema,
            PropertyDefinition.KmsMetadataCacheEnabled.GetBoolean(props),
            TimeSpan.FromMinutes(
                RequirePositive(
                    PropertyDefinition.KmsMetadataCacheExpirationMinutes.GetInt(props),
                    PropertyDefinition.KmsMetadataCacheExpirationMinutes.Name)),
            PropertyDefinition.KmsDataKeyCacheEnabled.GetBoolean(props),
            RequirePositive(
                PropertyDefinition.KmsDataKeyCacheMaxSize.GetInt(props),
                PropertyDefinition.KmsDataKeyCacheMaxSize.Name),
            TimeSpan.FromMilliseconds(
                RequirePositive(
                    PropertyDefinition.KmsDataKeyCacheExpirationMs.GetLong(props),
                    PropertyDefinition.KmsDataKeyCacheExpirationMs.Name)));
    }

    private const string PluginCodesText = "kmsEncryption";

    /// <summary>
    /// Validates that a numeric property parsed successfully and is positive. Expiration values are
    /// rejected rather than clamped: a non-positive cache lifetime cannot be expressed to the
    /// underlying cache, and silently substituting a default would hide a misconfiguration.
    /// </summary>
    private static T RequirePositive<T>(T? value, string propertyName)
        where T : struct, IComparable<T>
    {
        if (value is null || value.Value.CompareTo(default) <= 0)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.EncryptionConfig_PositiveNumber_Required,
                    propertyName),
                nameof(propertyName));
        }

        return value.Value;
    }
}
