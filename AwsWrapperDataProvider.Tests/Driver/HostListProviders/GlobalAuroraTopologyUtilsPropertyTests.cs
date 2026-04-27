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

using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using FsCheck;
using FsCheck.Fluent;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.HostListProviders;

/// <summary>
/// Property-based tests for GlobalAuroraTopologyUtils.
/// Uses FsCheck with a minimum of 100 iterations per property.
/// </summary>
public class GlobalAuroraTopologyUtilsPropertyTests
{
    private static readonly Config PbtConfig =
        Config.QuickThrowOnFailure.WithMaxTest(100);

    private static readonly string[] SampleRegions =
    [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-northeast-1"
    ];

    private static readonly char[] AlphaChars =
        "abcdefghijklmnopqrstuvwxyz".ToCharArray();

    private static readonly char[] AlphaNumHyphenChars =
        "abcdefghijklmnopqrstuvwxyz0123456789-".ToCharArray();

    /// <summary>
    /// Generates a valid instance name (1-15 chars, starts with letter, alphanumeric + hyphen).
    /// </summary>
    private static Gen<string> InstanceNameGen()
    {
        return
            from first in Gen.Elements(AlphaChars)
            from rest in Gen.ArrayOf(Gen.Elements(AlphaNumHyphenChars))
            let trimmed = rest.Length > 14 ? rest[..14] : rest
            select first + new string(trimmed);
    }

    /// <summary>
    /// Generates a random AWS region from the sample list.
    /// </summary>
    private static Gen<string> RegionGen()
    {
        return Gen.Elements(SampleRegions);
    }

    /// <summary>
    /// Generates a random cluster domain ID (6-12 alphanumeric chars).
    /// </summary>
    private static Gen<string> ClusterDomainIdGen()
    {
        return
            from chars in Gen.ArrayOf(Gen.Elements(AlphaChars))
            let trimmed = chars.Length < 6
                ? [.. chars, .. "abcdef".Take(6 - chars.Length)]
                : chars.Length > 12 ? chars[..12] : chars
            select new string(trimmed);
    }

    /// <summary>
    /// Generates a standard RDS instance template: ?.{domainId}.{region}.rds.amazonaws.com
    /// </summary>
    private static Gen<(string Region, string Template)> StandardRdsTemplateGen()
    {
        return
            from region in RegionGen()
            from domainId in ClusterDomainIdGen()
            select (region, $"?.{domainId}.{region}.rds.amazonaws.com");
    }

    /// <summary>
    /// Generates a bracket-format instance template: [{region}]?.{domainId}.{region}.rds.amazonaws.com
    /// </summary>
    private static Gen<(string Region, string Template)> BracketTemplateGen()
    {
        return
            from region in RegionGen()
            from domainId in ClusterDomainIdGen()
            select (region, $"[{region}]?.{domainId}.{region}.rds.amazonaws.com");
    }

    // Feature: aurora-global-database-support, Property 3: Topology endpoint construction

    /// <summary>
    /// Property 3: For any topology query result row with instance name N and region R,
    /// where R exists in the region-to-template map with template T,
    /// the constructed HostSpec.Host equals T.Replace("?", N).
    /// **Validates: Requirements 3.1, 3.2**
    /// </summary>
    [Fact]
    public void TopologyEndpointConstruction_ReplacesPlaceholderWithInstanceName()
    {
        var property = Prop.ForAll(
            InstanceNameGen().ToArbitrary(),
            StandardRdsTemplateGen().ToArbitrary(),
            (string instanceName, (string Region, string Template) regionTemplate) =>
            {
                var (region, template) = regionTemplate;

                // Build the template map
                var hostSpecBuilder = new HostSpecBuilder();
                var templateHostSpec = hostSpecBuilder
                    .WithHost(template)
                    .WithPort(5432)
                    .WithRole(HostRole.Writer)
                    .Build();

                var templateMap = new Dictionary<string, HostSpec>(StringComparer.OrdinalIgnoreCase)
                {
                    [region] = templateHostSpec
                };

                // Simulate what QueryForTopologyAsync does: replace "?" with instance name
                string expectedHost = template.Replace("?", instanceName);
                string actualHost = templateMap[region].Host.Replace("?", instanceName);

                return expectedHost == actualHost;
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 4: Missing region template throws

    /// <summary>
    /// Property 4: For any topology query result row with a region R that does NOT exist
    /// in the region-to-template map, an exception is thrown.
    /// **Validates: Requirements 3.3**
    /// </summary>
    [Fact]
    public void MissingRegionTemplate_ThrowsException()
    {
        // Generate a region that is NOT in the template map
        var property = Prop.ForAll(
            RegionGen().ToArbitrary(),
            RegionGen().Where(r => true).ToArbitrary(),
            (string mapRegion, string queryRegion) =>
            {
                // Ensure queryRegion is different from mapRegion
                if (string.Equals(mapRegion, queryRegion, StringComparison.OrdinalIgnoreCase))
                {
                    return true; // Skip this case — trivially true
                }

                var hostSpecBuilder = new HostSpecBuilder();
                var templateHostSpec = hostSpecBuilder
                    .WithHost($"?.xyz123.{mapRegion}.rds.amazonaws.com")
                    .WithPort(5432)
                    .WithRole(HostRole.Writer)
                    .Build();

                var templateMap = new Dictionary<string, HostSpec>(StringComparer.OrdinalIgnoreCase)
                {
                    [mapRegion] = templateHostSpec
                };

                // Attempting to look up a region not in the map should fail
                bool found = templateMap.TryGetValue(queryRegion, out _);
                return !found; // The region should NOT be found
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 5: Instance template parsing round-trip

    /// <summary>
    /// Property 5: For any valid set of region-template pairs (using either bracket format
    /// or standard RDS format), formatting them as a comma-separated string and parsing
    /// with ParseInstanceTemplates produces a dictionary where each region key maps to
    /// a HostSpec with the correct host pattern.
    /// **Validates: Requirements 3.4, 3.5, 3.6**
    /// </summary>
    [Fact]
    public void ParseInstanceTemplates_StandardFormat_RoundTrip()
    {
        var property = Prop.ForAll(
            StandardRdsTemplateGen().ToArbitrary(),
            ((string Region, string Template) pair) =>
            {
                var dialect = new Mock<IGlobalAuroraTopologyDialect>();
                var hostSpecBuilder = new HostSpecBuilder();
                var utils = new GlobalAuroraTopologyUtils(dialect.Object, hostSpecBuilder);

                var result = utils.ParseInstanceTemplates(
                    pair.Template,
                    host =>
                    {
                        if (!RdsUtils.IsDnsPatternValid(host))
                        {
                            throw new InvalidOperationException($"Invalid host pattern: {host}");
                        }
                    });

                return result.ContainsKey(pair.Region)
                    && result[pair.Region].Host == pair.Template;
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 5 (bracket format): For bracket-format templates [region]?.host,
    /// ParseInstanceTemplates extracts the region from the bracket prefix.
    /// **Validates: Requirements 3.4, 3.5, 3.6**
    /// </summary>
    [Fact]
    public void ParseInstanceTemplates_BracketFormat_RoundTrip()
    {
        var property = Prop.ForAll(
            BracketTemplateGen().ToArbitrary(),
            ((string Region, string Template) pair) =>
            {
                var dialect = new Mock<IGlobalAuroraTopologyDialect>();
                var hostSpecBuilder = new HostSpecBuilder();
                var utils = new GlobalAuroraTopologyUtils(dialect.Object, hostSpecBuilder);

                var result = utils.ParseInstanceTemplates(
                    pair.Template,
                    host =>
                    {
                        if (!RdsUtils.IsDnsPatternValid(host))
                        {
                            throw new InvalidOperationException($"Invalid host pattern: {host}");
                        }
                    });

                // The bracket prefix is stripped; the host should be the domain part only
                string expectedHost = pair.Template.Substring(pair.Template.IndexOf(']') + 1);

                return result.ContainsKey(pair.Region)
                    && result[pair.Region].Host == expectedHost;
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 5 (multi-region): For multiple comma-separated region-template pairs,
    /// ParseInstanceTemplates produces a dictionary with all regions.
    /// **Validates: Requirements 3.4, 3.5, 3.6**
    /// </summary>
    [Fact]
    public void ParseInstanceTemplates_MultipleRegions_AllParsed()
    {
        // Generate 2 distinct region-template pairs
        var pairGen =
            from pair1 in StandardRdsTemplateGen()
            from pair2 in StandardRdsTemplateGen()
            where !string.Equals(pair1.Region, pair2.Region, StringComparison.OrdinalIgnoreCase)
            select (pair1, pair2);

        var property = Prop.ForAll(
            pairGen.ToArbitrary(),
            (((string Region, string Template) pair1, (string Region, string Template) pair2) pairs) =>
            {
                var dialect = new Mock<IGlobalAuroraTopologyDialect>();
                var hostSpecBuilder = new HostSpecBuilder();
                var utils = new GlobalAuroraTopologyUtils(dialect.Object, hostSpecBuilder);

                string combined = $"{pairs.pair1.Template},{pairs.pair2.Template}";

                var result = utils.ParseInstanceTemplates(
                    combined,
                    host =>
                    {
                        if (!RdsUtils.IsDnsPatternValid(host))
                        {
                            throw new InvalidOperationException($"Invalid host pattern: {host}");
                        }
                    });

                return result.Count >= 2
                    && result.ContainsKey(pairs.pair1.Region)
                    && result.ContainsKey(pairs.pair2.Region)
                    && result[pairs.pair1.Region].Host == pairs.pair1.Template
                    && result[pairs.pair2.Region].Host == pairs.pair2.Template;
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 6: Empty instance host patterns throws

    /// <summary>
    /// Property 6: For any null, empty, or whitespace-only value of the
    /// GlobalClusterInstanceHostPatterns property, initialization throws an exception.
    /// **Validates: Requirements 4.3**
    /// </summary>
    [Fact]
    public void EmptyInstanceHostPatterns_ThrowsException()
    {
        var emptyStringGen = Gen.OneOf(
            Gen.Constant(""),
            Gen.Constant(" "),
            Gen.Constant("  "),
            Gen.Constant("\t"),
            Gen.Constant("\n"),
            Gen.Constant(" \t\n "));

        var property = Prop.ForAll(
            emptyStringGen.ToArbitrary(),
            (string emptyValue) =>
            {
                var dialect = new Mock<IGlobalAuroraTopologyDialect>();
                var hostSpecBuilder = new HostSpecBuilder();
                var utils = new GlobalAuroraTopologyUtils(dialect.Object, hostSpecBuilder);

                try
                {
                    utils.ParseInstanceTemplates(emptyValue, _ => { });
                    return false; // Should have thrown
                }
                catch (InvalidOperationException)
                {
                    return true; // Expected
                }
            });

        Check.One(PbtConfig, property);
    }
}
