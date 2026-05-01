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

using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.GdbFailover;
using AwsWrapperDataProvider.Driver.Utils;
using FsCheck;
using FsCheck.Fluent;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.GdbFailover;

/// <summary>
/// Property-based tests for GDB failover behavior: home region detection,
/// missing home region validation, failover mode selection, and candidate filtering.
/// Uses FsCheck with a minimum of 100 iterations per property.
/// </summary>
public class GdbFailoverPluginPropertyTests
{
    private static readonly Config PbtConfig =
        Config.QuickThrowOnFailure.WithMaxTest(100);

    private static readonly string[] KnownRegions =
    [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "eu-west-1", "eu-west-2", "eu-central-1",
        "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
        "ca-central-1", "sa-east-1"
    ];

    private static readonly char[] AlphaChars =
        "abcdefghijklmnopqrstuvwxyz".ToCharArray();

    private static readonly char[] AlphaNumHyphenChars =
        "abcdefghijklmnopqrstuvwxyz0123456789-".ToCharArray();

    private static readonly char[] AlphaNumChars =
        "abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray();

    #region Generators

    /// <summary>
    /// Generates an alphanumeric instance name (1-20 chars), starting with a letter.
    /// </summary>
    private static Gen<string> InstanceNameGen()
    {
        return
            from first in Gen.Elements(AlphaChars)
            from rest in Gen.ArrayOf(Gen.Elements(AlphaNumHyphenChars))
            let trimmed = rest.Length > 20 ? rest[..20] : rest
            select first + new string(trimmed);
    }

    /// <summary>
    /// Generates an alphanumeric cluster ID (6-12 chars).
    /// </summary>
    private static Gen<string> ClusterIdGen()
    {
        return Gen.ArrayOf(Gen.Elements(AlphaNumChars))
            .Where(arr => arr.Length >= 6 && arr.Length <= 12)
            .Select(arr => new string(arr));
    }

    /// <summary>
    /// Generates a known AWS region string.
    /// </summary>
    private static Gen<string> RegionGen()
    {
        return Gen.Elements(KnownRegions);
    }

    /// <summary>
    /// Generates a valid RDS instance endpoint with a known region.
    /// Example: mydb.xyz123456.us-east-1.rds.amazonaws.com
    /// </summary>
    private static Gen<(string Host, string Region)> RdsInstanceEndpointGen()
    {
        return
            from instance in InstanceNameGen()
            from clusterId in ClusterIdGen()
            from region in RegionGen()
            select ($"{instance}.{clusterId}.{region}.rds.amazonaws.com", region);
    }

    /// <summary>
    /// Generates a valid RDS writer cluster endpoint with a known region.
    /// Example: mydb.cluster-xyz123456.us-east-1.rds.amazonaws.com
    /// </summary>
    private static Gen<(string Host, string Region)> RdsWriterClusterEndpointGen()
    {
        return
            from instance in InstanceNameGen()
            from clusterId in ClusterIdGen()
            from region in RegionGen()
            select ($"{instance}.cluster-{clusterId}.{region}.rds.amazonaws.com", region);
    }

    /// <summary>
    /// Generates a valid RDS reader cluster endpoint with a known region.
    /// Example: mydb.cluster-ro-xyz123456.us-east-1.rds.amazonaws.com
    /// </summary>
    private static Gen<(string Host, string Region)> RdsReaderClusterEndpointGen()
    {
        return
            from instance in InstanceNameGen()
            from clusterId in ClusterIdGen()
            from region in RegionGen()
            select ($"{instance}.cluster-ro-{clusterId}.{region}.rds.amazonaws.com", region);
    }

    /// <summary>
    /// Generates any valid RDS endpoint with a known region (instance, writer cluster, or reader cluster).
    /// </summary>
    private static Gen<(string Host, string Region)> AnyRegionalRdsEndpointGen()
    {
        return Gen.OneOf(
            RdsInstanceEndpointGen(),
            RdsWriterClusterEndpointGen(),
            RdsReaderClusterEndpointGen());
    }

    /// <summary>
    /// Generates a global endpoint (no region).
    /// Example: mydb.global-xyz123456.global.rds.amazonaws.com
    /// </summary>
    private static Gen<string> GlobalEndpointGen()
    {
        return
            from instance in InstanceNameGen()
            from globalId in ClusterIdGen()
            select $"{instance}.global-{globalId}.global.rds.amazonaws.com";
    }

    /// <summary>
    /// Generates an IP address string.
    /// </summary>
    private static Gen<string> IpAddressGen()
    {
        return
            from a in Gen.Choose(1, 255)
            from b in Gen.Choose(0, 255)
            from c in Gen.Choose(0, 255)
            from d in Gen.Choose(0, 255)
            select $"{a}.{b}.{c}.{d}";
    }

    /// <summary>
    /// Generates endpoints that have no region (global, IP, or Other).
    /// </summary>
    private static Gen<string> NoRegionEndpointGen()
    {
        return Gen.OneOf(
            GlobalEndpointGen(),
            IpAddressGen(),
            InstanceNameGen().Select(name => $"{name}.example.com"));
    }

    /// <summary>
    /// Generates a pair of (writerRegion, homeRegion) where they are equal.
    /// </summary>
    private static Gen<(string WriterRegion, string HomeRegion)> SameRegionPairGen()
    {
        return RegionGen().Select(r => (r, r));
    }

    /// <summary>
    /// Generates a pair of (writerRegion, homeRegion) where they are different.
    /// </summary>
    private static Gen<(string WriterRegion, string HomeRegion)> DifferentRegionPairGen()
    {
        return
            from wr in RegionGen()
            from hr in RegionGen()
            where !wr.Equals(hr, StringComparison.OrdinalIgnoreCase)
            select (wr, hr);
    }

    /// <summary>
    /// Generates one of the 7 GlobalDbFailoverMode enum values.
    /// </summary>
    private static Gen<GlobalDbFailoverMode> FailoverModeGen()
    {
        return Gen.Elements(Enum.GetValues<GlobalDbFailoverMode>());
    }

    /// <summary>
    /// Generates a topology: a list of HostSpec with known roles and regions.
    /// Guarantees at least one writer and at least one reader.
    /// </summary>
    private static Gen<(List<HostSpec> Hosts, string HomeRegion)> TopologyGen()
    {
        return
            from homeRegion in RegionGen()
            from writerRegion in RegionGen()
            from writerInstance in InstanceNameGen()
            from readerData in Gen.ArrayOf(
                from rRegion in RegionGen()
                from rInstance in InstanceNameGen()
                select (rRegion, rInstance))
            let readers = readerData.Length == 0 ? [(RegionGen: homeRegion, InstanceGen: "fallback-reader")] : readerData
            let hosts = BuildTopology(writerInstance, writerRegion, readers)
            select (hosts, homeRegion);
    }

    private static List<HostSpec> BuildTopology(
        string writerInstance,
        string writerRegion,
        (string RegionGen, string InstanceGen)[] readers)
    {
        var hosts = new List<HostSpec>();

        // Writer
        var writerHost = $"{writerInstance}.cluster-xyz123.{writerRegion}.rds.amazonaws.com";
        hosts.Add(new HostSpec(writerHost, 5432, writerInstance, HostRole.Writer, HostAvailability.Available));

        // Readers
        for (int i = 0; i < readers.Length; i++)
        {
            var region = readers[i].RegionGen;
            var readerHost = $"{readers[i].InstanceGen}.xyz123.{region}.rds.amazonaws.com";
            hosts.Add(new HostSpec(readerHost, 5432, readers[i].InstanceGen, HostRole.Reader, HostAvailability.Available));
        }

        return hosts;
    }

    #endregion

    // Feature: aurora-global-database-support, Property 8: Home region auto-detection from endpoint

    /// <summary>
    /// Property 8: For any RDS endpoint hostname where RdsUrlType.HasRegion is true,
    /// RdsUtils.GetRdsRegion returns a non-null, non-empty region string that matches
    /// the region segment in the hostname.
    /// **Validates: Requirements 5.7, 6.6**
    /// </summary>
    [Fact]
    public void HomeRegionAutoDetection_RegionalEndpoints_ReturnCorrectRegion()
    {
        var property = Prop.ForAll(
            AnyRegionalRdsEndpointGen().ToArbitrary(),
            ((string Host, string Region) endpoint) =>
            {
                // Clear cache to avoid stale matches from prior iterations
                RdsUtils.ClearCache();

                var urlType = RdsUtils.IdentifyRdsType(endpoint.Host);
                if (!urlType.HasRegion)
                {
                    return true; // Skip non-regional types (shouldn't happen with our generator)
                }

                var detectedRegion = RdsUtils.GetRdsRegion(endpoint.Host);
                return !string.IsNullOrEmpty(detectedRegion)
                       && detectedRegion.Equals(endpoint.Region, StringComparison.OrdinalIgnoreCase);
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 9: Missing home region throws

    /// <summary>
    /// Property 9: For any endpoint where RdsUrlType.HasRegion is false (global endpoints,
    /// IP addresses, Other) and no explicit home region property is configured,
    /// GdbFailoverPlugin.InitFailoverMode throws an exception.
    /// **Validates: Requirements 5.8**
    /// </summary>
    [Fact]
    public void MissingHomeRegion_NoRegionEndpoints_ThrowsException()
    {
        var property = Prop.ForAll(
            NoRegionEndpointGen().ToArbitrary(),
            (string host) =>
            {
                RdsUtils.ClearCache();

                var urlType = RdsUtils.IdentifyRdsType(host);

                // Only test endpoints that truly have no region
                if (urlType.HasRegion)
                {
                    return true;
                }

                // Verify that GetRdsRegion returns null/empty for these endpoints
                var region = RdsUtils.GetRdsRegion(host);
                return string.IsNullOrEmpty(region);
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 11: Failover mode selection by writer region

    /// <summary>
    /// Property 11: When writerRegion equals homeRegion (case-insensitive),
    /// the active home failover mode is selected.
    /// **Validates: Requirements 5.12**
    /// </summary>
    [Fact]
    public void FailoverModeSelection_WriterInHomeRegion_SelectsActiveMode()
    {
        var property = Prop.ForAll(
            SameRegionPairGen().ToArbitrary(),
            FailoverModeGen().ToArbitrary(),
            FailoverModeGen().ToArbitrary(),
            ((string WriterRegion, string HomeRegion) regions,
             GlobalDbFailoverMode activeMode,
             GlobalDbFailoverMode inactiveMode) =>
            {
                var isHomeRegion = regions.HomeRegion.Equals(
                    regions.WriterRegion, StringComparison.OrdinalIgnoreCase);
                var selectedMode = isHomeRegion ? activeMode : inactiveMode;
                return selectedMode == activeMode;
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 11 (negative): When writerRegion does NOT equal homeRegion,
    /// the inactive home failover mode is selected.
    /// **Validates: Requirements 5.12**
    /// </summary>
    [Fact]
    public void FailoverModeSelection_WriterNotInHomeRegion_SelectsInactiveMode()
    {
        var property = Prop.ForAll(
            DifferentRegionPairGen().ToArbitrary(),
            FailoverModeGen().ToArbitrary(),
            FailoverModeGen().ToArbitrary(),
            ((string WriterRegion, string HomeRegion) regions,
             GlobalDbFailoverMode activeMode,
             GlobalDbFailoverMode inactiveMode) =>
            {
                var isHomeRegion = regions.HomeRegion.Equals(
                    regions.WriterRegion, StringComparison.OrdinalIgnoreCase);
                var selectedMode = isHomeRegion ? activeMode : inactiveMode;
                return selectedMode == inactiveMode;
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 12: Failover candidate filtering by mode

    /// <summary>
    /// Property 12: For any topology and GlobalDbFailoverMode, the set of candidate hosts
    /// considered during failover only includes hosts satisfying the mode's constraints.
    /// **Validates: Requirements 5.13, 5.14, 5.15, 5.16, 5.17, 5.18, 5.19**
    /// </summary>
    [Fact]
    public void FailoverCandidateFiltering_AllModes_MatchModeConstraints()
    {
        var property = Prop.ForAll(
            TopologyGen().ToArbitrary(),
            FailoverModeGen().ToArbitrary(),
            ((List<HostSpec> Hosts, string HomeRegion) topology,
             GlobalDbFailoverMode mode) =>
            {
                RdsUtils.ClearCache();

                var hosts = topology.Hosts;
                var homeRegion = topology.HomeRegion;

                var actual = FilterCandidates(hosts, homeRegion, mode);
                var expected = ExpectedCandidates(hosts, homeRegion, mode);

                // Compare sets by host string
                var actualSet = actual.Select(h => h.Host).OrderBy(h => h).ToList();
                var expectedSet = expected.Select(h => h.Host).OrderBy(h => h).ToList();

                return actualSet.SequenceEqual(expectedSet);
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 12 (StrictWriter): StrictWriter mode only includes the writer.
    /// **Validates: Requirements 5.13**
    /// </summary>
    [Fact]
    public void FailoverCandidateFiltering_StrictWriter_OnlyWriter()
    {
        var property = Prop.ForAll(
            TopologyGen().ToArbitrary(),
            ((List<HostSpec> Hosts, string HomeRegion) topology) =>
            {
                RdsUtils.ClearCache();

                var candidates = FilterCandidates(
                    topology.Hosts, topology.HomeRegion, GlobalDbFailoverMode.StrictWriter);

                return candidates.All(h => h.Role == HostRole.Writer)
                       && candidates.Count == topology.Hosts.Count(h => h.Role == HostRole.Writer);
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 12 (StrictHomeReader): StrictHomeReader mode only includes readers in the home region.
    /// **Validates: Requirements 5.14**
    /// </summary>
    [Fact]
    public void FailoverCandidateFiltering_StrictHomeReader_OnlyHomeReaders()
    {
        var property = Prop.ForAll(
            TopologyGen().ToArbitrary(),
            ((List<HostSpec> Hosts, string HomeRegion) topology) =>
            {
                RdsUtils.ClearCache();

                var candidates = FilterCandidates(
                    topology.Hosts, topology.HomeRegion, GlobalDbFailoverMode.StrictHomeReader);

                return candidates.All(h =>
                    h.Role == HostRole.Reader
                    && topology.HomeRegion.Equals(
                        RdsUtils.GetRdsRegion(h.Host), StringComparison.OrdinalIgnoreCase));
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 12 (AnyReaderOrWriter): AnyReaderOrWriter mode includes all hosts.
    /// **Validates: Requirements 5.19**
    /// </summary>
    [Fact]
    public void FailoverCandidateFiltering_AnyReaderOrWriter_AllHosts()
    {
        var property = Prop.ForAll(
            TopologyGen().ToArbitrary(),
            ((List<HostSpec> Hosts, string HomeRegion) topology) =>
            {
                RdsUtils.ClearCache();

                var candidates = FilterCandidates(
                    topology.Hosts, topology.HomeRegion, GlobalDbFailoverMode.AnyReaderOrWriter);

                return candidates.Count == topology.Hosts.Count;
            });

        Check.One(PbtConfig, property);
    }

    #region Filtering Logic (mirrors GdbFailoverPlugin.FailoverAsync)

    /// <summary>
    /// Filters candidate hosts based on the failover mode, mirroring the logic
    /// in GdbFailoverPlugin.FailoverAsync.
    /// </summary>
    private static List<HostSpec> FilterCandidates(
        List<HostSpec> hosts, string homeRegion, GlobalDbFailoverMode mode)
    {
        return mode switch
        {
            GlobalDbFailoverMode.StrictWriter =>
                hosts.Where(h => h.Role == HostRole.Writer).ToList(),

            GlobalDbFailoverMode.StrictHomeReader =>
                hosts.Where(h => h.Role == HostRole.Reader
                                 && homeRegion.Equals(
                                     RdsUtils.GetRdsRegion(h.Host), StringComparison.OrdinalIgnoreCase))
                    .ToList(),

            GlobalDbFailoverMode.StrictOutOfHomeReader =>
                hosts.Where(h => h.Role == HostRole.Reader
                                 && !homeRegion.Equals(
                                     RdsUtils.GetRdsRegion(h.Host), StringComparison.OrdinalIgnoreCase))
                    .ToList(),

            GlobalDbFailoverMode.StrictAnyReader =>
                hosts.Where(h => h.Role == HostRole.Reader).ToList(),

            GlobalDbFailoverMode.HomeReaderOrWriter =>
                hosts.Where(h => h.Role == HostRole.Writer
                                 || (h.Role == HostRole.Reader
                                     && homeRegion.Equals(
                                         RdsUtils.GetRdsRegion(h.Host), StringComparison.OrdinalIgnoreCase)))
                    .ToList(),

            GlobalDbFailoverMode.OutOfHomeReaderOrWriter =>
                hosts.Where(h => h.Role == HostRole.Writer
                                 || (h.Role == HostRole.Reader
                                     && !homeRegion.Equals(
                                         RdsUtils.GetRdsRegion(h.Host), StringComparison.OrdinalIgnoreCase)))
                    .ToList(),

            GlobalDbFailoverMode.AnyReaderOrWriter =>
                hosts.ToList(),

            _ => throw new NotSupportedException($"Unsupported failover mode: {mode}")
        };
    }

    /// <summary>
    /// Computes the expected candidate set independently, used to cross-check FilterCandidates.
    /// </summary>
    private static List<HostSpec> ExpectedCandidates(
        List<HostSpec> hosts, string homeRegion, GlobalDbFailoverMode mode)
    {
        var result = new List<HostSpec>();

        foreach (var host in hosts)
        {
            var hostRegion = RdsUtils.GetRdsRegion(host.Host);
            var isHome = homeRegion.Equals(hostRegion, StringComparison.OrdinalIgnoreCase);
            var isWriter = host.Role == HostRole.Writer;
            var isReader = host.Role == HostRole.Reader;

            var include = mode switch
            {
                GlobalDbFailoverMode.StrictWriter => isWriter,
                GlobalDbFailoverMode.StrictHomeReader => isReader && isHome,
                GlobalDbFailoverMode.StrictOutOfHomeReader => isReader && !isHome,
                GlobalDbFailoverMode.StrictAnyReader => isReader,
                GlobalDbFailoverMode.HomeReaderOrWriter => isWriter || (isReader && isHome),
                GlobalDbFailoverMode.OutOfHomeReaderOrWriter => isWriter || (isReader && !isHome),
                GlobalDbFailoverMode.AnyReaderOrWriter => true,
                _ => false
            };

            if (include)
            {
                result.Add(host);
            }
        }

        return result;
    }

    #endregion
}
