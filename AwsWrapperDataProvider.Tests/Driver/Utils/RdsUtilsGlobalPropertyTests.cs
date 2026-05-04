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

using AwsWrapperDataProvider.Driver.Utils;
using FsCheck;
using FsCheck.Fluent;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

/// <summary>
/// Property-based tests for global endpoint recognition in RdsUtils.
/// Uses FsCheck with a minimum of 100 iterations per property.
/// </summary>
public class RdsUtilsGlobalPropertyTests
{
    private static readonly Config PbtConfig =
        Config.QuickThrowOnFailure.WithMaxTest(100);

    private static readonly char[] AlphaChars =
        "abcdefghijklmnopqrstuvwxyz".ToCharArray();

    private static readonly char[] AlphaNumChars =
        "abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray();

    private static readonly char[] AlphaNumHyphenChars =
        "abcdefghijklmnopqrstuvwxyz0123456789-".ToCharArray();

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
    /// Generates an alphanumeric global cluster ID (1-12 chars).
    /// </summary>
    private static Gen<string> GlobalIdGen()
    {
        return Gen.ArrayOf(Gen.Elements(AlphaNumChars))
            .Where(arr => arr.Length >= 1 && arr.Length <= 12)
            .Select(arr => new string(arr));
    }

    /// <summary>
    /// Generates a valid global endpoint hostname of the form:
    /// {instance}.global-{id}.global.rds.amazonaws.com.
    /// </summary>
    private static Gen<string> ValidGlobalEndpointGen()
    {
        return
            from instance in InstanceNameGen()
            from globalId in GlobalIdGen()
            select $"{instance}.global-{globalId}.global.rds.amazonaws.com";
    }

    /// <summary>
    /// Generates hostnames that should NOT match the global endpoint pattern.
    /// </summary>
    private static Gen<string> NonGlobalEndpointGen()
    {
        // Non-global RDS endpoints: regional writer/reader/instance/proxy clusters, plain hostnames, and IP addresses.
        return Gen.OneOf(
            InstanceNameGen().Select(
                name => $"{name}.cluster-xyz123.us-east-1.rds.amazonaws.com"),
            InstanceNameGen().Select(
                name => $"{name}.cluster-ro-xyz123.us-east-1.rds.amazonaws.com"),
            InstanceNameGen().Select(
                name => $"{name}.xyz123.us-east-1.rds.amazonaws.com"),
            InstanceNameGen().Select(
                name => $"{name}.proxy-xyz123.us-east-1.rds.amazonaws.com"),
            InstanceNameGen().Select(name => $"{name}.example.com"),
            Gen.Constant("192.168.1.1"));
    }

    // Feature: aurora-global-database-support, Property 1: Global endpoint recognition

    /// <summary>
    /// Property 1: For any hostname of the form {name}.global-{id}.global.rds.amazonaws.com,
    /// IsGlobalDbWriterClusterDns returns true.
    /// Validates: Requirements 1.3, 1.4.
    /// </summary>
    [Fact]
    public void GlobalEndpointRecognition_ValidEndpoints_ReturnTrue()
    {
        var property = Prop.ForAll(
            ValidGlobalEndpointGen().ToArbitrary(),
            (string host) => RdsUtils.IsGlobalDbWriterClusterDns(host));

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 1 (negative): For any hostname NOT matching the global endpoint pattern,
    /// IsGlobalDbWriterClusterDns returns false.
    /// Validates: Requirements 1.3, 1.4.
    /// </summary>
    [Fact]
    public void GlobalEndpointRecognition_NonGlobalEndpoints_ReturnFalse()
    {
        var property = Prop.ForAll(
            NonGlobalEndpointGen().ToArbitrary(),
            (string host) => !RdsUtils.IsGlobalDbWriterClusterDns(host));

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 2: Global endpoint classification

    /// <summary>
    /// Property 2: For any hostname matching the global endpoint pattern,
    /// IdentifyRdsType returns RdsUrlType.RdsGlobalWriterCluster.
    /// Validates: Requirements 1.5, 1.6.
    /// </summary>
    [Fact]
    public void GlobalEndpointClassification_ValidEndpoints_ReturnRdsGlobalWriterCluster()
    {
        var property = Prop.ForAll(
            ValidGlobalEndpointGen().ToArbitrary(),
            (string host) => RdsUtils.IdentifyRdsType(host) == RdsUrlType.RdsGlobalWriterCluster);

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Property 2 (negative): For non-global endpoints, IdentifyRdsType does NOT return
    /// RdsGlobalWriterCluster.
    /// Validates: Requirements 1.5, 1.6.
    /// </summary>
    [Fact]
    public void GlobalEndpointClassification_NonGlobalEndpoints_DoNotReturnRdsGlobalWriterCluster()
    {
        var property = Prop.ForAll(
            NonGlobalEndpointGen().ToArbitrary(),
            (string host) => RdsUtils.IdentifyRdsType(host) != RdsUrlType.RdsGlobalWriterCluster);

        Check.One(PbtConfig, property);
    }
}
