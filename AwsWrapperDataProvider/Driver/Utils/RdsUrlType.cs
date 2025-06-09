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

namespace AwsWrapperDataProvider.Driver.Utils;

/// <summary>
/// Represents the different types of RDS endpoints that can be identified from a hostname.
/// </summary>
public sealed class RdsUrlType
{
    /// <summary>
    /// Represents an IP address (IPv4 or IPv6).
    /// </summary>
    public static readonly RdsUrlType IpAddress = new(false, false);

    /// <summary>
    /// Represents an RDS cluster writer endpoint (e.g., mydb.cluster-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    public static readonly RdsUrlType RdsWriterCluster = new(true, true);

    /// <summary>
    /// Represents an RDS cluster reader endpoint (e.g., mydb.cluster-ro-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    public static readonly RdsUrlType RdsReaderCluster = new(true, true);

    /// <summary>
    /// Represents an RDS custom cluster endpoint (e.g., mydb.cluster-custom-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    public static readonly RdsUrlType RdsCustomCluster = new(true, true);

    /// <summary>
    /// Represents an RDS proxy endpoint (e.g., mydb.proxy-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    public static readonly RdsUrlType RdsProxy = new(true, false);

    /// <summary>
    /// Represents an RDS instance endpoint (e.g., mydb.123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    public static readonly RdsUrlType RdsInstance = new(true, false);

    /// <summary>
    /// Represents an RDS Aurora limitless shard group (e.g., mydb.shardgrp-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    public static readonly RdsUrlType RdsAuroraLimitlessDbShardGroup = new(true, false);

    /// <summary>
    /// Represents any other type of hostname that doesn't match the RDS endpoint patterns.
    /// </summary>
    public static readonly RdsUrlType Other = new(false, false);

    public bool IsRds { get; }

    public bool IsRdsCluster { get; }

    private RdsUrlType(bool isRds, bool isRdsCluster)
    {
        this.IsRds = isRds;
        this.IsRdsCluster = isRdsCluster;
    }
}
