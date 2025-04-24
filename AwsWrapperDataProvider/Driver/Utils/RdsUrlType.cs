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

using System.ComponentModel;

namespace AwsWrapperDataProvider.Driver.Utils;

/// <summary>
/// Represents the different types of RDS endpoints that can be identified from a hostname.
/// </summary>
public enum RdsUrlType
{
    /// <summary>
    /// Represents an IP address (IPv4 or IPv6).
    /// </summary>
    [Description("IP Address")]
    IpAddress,

    /// <summary>
    /// Represents an RDS cluster writer endpoint (e.g., mydb.cluster-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    [Description("RDS Writer Cluster Endpoint")]
    RdsWriterCluster,

    /// <summary>
    /// Represents an RDS cluster reader endpoint (e.g., mydb.cluster-ro-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    [Description("RDS Reader Cluster Endpoint")]
    RdsReaderCluster,

    /// <summary>
    /// Represents an RDS custom cluster endpoint (e.g., mydb.cluster-custom-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    [Description("RDS Custom Cluster Endpoint")]
    RdsCustomCluster,

    /// <summary>
    /// Represents an RDS proxy endpoint (e.g., mydb.proxy-123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    [Description("RDS Proxy Endpoint")]
    RdsProxy,

    /// <summary>
    /// Represents an RDS instance endpoint (e.g., mydb.123456789012.us-east-1.rds.amazonaws.com).
    /// </summary>
    [Description("RDS Instance Endpoint")]
    RdsInstance,

    /// <summary>
    /// Represents any other type of hostname that doesn't match the RDS endpoint patterns.
    /// </summary>
    [Description("Other Hostname")]
    Other,
}
