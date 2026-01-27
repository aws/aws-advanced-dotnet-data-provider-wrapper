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

using Amazon.RDS.Model;

namespace AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;

/// <summary>
/// Represents custom endpoint information for a given custom endpoint.
/// </summary>
public class CustomEndpointInfo
{
    /// <summary>
    /// Gets the endpoint identifier for the custom endpoint. For example, if the custom endpoint URL
    /// is "my-custom-endpoint.cluster-custom-XYZ.us-east-1.rds.amazonaws.com", the endpoint identifier is
    /// "my-custom-endpoint".
    /// </summary>
    public string EndpointIdentifier { get; }

    /// <summary>
    /// Gets the cluster identifier for the cluster that the custom endpoint belongs to.
    /// </summary>
    public string ClusterIdentifier { get; }

    /// <summary>
    /// Gets the URL for the custom endpoint.
    /// </summary>
    public string Url { get; }

    /// <summary>
    /// Gets the role type of the custom endpoint.
    /// </summary>
    public CustomEndpointRoleType RoleType { get; }

    /// <summary>
    /// Gets the member list type of the custom endpoint.
    /// </summary>
    public MemberTypeList MemberListType { get; }

    /// <summary>
    /// Gets the members (instance IDs) for the hosts in the custom endpoint.
    /// </summary>
    private HashSet<string> Members { get; }

    /// <summary>
    /// Constructs a new CustomEndpointInfo instance with the specified details.
    /// </summary>
    /// <param name="endpointIdentifier">The endpoint identifier for the custom endpoint.</param>
    /// <param name="clusterIdentifier">The cluster identifier for the cluster that the custom endpoint belongs to.</param>
    /// <param name="url">The URL for the custom endpoint.</param>
    /// <param name="roleType">The role type of the custom endpoint.</param>
    /// <param name="members">The instance IDs for the hosts in the custom endpoint.</param>
    /// <param name="memberListType">The list type for members.</param>
    public CustomEndpointInfo(
        string endpointIdentifier,
        string clusterIdentifier,
        string url,
        CustomEndpointRoleType roleType,
        HashSet<string> members,
        MemberTypeList memberListType)
    {
        this.EndpointIdentifier = endpointIdentifier;
        this.ClusterIdentifier = clusterIdentifier;
        this.Url = url;
        this.RoleType = roleType;
        this.Members = members;
        this.MemberListType = memberListType;
    }

    /// <summary>
    /// Constructs a CustomEndpointInfo object from a DBClusterEndpoint instance as returned by the RDS API.
    /// </summary>
    /// <param name="responseEndpointInfo">The endpoint info returned by the RDS API.</param>
    /// <returns>A CustomEndpointInfo object representing the information in the given DBClusterEndpoint.</returns>
    public static CustomEndpointInfo FromDBClusterEndpoint(DBClusterEndpoint responseEndpointInfo)
    {
        List<string> members;
        MemberTypeList memberListType;

        if (responseEndpointInfo.StaticMembers != null && responseEndpointInfo.StaticMembers.Count > 0)
        {
            members = responseEndpointInfo.StaticMembers;
            memberListType = MemberTypeList.StaticList;
        }
        else
        {
            members = responseEndpointInfo.ExcludedMembers ?? new List<string>();
            memberListType = MemberTypeList.ExclusionList;
        }

        CustomEndpointRoleType roleType = Enum.TryParse<CustomEndpointRoleType>(
            responseEndpointInfo.CustomEndpointType,
            true,
            out var parsedRoleType) ? parsedRoleType : CustomEndpointRoleType.Any;

        return new CustomEndpointInfo(
            responseEndpointInfo.DBClusterEndpointIdentifier,
            responseEndpointInfo.DBClusterIdentifier,
            responseEndpointInfo.Endpoint,
            roleType,
            new HashSet<string>(members),
            memberListType
        );
    }

    /// <summary>
    /// Gets the static members of the custom endpoint. If the custom endpoint member list type is an exclusion list,
    /// returns null.
    /// </summary>
    /// <returns>The static members of the custom endpoint, or null if the custom endpoint member list type is an exclusion list.</returns>
    public HashSet<string>? GetStaticMembers()
    {
        return this.MemberListType == MemberTypeList.StaticList ? this.Members : null;
    }

    /// <summary>
    /// Gets the excluded members of the custom endpoint. If the custom endpoint member list type is a static list,
    /// returns null.
    /// </summary>
    /// <returns>The excluded members of the custom endpoint, or null if the custom endpoint member list type is a static list.</returns>
    public HashSet<string>? GetExcludedMembers()
    {
        return this.MemberListType == MemberTypeList.ExclusionList ? this.Members : null;
    }

    public override bool Equals(object? obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (obj == null || this.GetType() != obj.GetType())
        {
            return false;
        }

        CustomEndpointInfo info = (CustomEndpointInfo)obj;
        return this.EndpointIdentifier == info.EndpointIdentifier
               && this.ClusterIdentifier == info.ClusterIdentifier
               && this.Url == info.Url
               && this.RoleType == info.RoleType
               && this.Members.SetEquals(info.Members)
               && this.MemberListType == info.MemberListType;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(
            this.EndpointIdentifier,
            this.ClusterIdentifier,
            this.Url,
            this.RoleType,
            this.MemberListType,
            this.Members);
    }

    public override string ToString()
    {
        return $"CustomEndpointInfo[url={this.Url}, clusterIdentifier={this.ClusterIdentifier}, customEndpointType={this.RoleType}, memberListType={this.MemberListType}, members={string.Join(", ", this.Members)}]";
    }
}
