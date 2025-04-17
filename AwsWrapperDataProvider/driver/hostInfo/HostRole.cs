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

namespace AwsWrapperDataProvider.driver.hostInfo;

/// <summary>
/// Defines the role of a host in a database cluster.
/// </summary>
public enum HostRole
{
    /// <summary>
    /// The role of the host is unknown.
    /// </summary>
    Unknown,
    
    /// <summary>
    /// The host is a writer node that can handle write operations.
    /// </summary>
    Writer,
    
    /// <summary>
    /// The host is a reader node that can handle read operations.
    /// </summary>
    Reader
}
