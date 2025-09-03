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

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class TestDatabaseInfo
{
    public string Username { get; set; } = null!;
    public string Password { get; set; } = null!;
    public string DefaultDbName { get; set; } = null!;
    public string ClusterEndpoint { get; set; } = null!;
    public int ClusterEndpointPort { get; set; }
    public string ClusterReadOnlyEndpoint { get; set; } = null!;
    public int ClusterReadOnlyEndpointPort { get; set; }
    public string InstanceEndpointSuffix { get; set; } = null!;
    public int InstanceEndpointPort { get; set; }

    public List<TestInstanceInfo> Instances { get; set; } = new();

    public void SetClusterEndpoint(string clusterEndpoint, int clusterEndpointPort)
    {
        this.ClusterEndpoint = clusterEndpoint;
        this.ClusterEndpointPort = clusterEndpointPort;
    }

    public void SetClusterReadOnlyEndpoint(string clusterReadOnlyEndpoint, int clusterReadOnlyEndpointPort)
    {
        this.ClusterReadOnlyEndpoint = clusterReadOnlyEndpoint;
        this.ClusterReadOnlyEndpointPort = clusterReadOnlyEndpointPort;
    }

    public void SetInstanceEndpointSuffix(string instanceEndpointSuffix, int instanceEndpointPort)
    {
        this.InstanceEndpointSuffix = instanceEndpointSuffix;
        this.InstanceEndpointPort = instanceEndpointPort;
    }

    public TestInstanceInfo GetInstance(string instanceName)
    {
        foreach (var instance in this.Instances)
        {
            if (!string.IsNullOrEmpty(instanceName) && instanceName == instance.InstanceId)
            {
                return instance;
            }
        }

        throw new Exception($"Instance {instanceName} not found.");
    }

    public void MoveInstanceFirst(string instanceName)
    {
        for (int i = 0; i < this.Instances.Count; i++)
        {
            var currentInstance = this.Instances[i];
            if (!string.IsNullOrEmpty(instanceName) && instanceName == currentInstance.InstanceId)
            {
                this.Instances.RemoveAt(i);
                this.Instances.Insert(0, currentInstance);
                Console.WriteLine($"Moved writer instance {instanceName} to the first position.");
                return;
            }
        }

        throw new Exception($"Instance {instanceName} not found.");
    }
}
