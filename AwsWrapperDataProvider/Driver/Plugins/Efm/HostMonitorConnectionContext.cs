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

using System.Data.Common;

namespace AwsWrapperDataProvider.Driver.Plugins.Efm;

public class HostMonitorConnectionContext
{
    private readonly WeakReference<DbConnection?> connectionToAbort = new(null);
    private readonly object contextLock = new();
    private volatile bool nodeUnhealthy = false;

    public bool NodeUnhealthy
    {
        get => this.nodeUnhealthy;
        set => this.nodeUnhealthy = value;
    }

    public HostMonitorConnectionContext(DbConnection connectionToAbort)
    {
        this.connectionToAbort.SetTarget(connectionToAbort);
    }

    public bool ShouldAbort()
    {
        lock (this.contextLock)
        {
            return this.NodeUnhealthy && this.connectionToAbort.TryGetTarget(out _);
        }
    }

    public void SetInactive()
    {
        lock (this.contextLock)
        {
            this.connectionToAbort.SetTarget(null);
        }
    }

    public DbConnection? GetConnection()
    {
        DbConnection? conn = null;

        lock (this.contextLock)
        {
            this.connectionToAbort.TryGetTarget(out conn);
        }

        return conn;
    }

    public bool IsActive()
    {
        lock (this.contextLock)
        {
            return this.connectionToAbort.TryGetTarget(out DbConnection? conn) && conn != null;
        }
    }
}
