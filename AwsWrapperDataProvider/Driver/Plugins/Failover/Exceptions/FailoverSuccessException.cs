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

namespace AwsWrapperDataProvider.Driver.Plugins.Failover;

/// <summary>
/// Exception thrown when a failover operation completes successfully.
/// This exception is used to signal that the connection has changed and
/// the application should re-configure session state if required.
/// </summary>
public class FailoverSuccessException : DbException
{
    public FailoverSuccessException() : base("Failover completed successfully")
    {
    }

    public FailoverSuccessException(string message) : base(message)
    {
    }

    public FailoverSuccessException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
