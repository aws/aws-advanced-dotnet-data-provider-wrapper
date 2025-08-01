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
/// Exception thrown when a failover occurs during a transaction and the transaction state becomes unknown.
/// This indicates that the application should re-configure session state and restart the transaction.
/// </summary>
public class TransactionStateUnknownException : DbException
{
    public TransactionStateUnknownException() : base("Transaction state is unknown after failover")
    {
    }

    public TransactionStateUnknownException(string message) : base(message)
    {
    }

    public TransactionStateUnknownException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
