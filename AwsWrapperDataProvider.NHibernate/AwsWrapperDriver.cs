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
using NHibernate.AdoNet;
using NHibernate.Driver;
using NHibernate.Engine;

namespace AwsWrapperDataProvider.NHibernate
{
    public class AwsWrapperDriver : DriverBase, IEmbeddedBatcherFactoryProvider
    {
        protected DriverBase? _targetDriver;
        private AwsWrapperConnection? _lastCreatedConnection;

        public AwsWrapperDriver(Type targetDriverType)
        {
            this._targetDriver = Activator.CreateInstance(targetDriverType) as DriverBase;
        }

        public override bool UseNamedPrefixInSql => this._targetDriver?.UseNamedPrefixInSql ?? false;
        public override bool UseNamedPrefixInParameter => this._targetDriver?.UseNamedPrefixInParameter ?? true;
        public override string NamedPrefix => this._targetDriver?.NamedPrefix ?? "@";
        public override bool SupportsMultipleQueries => this._targetDriver?.SupportsMultipleQueries ?? true;
        public override bool SupportsMultipleOpenReaders => this._targetDriver?.SupportsMultipleOpenReaders ?? false;
        protected override bool SupportsPreparingCommands => true;
        public override DateTime MinDate => new(1000, 1, 1);

        public Type BatcherFactoryClass => (this._targetDriver is IEmbeddedBatcherFactoryProvider provider)
            ? provider.BatcherFactoryClass
            : typeof(GenericBatchingBatcherFactory);

        public override DbConnection CreateConnection()
        {
            var targetConnection = this._targetDriver?.CreateConnection() ?? throw new InvalidOperationException("Target driver not set");
            this._lastCreatedConnection = new AwsWrapperConnection(targetConnection.GetType());
            return this._lastCreatedConnection;
        }

        public override DbCommand CreateCommand()
        {
            if (this._lastCreatedConnection == null)
            {
                throw new InvalidOperationException("CreateConnection must be called before CreateCommand. The AwsWrapperDriver requires a connection to be created first.");
            }

            return this._lastCreatedConnection.CreateCommand();
        }

        public override IResultSetsCommand GetResultSetsCommand(ISessionImplementor session) =>
            this._targetDriver?.GetResultSetsCommand(session) ?? new BasicResultSetsCommand(session);
    }

    public class AwsWrapperDriver<TDriver> : AwsWrapperDriver where TDriver : IDriver
    {
        public AwsWrapperDriver() : base(typeof(TDriver)) { }
    }
}
