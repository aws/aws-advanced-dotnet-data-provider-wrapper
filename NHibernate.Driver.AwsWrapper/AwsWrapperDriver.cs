using AwsWrapperDataProvider;
using NHibernate.AdoNet;
using NHibernate.Engine;
using System.Data.Common;
using System.Diagnostics;

namespace NHibernate.Driver.AwsWrapper
{
    public class AwsWrapperDriver : DriverBase, IEmbeddedBatcherFactoryProvider
    {
        protected System.Type? _targetDriverType = null;
        protected DriverBase? _targetDriver = null;
        protected bool _useNamedPrefixInSql = false;
        protected bool _useNamedPrefixInParameter = true;

        public AwsWrapperDriver() 
        { 
        }

        public AwsWrapperDriver(System.Type targetDriverType)
        {
            this._targetDriverType = targetDriverType;
            if (this._targetDriverType == null)
            {
                this._targetDriver = null;
            }
            else
            { 
                this._targetDriver = Activator.CreateInstance(this._targetDriverType) as DriverBase;
            }
        }

        public System.Type? TargetDriver 
        {
            get => this._targetDriverType;
            set
            {
                this._targetDriverType = value;
                if (this._targetDriverType == null)
                {
                    this._targetDriver = null;
                }
                else
                { 
                    this._targetDriver = Activator.CreateInstance(this._targetDriverType) as DriverBase;
                }
            }
        }

        public override bool UseNamedPrefixInSql => this._targetDriver?.UseNamedPrefixInSql ?? this._useNamedPrefixInSql;

        public void SetNamedPrefixInSql(bool value)
        {
            this._useNamedPrefixInSql = value;
        }

        public override bool UseNamedPrefixInParameter => this._targetDriver?.UseNamedPrefixInParameter ?? this._useNamedPrefixInParameter;

        public void SetUseNamedPrefixInParameter(bool value)
        {
            this._useNamedPrefixInParameter = value;
        }

        // TODO: ontinue with setters for more flags/settings

        public override string NamedPrefix => this._targetDriver?.NamedPrefix ?? "@";

        public override bool SupportsMultipleQueries => this._targetDriver?.SupportsMultipleQueries ?? true;

        public override bool SupportsMultipleOpenReaders => this._targetDriver?.SupportsMultipleOpenReaders ?? false;

        protected override bool SupportsPreparingCommands => true;

        public override DateTime MinDate => new(1000, 1, 1);

        public System.Type BatcherFactoryClass => (this._targetDriver != null && this._targetDriver is IEmbeddedBatcherFactoryProvider batcherFactoryProvider) 
        ? batcherFactoryProvider.BatcherFactoryClass 
        : typeof(GenericBatchingBatcherFactory);

        public override DbConnection CreateConnection() => this._targetDriver != null ? new AwsWrapperConnection(this._targetDriver.CreateConnection()) : new AwsWrapperConnection();

        public override DbCommand CreateCommand() => this._targetDriver != null ? new AwsWrapperCommand(this._targetDriver.CreateCommand()) : new AwsWrapperCommand();

        public override IResultSetsCommand GetResultSetsCommand(ISessionImplementor session) => this._targetDriver?.GetResultSetsCommand(session) ?? new BasicResultSetsCommand(session);
    }

    public class AwsWrapperDriver<TDriver> : AwsWrapperDriver where TDriver : IDriver
    {
        public AwsWrapperDriver() : base(typeof(TDriver)) { }
    }
}
