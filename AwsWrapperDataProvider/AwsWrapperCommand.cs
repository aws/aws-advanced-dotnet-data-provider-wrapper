using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider
{
    public class AwsWrapperCommand : DbCommand
    {
        protected Type? _targetCommandType;
        protected DbConnection? _connection;
        protected AwsWrapperConnection? _wrapperConnection;
        protected DbCommand? _targetCommand;
        protected string? _commandText;
        protected int? _commandTimeout;
        protected DbTransaction? _transaction;

        public AwsWrapperCommand() { }

        public AwsWrapperCommand(DbCommand command, DbConnection connection)
        {
            Debug.Assert(command != null);
            Debug.Assert(connection is AwsWrapperConnection);
            this._targetCommand = command;
            this._targetCommandType = this._targetCommand.GetType();

            Debug.Assert(connection != null);
            this._connection = connection;
            this._wrapperConnection = connection is AwsWrapperConnection awsWrapperConnection ? awsWrapperConnection : null;
        }

        public AwsWrapperCommand(DbCommand command)
        {
            Debug.Assert(command != null);
            this._targetCommand = command;
            this._targetCommandType = this._targetCommand.GetType();
        }

        public AwsWrapperCommand(Type targetCommandType)
        {
            Debug.Assert(targetCommandType != null);
            this._targetCommandType = targetCommandType;
        }

        public AwsWrapperCommand(Type targetCommandType, string? commandText)
        {
            Debug.Assert(targetCommandType != null);
            this._targetCommandType = targetCommandType;

            this._commandText = commandText;
            this.EnsureCommandCreated();
        }

        public AwsWrapperCommand(Type targetCommandType, AwsWrapperConnection connection)
        {
            Debug.Assert(targetCommandType != null);
            this._targetCommandType = targetCommandType;

            Debug.Assert(connection != null);
            this._connection = connection;
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            this._targetCommand.Connection = _connection;
        }

        public AwsWrapperCommand(Type targetCommandType, string? commandText, AwsWrapperConnection connection)
        {
            Debug.Assert(targetCommandType != null);
            this._targetCommandType = targetCommandType;

            this._commandText = commandText;

            Debug.Assert(connection != null);
            this._connection = connection;
            this.EnsureCommandCreated();
        }

        public override string CommandText
        {
            get => this._commandText ?? string.Empty;
            set
            {
                this._commandText = value;
                if (this._targetCommand != null)
                {
                    this._targetCommand.CommandText = value ?? string.Empty;
                }
            }
        }
        public override int CommandTimeout
        {
            get
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                return this._targetCommand.CommandTimeout;
            }
            set
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                this._targetCommand.CommandTimeout = value;
            }
        }

        public override CommandType CommandType
        {
            get
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                return this._targetCommand.CommandType;
            }
            set
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                this._targetCommand.CommandType = value;
            }
        }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                return this._targetCommand.UpdatedRowSource;
            }
            set
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                this._targetCommand.UpdatedRowSource = value;
            }
        }

        protected override DbConnection? DbConnection 
        {
            get => this._connection;
            set
            {
                this._connection = value;
                this._wrapperConnection = this._connection is AwsWrapperConnection awsWrapperConnection ? awsWrapperConnection : null;
                if (this._targetCommand != null)
                {
                    this._targetCommand.Connection = this._wrapperConnection?.TargetConnection;
                }
            }
        }

        protected override System.Data.Common.DbParameterCollection DbParameterCollection
        {
            get
            {
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                return this._targetCommand.Parameters;
            }
        }

        protected override DbTransaction? DbTransaction 
        {
            get => this._targetCommand?.Transaction ?? this._transaction;
            set
            {
                this._transaction = value;
                this.EnsureCommandCreated();
                Debug.Assert(this._targetCommand != null);
                this._targetCommand.Transaction = value;
            }
        }

        public override void Cancel()
        {
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            this._targetCommand.Cancel();
        }

        public override int ExecuteNonQuery()
        {
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            var result = this._targetCommand.ExecuteNonQuery();
            Console.WriteLine("AwsWrapperCommand.ExecuteNonQuery()");
            return result;
        }

        public override object? ExecuteScalar()
        {
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            var result = this._targetCommand.ExecuteScalar();
            Console.WriteLine("AwsWrapperCommand.ExecuteScalar()");
            return result;
        }

        public override void Prepare()
        {
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            this._targetCommand.Prepare();
            Console.WriteLine("AwsWrapperCommand.Prepare()");
        }

        protected override DbParameter CreateDbParameter()
        {
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            return this._targetCommand.CreateParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            this.EnsureCommandCreated();
            Debug.Assert(this._targetCommand != null);
            //TODO: wrap over
            //return new AwsWrapperDataReader(this._targetCommand.ExecuteReader(behavior)); 
            return this._targetCommand.ExecuteReader(behavior);
        }

        protected void EnsureCommandCreated()
        {
            if (this._targetCommand == null)
            {
                if (this._targetCommandType != null) 
                {
                    this._targetCommand = Activator.CreateInstance(this._targetCommandType) as DbCommand;
                    if (this._targetCommand == null)
                    {
                        throw new Exception("Provided type doesn't implement IDbCommand.");
                    }
                } else if (this._connection != null) {
                    this._targetCommand = this._connection.CreateCommand();
                    if (this._targetCommandType == null)
                    {
                        this._targetCommandType = this._targetCommand?.GetType();
                    }
                }

                Debug.Assert(this._targetCommand != null);
                if (this._connection != null)
                {
                    this._targetCommand.Connection = this._connection;
                }
                if (this._commandText != null)
                {
                    this._targetCommand.CommandText = this._commandText;
                }
                if (this._commandTimeout != null)
                {
                    this._targetCommand.CommandTimeout = this._commandTimeout.Value;
                }
                if (this._transaction != null)
                {
                    this._targetCommand.Transaction = this._transaction;
                }
            }
        }
    }

    public class AwsWrapperCommand<TCommand> : AwsWrapperCommand where TCommand : IDbCommand
    {
        internal AwsWrapperCommand(DbCommand command, AwsWrapperConnection wrpperConnection) : base(command, wrpperConnection) { }

        public AwsWrapperCommand() : base(typeof(TCommand)) { }

        public AwsWrapperCommand(string? commandText) : base(typeof(TCommand), commandText) { }

        public AwsWrapperCommand(AwsWrapperConnection wrapperConnection) : base(typeof(TCommand), wrapperConnection) { }

        public AwsWrapperCommand(string? commandText, AwsWrapperConnection wrapperConnection) : base(typeof(TCommand), commandText, wrapperConnection) { }

    }

}
