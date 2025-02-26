using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider
{
    public class AwsWrapperConnection : DbConnection
    {
        protected static HashSet<string> wrapperParameterNames = new(["targetConnectionType", "targetCommandType"]);

        protected Type? _targetType;
        protected DbConnection? _targetConnection = null;
        protected string? _connectionString;
        protected string? _targetConnectionString;
        protected string? _database;
        protected Dictionary<string, string> _parameters = new Dictionary<string, string>();
        protected Dictionary<string, string> _targetConnectionParameters = new Dictionary<string, string>();

        public AwsWrapperConnection() : base() { }

        public AwsWrapperConnection(DbConnection connection) : base()
        {
            this._targetConnection = connection;
            if (this._targetConnection != null)
            {
                this._targetType = this._targetConnection.GetType();
                this._connectionString = this._targetConnection.ConnectionString;
                this._database = this._targetConnection.Database;
            }
        }

        public AwsWrapperConnection(string connectionString) : base()
        {
            this._connectionString = connectionString;
            this.ParseConnectionStringParameters();
            this.EnsureTargetType();
        }

        public AwsWrapperConnection(Type targetConnectionType) : base()
        {
            this._targetType = targetConnectionType;
        }

        public AwsWrapperConnection(Type targetConnectionType, string connectionString) : base()
        {
            this._connectionString = connectionString;
            this._targetType = targetConnectionType;
            this.ParseConnectionStringParameters();
        }

        [AllowNull]
        public override string ConnectionString
        {
            get => this._targetConnection?.ConnectionString ?? this._connectionString ?? string.Empty;
            set
            {
                this._connectionString = value;
                this.ParseConnectionStringParameters();
                this.EnsureTargetType();

                if (this._targetConnection != null)
                {
                    this._targetConnection.ConnectionString = value;
                }
            }
        }

        public override string Database => this._targetConnection?.Database ?? this._database ?? string.Empty;

        public override string DataSource => this._targetConnection is DbConnection dbConnection ? dbConnection.DataSource : string.Empty;

        public override string ServerVersion => this._targetConnection is DbConnection dbConnection ? dbConnection.ServerVersion : string.Empty;

        public override ConnectionState State => this._targetConnection?.State ?? ConnectionState.Closed;

        public override void ChangeDatabase(string databaseName)
        {
            this._database = databaseName;
            if (this._targetConnection != null)
            {
                this._targetConnection.ChangeDatabase(databaseName);
            }
        }

        public override void Close()
        {
            this._targetConnection?.Close();
            this._targetConnection = null;
        }

        public override void Open()
        {            
            this.EnsureConnectionCreated();
            Debug.Assert(this._targetConnection != null);
            this._targetConnection.Open();
            Console.WriteLine("AwsWrapperConnection.Open()");
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            this.EnsureConnectionCreated();
            Debug.Assert(this._targetConnection != null);
            var result = this._targetConnection.BeginTransaction(isolationLevel);
            Console.WriteLine("AwsWrapperConnection.BeginDbTransaction()");
            return result;
        }

        protected override DbCommand CreateDbCommand() => CreateCommand();

        public new AwsWrapperCommand CreateCommand()
        {
            this.EnsureConnectionCreated();
            Debug.Assert(this._targetConnection != null);
            DbCommand command = this._targetConnection.CreateCommand();
            var result = new AwsWrapperCommand(command, this);
            Console.WriteLine("AwsWrapperConnection.CreateCommand()");
            return result;
        }

        public AwsWrapperCommand<TCommand> CreateCommand<TCommand>() where TCommand : IDbCommand
        {
            this.EnsureConnectionCreated();
            Debug.Assert(this._targetConnection != null);
            DbCommand command = this._targetConnection.CreateCommand();
            return new AwsWrapperCommand<TCommand>(command, this);
        }

        public DbConnection? TargetConnection => this._targetConnection;

        protected void EnsureTargetType() 
        {
            if (this._targetType == null)
            {
                string? targetConnectionTypeString = this.GetTargetConnectionTypeName();
                if (!string.IsNullOrEmpty(targetConnectionTypeString))
                {
                    try
                    {
                        this._targetType = Type.GetType(targetConnectionTypeString);
                        if (this._targetType == null)
                        {
                            throw new Exception("Can't load target connection type " + targetConnectionTypeString);
                        }
                    }
                    catch
                    {
                        throw new Exception("Can't load target connection type " + targetConnectionTypeString);
                    }
                }
            }
        }

        protected void EnsureConnectionCreated()
        {
            if (this._targetConnection != null)
            {
                return;
            }

            Debug.Assert(this._targetType != null);
            this._targetConnection = String.IsNullOrWhiteSpace(this._targetConnectionString)
                ? (DbConnection?)Activator.CreateInstance(this._targetType)
                : (DbConnection?)Activator.CreateInstance(this._targetType, this._targetConnectionString);
        }

        protected string? GetTargetConnectionTypeName()
        {
            return this.GetTargetConnectionTypeName("targetConnectionType");
        }

        protected string? GetTargetCommandTypeName()
        {
            return this.GetTargetConnectionTypeName("targetCommandType");
        }

        protected string? GetTargetConnectionTypeName(string parameterName)
        {
            if (!this._parameters.TryGetValue(parameterName, out string? typeName))
            {
                return null;
            }

            if (String.IsNullOrEmpty(typeName))
            {
                throw new Exception($"Parameter {parameterName} value is invalid.");
            }

            return typeName;
        }

        protected void ParseConnectionStringParameters()
        {
            if (String.IsNullOrEmpty(this._connectionString))
            {
                throw new ArgumentNullException("Can't parse targetConnectionType parameter from connection string.");
            }
            this._parameters = this._connectionString
                .Split(";", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Split("=", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
                .Select(x => new { Key = x.Length > 0 ? x[0] : null, Value = x.Length > 1 ? x[1] : null })
                .Where(x => x.Key != null && x.Value != null)
                .ToDictionary(k => k.Key ?? string.Empty, v => v.Value ?? string.Empty);

            this._targetConnectionParameters = this._parameters.Where(x => !wrapperParameterNames.Contains(x.Key)).ToDictionary();
            this._targetConnectionString = string.Join("; ", this._targetConnectionParameters.Select(x => string.Format("{0}={1}", x.Key, x.Value)));
        }
    }

    public class AwsWrapperConnection<TConn> : AwsWrapperConnection where TConn : DbConnection
    {
        public AwsWrapperConnection(string connectionString) : base(typeof(TConn), connectionString) { }

        public new TConn? TargetConnection => this._targetConnection as TConn;
    }

}
