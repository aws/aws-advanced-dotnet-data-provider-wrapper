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

using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetDriverDialects;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider
{
    public class AwsWrapperConnection : DbConnection
    {
        private readonly ConnectionPluginManager _pluginManager;
        private readonly IPluginService _pluginService;
        private readonly IHostListProviderService _hostListProviderService;

        protected static HashSet<string> wrapperParameterNames = new(["targetConnectionType", "targetCommandType", "targetParameterType"]);

        private Type _targetType;
        private string _connectionString;
        protected DbConnection? _targetConnection;
        private string? _database;
        private Dictionary<string, string> _parameters = new Dictionary<string, string>();

        public DbConnection? TargetConnection => this._targetConnection;

        [AllowNull]
        public override string ConnectionString
        {
            get => this._targetConnection?.ConnectionString ?? this._connectionString;
            set
            {
                this._connectionString = value ?? string.Empty;
                this._parameters = ConnectionUrlParser.ParseConnectionStringParameters(this._connectionString);
                this._targetType = this.GetTargetType(this._parameters);

                if (this._targetConnection != null)
                {
                    this._targetConnection.ConnectionString = value;
                }
            }
        }

        public AwsWrapperConnection(DbConnection connection) : this(connection.GetType(), connection.ConnectionString)
        {
            Debug.Assert(connection != null);
            this._targetConnection = connection;
            this._database = connection.Database;
        }

        public AwsWrapperConnection(string connectionString) : this(null, connectionString) { }

        public AwsWrapperConnection(Type? targetType, string connectionString) : base()
        {
            this._connectionString = connectionString;
            this._parameters = ConnectionUrlParser.ParseConnectionStringParameters(this._connectionString);
            this._targetType = targetType ?? this.GetTargetType(this._parameters);

            ITargetDriverDialect driverDialect =
                TargetDriverDialectProvider.GetDialect(this._targetType, this._parameters);

            IConnectionProvider connectionProvider = new DriverConnectionProvider(
                this._targetType);

            this._pluginManager = new ConnectionPluginManager(
                connectionProvider,
                null,
                this);

            PluginService pluginService = new PluginService(this._targetConnection, this._targetType, this._pluginManager, this._parameters, this._connectionString, driverDialect);

            this._pluginService = pluginService;
            this._hostListProviderService = pluginService;

            this._pluginManager.Init(this._pluginService, this._parameters);

            // Set HostListProvider
            HostListProviderSupplier supplier = this._pluginService.Dialect.HostListProviderSupplier;
            IHostListProvider? provider = supplier.Invoke(this._parameters, this._connectionString, this._hostListProviderService, this._pluginService);
            this._hostListProviderService.HostListProvider = provider;

            this._pluginManager.InitHostProvider(this._connectionString, this._parameters, this._hostListProviderService);
            this._pluginService.RefreshHostList();

            DbConnection? conn = null;

            if (this._pluginService.CurrentConnection == null)
            {
                conn = this._pluginManager.Connect(
                    this._pluginService.InitialConnectionHostSpec,
                    this._parameters,
                    true,
                    null);
            }

            if (conn == null)
            {
                throw new Exception($"Can't connect to target connection {this._connectionString}");
            }

            this._pluginService.SetCurrentConnection(conn, this._pluginService.InitialConnectionHostSpec);
            this._pluginService.RefreshHostList();
        }

        public override string Database => this._targetConnection?.Database ?? this._database ?? string.Empty;

        public override string DataSource => this._targetConnection is DbConnection dbConnection
            ? dbConnection.DataSource
            : string.Empty;

        public override string ServerVersion => this._targetConnection is DbConnection dbConnection
            ? dbConnection.ServerVersion
            : string.Empty;

        public override ConnectionState State => this._targetConnection?.State ?? ConnectionState.Closed;

        public override void ChangeDatabase(string databaseName)
        {
            this._database = databaseName;
            this._targetConnection?.ChangeDatabase(databaseName);
        }

        // TODO: switch method to use pluginService
        public override void Close()
        {
            this._targetConnection?.Close();
        }

        public override void Open()
        {
            ArgumentNullException.ThrowIfNull(this._pluginService.CurrentConnection);
            this._pluginManager.Execute(
                this._pluginService.CurrentConnection,
                "DbConnection.CreateCommand",
                (args) => this._pluginService.CurrentConnection.Open(),
                []);
            Console.WriteLine("AwsWrapperConnection.Open()");
        }

        // TODO: switch method to use pluginService
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            Debug.Assert(this._targetConnection != null);
            DbTransaction result = this._targetConnection.BeginTransaction(isolationLevel);
            Console.WriteLine("AwsWrapperConnection.BeginDbTransaction()");
            return result;
        }

        protected override DbCommand CreateDbCommand() => this.CreateCommand();

        // TODO: implement WrapperUtils.executeWithPlugins.
        public new AwsWrapperCommand CreateCommand()
        {
            ArgumentNullException.ThrowIfNull(this._pluginService.CurrentConnection);
            DbCommand command = this._pluginManager.Execute<DbCommand>(
                this._pluginService.CurrentConnection,
                "DbConnection.CreateCommand",
                (args) => this._pluginService.CurrentConnection.CreateCommand(),
                []);
            var result = new AwsWrapperCommand(command, this, this._pluginManager);
            Console.WriteLine("AwsWrapperConnection.CreateCommand()");
            return result;
        }

        // TODO: implement WrapperUtils.executeWithPlugins.
        public AwsWrapperCommand<TCommand> CreateCommand<TCommand>() where TCommand : DbCommand
        {
            ArgumentNullException.ThrowIfNull(this._pluginService.CurrentConnection);

            TCommand command = this._pluginManager.Execute<TCommand>(
                this._pluginService.CurrentConnection,
                "DbConnection.CreateCommand",
                (args) => (TCommand)this._pluginService.CurrentConnection.CreateCommand(),
                []);
            return new AwsWrapperCommand<TCommand>(command, this, this._pluginManager);
        }

        private Type GetTargetType(Dictionary<string, string> parameters)
        {
            string? targetConnectionTypeString = PropertyDefinition.TargetConnectionType.GetString(parameters);
            if (!string.IsNullOrEmpty(targetConnectionTypeString))
            {
                try
                {
                    Type? targetType = Type.GetType(targetConnectionTypeString);
                    if (targetType == null)
                    {
                        throw new Exception("Can't load target connection type " + targetConnectionTypeString);
                    }

                    return targetType;
                }
                catch
                {
                    throw new Exception("Can't load target connection type " + targetConnectionTypeString);
                }
            }

            throw new Exception($"Can't load target connection type {targetConnectionTypeString}");
        }
    }

    public class AwsWrapperConnection<TConn> : AwsWrapperConnection where TConn : DbConnection
    {
        public AwsWrapperConnection(string connectionString) : base(typeof(TConn), connectionString)
        {
        }

        public new TConn? TargetConnection => this._targetConnection as TConn;
    }
}
