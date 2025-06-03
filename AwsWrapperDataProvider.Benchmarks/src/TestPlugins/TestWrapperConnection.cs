using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using Npgsql;

namespace AwsWrapperDataProvider.Benchmarks.TestPlugins;

public class MockConnection : DbConnection
{
    private ConnectionState _state = ConnectionState.Closed;

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new MockTransaction();

    public override void ChangeDatabase(string databaseName) => _state = _state; // No-op

    public override void Close() => _state = ConnectionState.Closed;

    public override void Open() => _state = ConnectionState.Open;

    [AllowNull] public override string ConnectionString { get; set; } = string.Empty;
    public override string Database => "MockDatabase";
    public override ConnectionState State => _state;
    public override string DataSource => "MockDataSource";
    public override string ServerVersion => "1.0.0";

    protected override DbCommand CreateDbCommand() => new MockCommand();
}

public class MockCommand : DbCommand
{
    private readonly List<DbParameter> _parameters = new();
    
    public override void Cancel() { } // No-op

    public override int ExecuteNonQuery() => 1; // Return a default value of 1 row affected

    public override object? ExecuteScalar() => null; // Return null as default

    public override void Prepare() { } // No-op

    [AllowNull] public override string CommandText { get; set; } = string.Empty;
    public override int CommandTimeout { get; set; } = 30;
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;
    protected override DbConnection? DbConnection { get; set; }
    
    protected override DbParameterCollection DbParameterCollection => new MockParameterCollection(_parameters);
    
    protected override DbTransaction? DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; } = true;

    protected override DbParameter CreateDbParameter() => new MockParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new MockDataReader();
}

public class MockParameterCollection : DbParameterCollection
{
    private readonly List<DbParameter> _parameters;

    public MockParameterCollection(List<DbParameter> parameters)
    {
        _parameters = parameters;
    }

    public override int Add(object value)
    {
        _parameters.Add((DbParameter)value);
        return _parameters.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (DbParameter parameter in values)
        {
            _parameters.Add(parameter);
        }
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => _parameters.Contains((DbParameter)value);

    public override bool Contains(string value) => _parameters.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index)
    {
        for (int i = 0; i < _parameters.Count; i++)
        {
            array.SetValue(_parameters[i], index + i);
        }
    }

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);

    public override int IndexOf(string parameterName) => _parameters.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);

    public override void Remove(object value) => _parameters.Remove((DbParameter)value);

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        int index = IndexOf(parameterName);
        if (index >= 0)
        {
            _parameters.RemoveAt(index);
        }
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName) => 
        _parameters.First(p => p.ParameterName == parameterName);

    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        int index = IndexOf(parameterName);
        if (index >= 0)
        {
            _parameters[index] = value;
        }
    }

    public override int Count => _parameters.Count;
    public override object SyncRoot => this;
    public override bool IsFixedSize => false;
    public override bool IsReadOnly => false;
    public override bool IsSynchronized => false;
}

public class MockParameter : DbParameter
{
    public override void ResetDbType() => DbType = DbType.String;

    public override DbType DbType { get; set; } = DbType.String;
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; } = string.Empty;
    public override string SourceColumn { get; set; } = string.Empty;
    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }
}

public class MockDataReader : DbDataReader
{
    private bool _isClosed = false;
    private bool _hasRows = false;
    private int _currentRow = -1;
    private readonly int _rowCount = 0;

    public override bool GetBoolean(int ordinal) => false;

    public override byte GetByte(int ordinal) => 0;

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;

    public override char GetChar(int ordinal) => ' ';

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;

    public override string GetDataTypeName(int ordinal) => "string";

    public override DateTime GetDateTime(int ordinal) => DateTime.Now;

    public override decimal GetDecimal(int ordinal) => 0;

    public override double GetDouble(int ordinal) => 0;

    public override Type GetFieldType(int ordinal) => typeof(string);

    public override float GetFloat(int ordinal) => 0;

    public override Guid GetGuid(int ordinal) => Guid.Empty;

    public override short GetInt16(int ordinal) => 0;

    public override int GetInt32(int ordinal) => 0;

    public override long GetInt64(int ordinal) => 0;

    public override string GetName(int ordinal) => $"Column{ordinal}";

    public override int GetOrdinal(string name) => 0;

    public override string GetString(int ordinal) => string.Empty;

    public override object GetValue(int ordinal) => DBNull.Value;

    public override int GetValues(object[] values) => 0;

    public override bool IsDBNull(int ordinal) => true;

    public override int FieldCount => 0;

    public override object this[int ordinal] => DBNull.Value;

    public override object this[string name] => DBNull.Value;

    public override int RecordsAffected => 0;
    public override bool HasRows => _hasRows;
    public override bool IsClosed => _isClosed;

    public override bool NextResult() => false;

    public override bool Read()
    {
        _currentRow++;
        return _currentRow < _rowCount;
    }

    public override int Depth => 0;

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    public override void Close() => _isClosed = true;
}

public class MockTransaction : DbTransaction
{
    private readonly IsolationLevel _isolationLevel = IsolationLevel.ReadCommitted;

    public override void Commit() { } // No-op

    public override void Rollback() { } // No-op

    protected override DbConnection? DbConnection => null;
    public override IsolationLevel IsolationLevel => _isolationLevel;
}

public class MockConnectionDialect : ITargetConnectionDialect
{
    public Type DriverConnectionType { get; } = typeof(MockConnection);
    public bool IsDialect(Type connectionType)
    {
        return true;
    }

    public string PrepareConnectionString(IDialect dialect, HostSpec? hostSpec, Dictionary<string, string> props) => string.Empty;

    public ISet<string> GetAllowedOnConnectionMethodNames() => new HashSet<string> { "*" };
}

public class MockDialect : IDialect
{
    public int DefaultPort { get; } = HostSpec.NoPort;
    public string HostAliasQuery { get; } = string.Empty;
    public string ServerVersionQuery { get; } = string.Empty;
    public IList<Type> DialectUpdateCandidates { get; } = [];

    public HostListProviderSupplier HostListProviderSupplier { get; } = (props,
        hostListProviderService,
        pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public bool IsDialect(IDbConnection conn) => true;

    public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
    {
        // Do nothing.
    }
}
