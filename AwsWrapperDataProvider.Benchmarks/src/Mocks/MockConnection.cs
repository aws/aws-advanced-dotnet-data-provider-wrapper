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

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace AwsWrapperDataProvider.Benchmarks.Mocks;

public class MockConnection : DbConnection
{
    private ConnectionState _state = ConnectionState.Closed;

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new MockTransaction();

    public override void ChangeDatabase(string databaseName) { /* No-op */ }

    public override void Close() => this._state = ConnectionState.Closed;

    public override void Open() => this._state = ConnectionState.Open;

    [AllowNull]
    public override string ConnectionString { get; set; } = "197.0.0.1";
    public override string Database => "MockDatabase";
    public override ConnectionState State => this._state;
    public override string DataSource => "MockDataSource";
    public override string ServerVersion => "1.0.0";

    protected override DbCommand CreateDbCommand() => new MockCommand();
}

public class MockCommand : DbCommand
{
    private readonly List<DbParameter> _parameters = new();

    public override void Cancel() { } // Do nothing

    public override int ExecuteNonQuery() => 1;

    public override object? ExecuteScalar() => null;

    public override void Prepare() { } // Do nothing

    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;
    public override int CommandTimeout { get; set; } = 30;
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;
    protected override DbConnection? DbConnection { get; set; }

    protected override DbParameterCollection DbParameterCollection => new MockParameterCollection(this._parameters);

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
        this._parameters = parameters;
    }

    public override int Add(object value)
    {
        this._parameters.Add((DbParameter)value);
        return this._parameters.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (DbParameter parameter in values)
        {
            this._parameters.Add(parameter);
        }
    }

    public override void Clear() => this._parameters.Clear();

    public override bool Contains(object value) => this._parameters.Contains((DbParameter)value);

    public override bool Contains(string value) => this._parameters.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index)
    {
        for (int i = 0; i < this._parameters.Count; i++)
        {
            array.SetValue(this._parameters[i], index + i);
        }
    }

    public override IEnumerator GetEnumerator() => this._parameters.GetEnumerator();

    public override int IndexOf(object value) => this._parameters.IndexOf((DbParameter)value);

    public override int IndexOf(string parameterName) => this._parameters.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => this._parameters.Insert(index, (DbParameter)value);

    public override void Remove(object value) => this._parameters.Remove((DbParameter)value);

    public override void RemoveAt(int index) => this._parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        int index = this.IndexOf(parameterName);
        if (index >= 0)
        {
            this._parameters.RemoveAt(index);
        }
    }

    protected override DbParameter GetParameter(int index) => this._parameters[index];

    protected override DbParameter GetParameter(string parameterName) =>
        this._parameters.First(p => p.ParameterName == parameterName);

    protected override void SetParameter(int index, DbParameter value) => this._parameters[index] = value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        int index = this.IndexOf(parameterName);
        if (index >= 0)
        {
            this._parameters[index] = value;
        }
    }

    public override int Count => this._parameters.Count;
    public override object SyncRoot => this;
    public override bool IsFixedSize => false;
    public override bool IsReadOnly => false;
    public override bool IsSynchronized => false;
}

public class MockParameter : DbParameter
{
    private readonly string _parameterName = string.Empty;
    public override void ResetDbType() => this.DbType = DbType.String;
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
    private readonly bool _hasRows = false;
    private readonly int _rowCount = 0;
    private bool _isClosed = false;
    private int _currentRow = -1;

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
    public override bool HasRows => this._hasRows;
    public override bool IsClosed => this._isClosed;

    public override bool NextResult() => false;

    public override bool Read()
    {
        this._currentRow++;
        return this._currentRow < this._rowCount;
    }

    public override int Depth => 0;

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    public override void Close() => this._isClosed = true;
}

public class MockTransaction : DbTransaction
{
    private readonly IsolationLevel _isolationLevel = IsolationLevel.ReadCommitted;

    public override void Commit() { } // No-op

    public override void Rollback() { } // No-op

    protected override DbConnection? DbConnection => null;
    public override IsolationLevel IsolationLevel => this._isolationLevel;
}
