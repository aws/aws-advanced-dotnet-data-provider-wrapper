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

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsWrapperDataProvider
{
    public class AwsWrapperDataReader : IDataReader
    {
        protected IDataReader _dataReader;

        internal AwsWrapperDataReader(IDataReader dataReader)
        {
            Debug.Assert(dataReader != null);
            this._dataReader = dataReader;
        }

        public int Depth
        {
            get => this._dataReader.Depth;
        }

        public bool IsClosed
        {
            get => this._dataReader.IsClosed;
        }

        public int RecordsAffected
        {
            get => this._dataReader.RecordsAffected;
        }

        public void Close()
        {
            // TODO: wrap over
            this._dataReader.Close();
        }

        public DataTable? GetSchemaTable()
        {
            return this._dataReader.GetSchemaTable();
        }

        public bool NextResult()
        {
            // TODO: wrap over
            return this._dataReader.NextResult();
        }

        public bool Read()
        {
            // TODO: wrap over
            return this._dataReader.Read();
        }

        public int FieldCount => this._dataReader.FieldCount;

        public object this[int i] => this._dataReader[i];

        public object this[string name] => this._dataReader[name];

        public void Dispose()
        {
            this._dataReader?.Dispose();
        }

        public bool GetBoolean(int i)
        {
            return this._dataReader.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return this._dataReader.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            return this._dataReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return this._dataReader.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
        {
            return this._dataReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return this._dataReader.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return this._dataReader.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return this._dataReader.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return this._dataReader.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return this._dataReader.GetDouble(i);
        }

        [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
        public Type GetFieldType(int i)
        {
            return this._dataReader.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return this._dataReader.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return this._dataReader.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return this._dataReader.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return this._dataReader.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return this._dataReader.GetInt64(i);
        }

        public string GetName(int i)
        {
            return this._dataReader.GetString(i);
        }

        public int GetOrdinal(string name)
        {
            return this._dataReader.GetOrdinal(name);
        }

        public string GetString(int i)
        {
            return this._dataReader.GetString(i);
        }

        public object GetValue(int i)
        {
            return this._dataReader.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            return this._dataReader.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return this._dataReader.IsDBNull(i);
        }
    }
}
