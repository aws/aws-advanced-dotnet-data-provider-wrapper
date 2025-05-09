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
using System.Collections;
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
    public class AwsWrapperDataReader : DbDataReader
    {
        protected DbDataReader _dataReader;

        internal AwsWrapperDataReader(DbDataReader dataReader)
        {
            Debug.Assert(dataReader != null);
            this._dataReader = dataReader;
        }

        public override int Depth
        {
            get => this._dataReader.Depth;
        }

        public override bool IsClosed
        {
            get => this._dataReader.IsClosed;
        }

        public override int RecordsAffected
        {
            get => this._dataReader.RecordsAffected;
        }

        public override void Close()
        {
            // TODO: wrap over
            this._dataReader.Close();
        }

        public override DataTable? GetSchemaTable()
        {
            return this._dataReader.GetSchemaTable();
        }

        public override bool NextResult()
        {
            // TODO: wrap over
            return this._dataReader.NextResult();
        }

        public override bool Read()
        {
            // TODO: wrap over
            return this._dataReader.Read();
        }

        public override int FieldCount => this._dataReader.FieldCount;

        public override bool HasRows => this._dataReader.HasRows;

        public override object this[int i] => this._dataReader[i];

        public override object this[string name] => this._dataReader[name];

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this._dataReader.Dispose();
            }
        }

        public override bool GetBoolean(int i)
        {
            return this._dataReader.GetBoolean(i);
        }

        public override byte GetByte(int i)
        {
            return this._dataReader.GetByte(i);
        }

        public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            return this._dataReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public override char GetChar(int i)
        {
            return this._dataReader.GetChar(i);
        }

        public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
        {
            return this._dataReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public override string GetDataTypeName(int i)
        {
            return this._dataReader.GetDataTypeName(i);
        }

        public override DateTime GetDateTime(int i)
        {
            return this._dataReader.GetDateTime(i);
        }

        public override decimal GetDecimal(int i)
        {
            return this._dataReader.GetDecimal(i);
        }

        public override double GetDouble(int i)
        {
            return this._dataReader.GetDouble(i);
        }

        [return:
            DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields |
                                       DynamicallyAccessedMemberTypes.PublicProperties)]
        public override Type GetFieldType(int i)
        {
            return this._dataReader.GetFieldType(i);
        }

        public override float GetFloat(int i)
        {
            return this._dataReader.GetFloat(i);
        }

        public override Guid GetGuid(int i)
        {
            return this._dataReader.GetGuid(i);
        }

        public override short GetInt16(int i)
        {
            return this._dataReader.GetInt16(i);
        }

        public override int GetInt32(int i)
        {
            return this._dataReader.GetInt32(i);
        }

        public override long GetInt64(int i)
        {
            return this._dataReader.GetInt64(i);
        }

        public override string GetName(int i)
        {
            return this._dataReader.GetString(i);
        }

        public override int GetOrdinal(string name)
        {
            return this._dataReader.GetOrdinal(name);
        }

        public override string GetString(int i)
        {
            return this._dataReader.GetString(i);
        }

        public override object GetValue(int i)
        {
            return this._dataReader.GetValue(i);
        }

        public override int GetValues(object[] values)
        {
            return this._dataReader.GetValues(values);
        }

        public override bool IsDBNull(int i)
        {
            return this._dataReader.IsDBNull(i);
        }

        public override IEnumerator GetEnumerator()
        {
            return this._dataReader.GetEnumerator();
        }
    }
}
