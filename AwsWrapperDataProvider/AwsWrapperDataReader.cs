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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider
{
    public class AwsWrapperDataReader : DbDataReader
    {
        private readonly ConnectionPluginManager _connectionPluginManager;
        protected DbDataReader _targetDataReader;

        internal AwsWrapperDataReader(DbDataReader targetDataReader, ConnectionPluginManager connectionPluginManager)
        {
            this._connectionPluginManager = connectionPluginManager;
            this._targetDataReader = targetDataReader;
        }

        public override int Depth => WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.Depth",
                () => this._targetDataReader.Depth);

        public override bool IsClosed => WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.IsClosed",
                () => this._targetDataReader!.IsClosed);

        public override int RecordsAffected => WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.RecordsAffected",
                () => this._targetDataReader.RecordsAffected);

        public override int FieldCount => WrapperUtils.ExecuteWithPlugins(
            this._connectionPluginManager,
            this._targetDataReader,
            "DbDataReader.FieldCount",
            () => this._targetDataReader!.FieldCount);

        public override bool HasRows => WrapperUtils.ExecuteWithPlugins(
            this._connectionPluginManager,
            this._targetDataReader,
            "DbDataReader.HasRows",
            () => this._targetDataReader!.HasRows);

        public override object this[int i] => WrapperUtils.ExecuteWithPlugins(
            this._connectionPluginManager,
            this._targetDataReader,
            "DbDataReader[i]",
            () => this._targetDataReader[i]);

        public override object this[string name] => WrapperUtils.ExecuteWithPlugins(
            this._connectionPluginManager,
            this._targetDataReader,
            "DbDataReader[name]",
            () => this._targetDataReader[name]);

        public override void Close()
        {
            WrapperUtils.RunWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.Close",
                () => this._targetDataReader.Close());
        }

        public override DataTable? GetSchemaTable()
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetSchemaTable",
                () => this._targetDataReader.GetSchemaTable());
        }

        public override bool NextResult()
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.NextResult",
                () => this._targetDataReader!.NextResult());
        }

        public override bool Read()
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.Read()",
                () => this._targetDataReader!.Read());
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                WrapperUtils.RunWithPlugins(
                    this._connectionPluginManager,
                    this._targetDataReader,
                    "DbDataReader.Dispose",
                    () => this._targetDataReader!.Dispose());
            }
        }

        public override bool GetBoolean(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetBoolean",
                () => this._targetDataReader!.GetBoolean(i));
        }

        public override byte GetByte(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetByte",
                () => this._targetDataReader!.GetByte(i));
        }

        public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetBytes",
                () => this._targetDataReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length));
        }

        public override char GetChar(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetChar",
                () => this._targetDataReader!.GetChar(i));
        }

        public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetChars",
                () => this._targetDataReader.GetChars(i, fieldoffset, buffer, bufferoffset, length));
        }

        public override string GetDataTypeName(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetDataTypeName",
                () => this._targetDataReader!.GetDataTypeName(i));
        }

        public override DateTime GetDateTime(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetDataTypeTime",
                () => this._targetDataReader!.GetDateTime(i));
        }

        public override decimal GetDecimal(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetDecimal",
                () => this._targetDataReader.GetDecimal(i));
        }

        public override double GetDouble(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetDouble",
                () => this._targetDataReader.GetDouble(i));
        }

        // TODO: Write integration test to check if user can use reflection on Type after trimming.
        [return:
            DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields |
                                       DynamicallyAccessedMemberTypes.PublicProperties)]
        public override Type GetFieldType(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetFieldType",
                () => this._targetDataReader.GetFieldType(i));
        }

        public override float GetFloat(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetFloat",
                () => this._targetDataReader.GetFloat(i));
        }

        public override Guid GetGuid(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetGuid",
                () => this._targetDataReader.GetGuid(i));
        }

        public override short GetInt16(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetInt16",
                () => this._targetDataReader.GetInt16(i));
        }

        public override int GetInt32(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetInt32",
                () => this._targetDataReader.GetInt32(i));
        }

        public override long GetInt64(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetInt64",
                () => this._targetDataReader.GetInt64(i));
        }

        public override string GetName(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetName",
                () => this._targetDataReader.GetName(i));
        }

        public override int GetOrdinal(string name)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetOrdinal",
                () => this._targetDataReader.GetOrdinal(name));
        }

        public override string GetString(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetString",
                () => this._targetDataReader.GetString(i));
        }

        public override object GetValue(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetValue",
                () => this._targetDataReader.GetValue(i));
        }

        public override int GetValues(object[] values)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetValues",
                () => this._targetDataReader.GetValues(values));
        }

        public override bool IsDBNull(int i)
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.IsDBNull",
                () => this._targetDataReader.IsDBNull(i));
        }

        public override IEnumerator GetEnumerator()
        {
            return WrapperUtils.ExecuteWithPlugins(
                this._connectionPluginManager,
                this._targetDataReader,
                "DbDataReader.GetEnumerator",
                () => this._targetDataReader.GetEnumerator());
        }
    }
}
