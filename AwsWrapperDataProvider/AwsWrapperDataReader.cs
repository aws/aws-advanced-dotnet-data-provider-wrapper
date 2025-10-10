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

namespace AwsWrapperDataProvider;

public class AwsWrapperDataReader : DbDataReader
{
    private readonly ConnectionPluginManager connectionPluginManager;
    protected DbDataReader targetDataReader;

    internal AwsWrapperDataReader(DbDataReader targetDataReader, ConnectionPluginManager connectionPluginManager)
    {
        this.connectionPluginManager = connectionPluginManager;
        this.targetDataReader = targetDataReader;
    }

    public override int Depth => this.targetDataReader.Depth;

    public override bool IsClosed => this.targetDataReader.IsClosed;

    public override int RecordsAffected => this.targetDataReader.RecordsAffected;

    public override int FieldCount => this.targetDataReader.FieldCount;

    public override bool HasRows => this.targetDataReader.HasRows;

    public override object this[int i] => this.targetDataReader[i];

    public override object this[string name] => this.targetDataReader[name];

    public override void Close()
    {
        WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.Close",
            () =>
            {
                this.targetDataReader.Close();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
    }

    public override async Task CloseAsync()
    {
        await WrapperUtils.RunWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.CloseAsync",
            () => this.targetDataReader.CloseAsync());
    }

    public override DataTable? GetSchemaTable() => this.targetDataReader.GetSchemaTable();

    public override bool NextResult()
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.NextResult",
            () => Task.FromResult(this.targetDataReader.NextResult()))
            .GetAwaiter().GetResult();
    }

    public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        return await WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.NextResultAsync",
            () => this.targetDataReader.NextResultAsync(cancellationToken));
    }

    public override bool Read()
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.Read",
            () => Task.FromResult(this.targetDataReader.Read()))
            .GetAwaiter().GetResult();
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        return await WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.ReadAsync",
            () => this.targetDataReader.ReadAsync(cancellationToken));
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            this.targetDataReader?.Dispose();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (this.targetDataReader is not null)
        {
            await this.targetDataReader.DisposeAsync();
        }

    }

    public override bool GetBoolean(int i) => this.targetDataReader.GetBoolean(i);

    public override byte GetByte(int i) => this.targetDataReader.GetByte(i);

    public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    => this.targetDataReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

    public override char GetChar(int i) => this.targetDataReader.GetChar(i);

    public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    => this.targetDataReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);

    public override string GetDataTypeName(int i) => this.targetDataReader.GetDataTypeName(i);

    public override DateTime GetDateTime(int i) => this.targetDataReader.GetDateTime(i);

    public override decimal GetDecimal(int i) => this.targetDataReader.GetDecimal(i);

    public override double GetDouble(int i) => this.targetDataReader.GetDouble(i);

    // TODO: Write integration test to check if user can use reflection on Type after trimming.
    [return:
        DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields |
                                   DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int i) => this.targetDataReader.GetFieldType(i);

    public override float GetFloat(int i) => this.targetDataReader.GetFloat(i);

    public override Guid GetGuid(int i) => this.targetDataReader.GetGuid(i);

    public override short GetInt16(int i) => this.targetDataReader.GetInt16(i);

    public override int GetInt32(int i) => this.targetDataReader.GetInt32(i);

    public override long GetInt64(int i) => this.targetDataReader.GetInt64(i);

    public override string GetName(int i) => this.targetDataReader.GetName(i);

    public override int GetOrdinal(string name) => this.targetDataReader.GetOrdinal(name);

    public override string GetString(int i) => this.targetDataReader.GetString(i);

    public override object GetValue(int i) => this.targetDataReader.GetValue(i);

    public override int GetValues(object[] values) => this.targetDataReader.GetValues(values);

    public override bool IsDBNull(int i)
    {
        return WrapperUtils.ExecuteWithPlugins(
            this.connectionPluginManager,
            this.targetDataReader,
            "DbDataReader.IsDBNull",
            () => Task.FromResult(this.targetDataReader.IsDBNull(i)))
            .GetAwaiter().GetResult();
    }

    public override IEnumerator GetEnumerator() => this.targetDataReader.GetEnumerator();
}
