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

using System.Data.Common;
using AwsWrapperDataProvider.Driver.Exceptions;

namespace AwsWrapperDataProvider.Tests.Driver.Exceptions
{
    public class MySqlExceptionHandlerTests
    {
        private readonly MySqlExceptionHandler _handler = new();

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("25006", true)] // read-only SQL transaction
        [InlineData("08001", false)]
        [InlineData("28000", false)]
        [InlineData("42000", false)]
        public void IsReadOnlyConnectionException_WithSqlState_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var exception = new TestDbException(sqlState);

            var result = this._handler.IsReadOnlyConnectionException(exception);

            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("25006", true)]
        [InlineData("08001", false)]
        public void IsReadOnlyConnectionException_WithNestedDbException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var innerException = new TestDbException(sqlState);
            var exception = new Exception("Outer exception", innerException);

            var result = this._handler.IsReadOnlyConnectionException(exception);

            Assert.Equal(expected, result);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void IsReadOnlyConnectionException_WithNonDbException_ReturnsFalse()
        {
            var result = this._handler.IsReadOnlyConnectionException(new Exception("Not a DB exception"));

            Assert.False(result);
        }

        private class TestDbException : DbException
        {
            private readonly string _sqlState;

            public TestDbException(string sqlState)
            {
                this._sqlState = sqlState;
            }

            public override string? SqlState => this._sqlState;
        }
    }
}
