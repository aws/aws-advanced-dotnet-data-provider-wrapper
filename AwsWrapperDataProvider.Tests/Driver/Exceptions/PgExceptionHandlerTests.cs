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
using AwsWrapperDataProvider.Driver.Exceptions;
using Npgsql;
using Xunit;

namespace AwsWrapperDataProvider.Tests.Driver.Exceptions
{
    public class PgExceptionHandlerTests
    {
        private readonly PgExceptionHandler _handler;

        public PgExceptionHandlerTests()
        {
            this._handler = new PgExceptionHandler();
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("53", true)]
        [InlineData("57P01", true)]
        [InlineData("57P02", true)]
        [InlineData("57P03", true)]
        [InlineData("58", true)]
        [InlineData("08", true)]
        [InlineData("99", true)]
        [InlineData("F0", true)]
        [InlineData("XX", true)]
        [InlineData("28000", false)]
        [InlineData("42000", false)]
        [InlineData("23505", false)]
        public void IsNetworkException_WithSqlState_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var result = this._handler.IsNetworkException(sqlState);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("28000", true)]
        [InlineData("28P01", true)]
        [InlineData("08", false)]
        [InlineData("42000", false)]
        [InlineData("23505", false)]
        public void IsLoginException_WithSqlState_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var result = this._handler.IsLoginException(sqlState);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("08", true)]
        [InlineData("28000", false)]
        public void IsNetworkException_WithPostgresException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var exception = new PostgresException("error", "sev", "invariant sev", sqlState);
            var result = this._handler.IsNetworkException(exception);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("28000", true)]
        [InlineData("28P01", true)]
        [InlineData("08", false)]
        public void IsLoginException_WithPostgresException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var exception = new PostgresException("error", "sev", "invariant sev", sqlState);
            var result = this._handler.IsLoginException(exception);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("08", true)]
        [InlineData("28000", false)]
        public void IsNetworkException_WithNestedPostgresException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var innerException = new PostgresException("error", "sev", "invariant sev", sqlState);
            var exception = new Exception("Outer exception", innerException);

            var result = this._handler.IsNetworkException(exception);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("28000", true)]
        [InlineData("08", false)]
        public void IsLoginException_WithNestedPostgresException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var innerException = new PostgresException("error", "sev", "invariant sev", sqlState);
            var exception = new Exception("Outer exception", innerException);

            var result = this._handler.IsLoginException(exception);
            Assert.Equal(expected, result);
        }
    }
}
