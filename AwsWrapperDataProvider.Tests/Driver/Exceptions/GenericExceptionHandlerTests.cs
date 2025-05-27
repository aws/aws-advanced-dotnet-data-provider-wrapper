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
using System.Data.Common;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using Moq;
using Xunit;

namespace AwsWrapperDataProvider.Tests.Driver.Exceptions
{
    public class GenericExceptionHandlerTests
    {
        private readonly string[] _networkStates = { "08001", "08S01" };
        private readonly string[] _loginStates = { "28000", "28P01" };

        private readonly IExceptionHandler _handler;

        public GenericExceptionHandlerTests()
        {
            this._handler = new TestGenericExceptionHandler(this._networkStates, this._loginStates);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("08001", true)]
        [InlineData("08S01", true)]
        [InlineData("28000", false)]
        [InlineData("42000", false)]
        public void IsNetworkException_WithSqlState_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var result = this._handler.IsNetworkException(sqlState);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("28000", true)]
        [InlineData("28P01", true)]
        [InlineData("08001", false)]
        [InlineData("42000", false)]
        public void IsLoginException_WithSqlState_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var result = this._handler.IsLoginException(sqlState);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("08001", true)]
        [InlineData("28000", false)]
        public void IsNetworkException_WithDbException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var exception = new TestDbException(sqlState);

            var result = this._handler.IsNetworkException(exception, null);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("28000", true)]
        [InlineData("08001", false)]
        public void IsLoginException_WithDbException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var exception = new TestDbException(sqlState);

            var result = this._handler.IsLoginException(exception, null);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("08001", true)]
        [InlineData("28000", false)]
        public void IsNetworkException_WithNestedDbException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var innerException = new TestDbException(sqlState);
            var exception = new Exception("Outer exception", innerException);

            var result = this._handler.IsNetworkException(exception, null);
            Assert.Equal(expected, result);
        }

        [Theory]
        [Trait("Category", "Unit")]
        [InlineData("28000", true)]
        [InlineData("08001", false)]
        public void IsLoginException_WithNestedDbException_ReturnsExpectedResult(string sqlState, bool expected)
        {
            var innerException = new TestDbException(sqlState);
            var exception = new Exception("Outer exception", innerException);

            var result = this._handler.IsLoginException(exception, null);
            Assert.Equal(expected, result);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void IsNetworkException_WithNonDbException_ReturnsFalse()
        {
            var exception = new Exception("Not a DB exception");

            var result = this._handler.IsNetworkException(exception, null);
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void IsLoginException_WithNonDbException_ReturnsFalse()
        {
            var exception = new Exception("Not a DB exception");

            var result = this._handler.IsLoginException(exception, null);
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

        private class TestGenericExceptionHandler : GenericExceptionHandler
        {
            public TestGenericExceptionHandler(string[] networkErrorStates, string[] loginErrorStates)
            {
                this._networkErrorStatesValue = networkErrorStates;
                this._loginErrorStatesValue = loginErrorStates;
            }

            private readonly string[] _networkErrorStatesValue;
            private readonly string[] _loginErrorStatesValue;

            protected override string[] NetworkErrorStates => this._networkErrorStatesValue;
            protected override string[] LoginErrorStates => this._loginErrorStatesValue;
        }
    }
}
