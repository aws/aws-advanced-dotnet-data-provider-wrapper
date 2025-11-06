# AWS Advanced .NET Data Provider Wrapper - MySqlConnector Dialect

## Overview

This project provides a database dialect implementation for MySqlConnector, enabling the AWS Advanced .NET Data Provider Wrapper to work with MySqlConnector-based applications and ORMs.

## Dependencies

This project depends on:
- **MySqlConnector**: High-performance MySQL connector for .NET

## Usage

This dialect is automatically loaded when using MySqlConnector with the AWS Wrapper. No direct usage is required - it's used internally by the wrapper when MySqlConnector is detected.

The dialect enables support for:
- Aurora MySQL clusters
- RDS MySQL instances
- Self-managed MySQL databases

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
