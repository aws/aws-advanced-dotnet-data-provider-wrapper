# AWS Advanced .NET Data Provider Wrapper - Npgsql Dialect

## Overview

This project provides a database dialect implementation for Npgsql, enabling the AWS Advanced .NET Data Provider Wrapper to work with PostgreSQL applications and ORMs using Npgsql.

## Dependencies

This project depends on:
- **Npgsql**: High-performance PostgreSQL driver for .NET

## Usage

This dialect is automatically loaded when using Npgsql with the AWS Wrapper. No direct usage is required - it's used internally by the wrapper when Npgsql is detected.

The dialect enables support for:
- Aurora PostgreSQL clusters
- RDS PostgreSQL instances
- Self-managed PostgreSQL databases

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
