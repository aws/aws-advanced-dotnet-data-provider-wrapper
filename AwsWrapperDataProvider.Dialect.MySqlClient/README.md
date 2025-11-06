# AWS Advanced .NET Data Provider Wrapper - MySql.Data Dialect

## Overview

This project provides a database dialect implementation for MySql.Data (MySQL Connector/NET), enabling the AWS Advanced .NET Data Provider Wrapper to work with MySql.Data-based applications and ORMs.

## Dependencies

This project depends on:
- **MySql.Data**: Official MySQL Connector/NET from Oracle

## Usage

This dialect is automatically loaded when using MySql.Data with the AWS Wrapper. No direct usage is required - it's used internally by the wrapper when MySql.Data is detected.

The dialect enables support for:
- Aurora MySQL clusters
- RDS MySQL instances
- Self-managed MySQL databases

## Documentation

For comprehensive information about the AWS Advanced .NET Data Provider Wrapper, visit the [main documentation](../docs/).
