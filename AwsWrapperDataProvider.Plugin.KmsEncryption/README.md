# AWS Advanced .NET Data Provider Wrapper - KMS Encryption Plugin

## Overview

This plugin provides transparent client-side column encryption for the AWS Advanced .NET Data Provider Wrapper using AWS Key Management Service (KMS). Values written to a configured column are encrypted before they leave the application, and decrypted after they are read back, so the database only ever stores ciphertext and no application code changes are required.

Encryption uses envelope encryption: a KMS master key protects per-column data keys, and those data keys perform the AES-GCM encryption locally. Data keys are cached in memory so the common path does not call KMS.

## Dependencies

This project depends on:
- **[AWSSDK.KeyManagementService](https://www.nuget.org/packages/AWSSDK.KeyManagementService/)**: AWS SDK for KMS to generate and decrypt data keys

## Usage

Register the KMS Encryption plugin before using it:

```csharp
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;

// Register the KMS Encryption plugin
ConnectionPluginChainBuilder.RegisterPluginFactory<KmsEncryptionPluginFactory>(PluginCodes.KmsEncryption);

// Use in connection string
var connectionString = "Server=your-rds-instance.amazonaws.com;" +
                       "Database=mydb;" +
                       "KmsRegion=us-east-1;" +
                       "Plugins=kmsEncryption;";
```

This plugin also requires encryption metadata tables in the database, which record which columns are encrypted and with which key and algorithm. See the documentation below for the required schema and for how to register a column for encryption.

## Documentation

For comprehensive information about client-side encryption and the AWS Advanced .NET Data Provider Wrapper, visit the [Using the KMS Encryption Plugin](../docs/using-the-dotnet-driver/using-plugins/UsingTheKmsEncryptionPlugin.md) guide.
