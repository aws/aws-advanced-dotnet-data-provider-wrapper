# Using the KMS Encryption Plugin

## Plugin Availability
The plugin is available since version 3.0.0.

The KMS Encryption Plugin encrypts configured columns inside your application, so the database only ever
stores ciphertext. Values are encrypted before they are sent and decrypted after they are read, with no
change to your application code.

Encryption uses envelope encryption: a master key held in AWS Key Management Service protects per-column
data keys, and those data keys perform the encryption locally. Data keys are cached in memory, so the
common path does not call AWS Key Management Service.

> [!IMPORTANT]\
> **The plugin does not refuse writes it cannot encrypt.** Some statements bind a value in a way the plugin
> cannot intercept - a literal in the SQL text, a value the server computes, an unnamed placeholder, a
> stored procedure, a `COPY`. In those cases the plugin logs a warning at `Warning` level and runs the
> statement anyway, so the readable value is stored. Enable `Warning` logging, and add the constraint in
> [Preventing unencrypted writes](#preventing-unencrypted-writes) - that constraint, not the driver, is
> what guarantees the column only ever holds ciphertext.

## Features

- **Transparent encryption**: values are encrypted and decrypted with no change to your application code.
- **AWS KMS integration**: a master key in AWS Key Management Service protects the data keys; the plaintext
  of a value never leaves your application.
- **Metadata-driven**: which columns are encrypted is configured in the database, so it changes without a
  redeploy.
- **Caching**: metadata and data keys are cached, so the common path makes no AWS Key Management Service call.
- **Both engines**: PostgreSQL and MySQL.

## Prerequisites

- An AWS KMS symmetric key, with `kms:GenerateDataKey` and `kms:Decrypt` granted to the application.
- The `AWS.AdvancedDotnetDataProviderWrapper.Plugin.KmsEncryption` package. It brings in
  [AWSSDK.KeyManagementService](https://www.nuget.org/packages/AWSSDK.KeyManagementService/) and
  [SqlParserCS](https://www.nuget.org/packages/SqlParserCS/), so nothing else needs adding to your project.
- The metadata tables described in [Metadata schema](#metadata-schema).
- AWS credentials the wrapper can resolve, from an IAM role, a profile, or the environment.
- Each column to be encrypted converted to a binary type - see
  [Preparing a column for encryption](#preparing-a-column-for-encryption).
- A server-side constraint on each encrypted column - see
  [Preventing unencrypted writes](#preventing-unencrypted-writes).

### Creating the master key

```bash
aws kms create-key --description "Database encryption master key" --key-usage ENCRYPT_DECRYPT
```

Note the key ARN from the response. It is recorded in `key_storage.master_key_arn` when a column is
registered.

### Data key management

Data keys are handled for you:

- A data key is generated once per column, with the master key, and stored in `key_storage` in its encrypted
  form.
- The plaintext data key exists only in memory, obtained by asking AWS Key Management Service to decrypt the
  stored copy.
- Decrypted data keys are cached, so a statement does not normally call AWS Key Management Service.
- No manual data key creation is required.

## Configuration

### Connection Properties

| Parameter | Value | Required | Description | Example | Default Value |
|---|:---:|:---:|---|---|---|
| `KmsRegion` | String | Yes | The region holding the master key. | `us-east-1` | `null` |
| `KmsEncryptionMetadataSchema` | String | No | The schema containing the metadata tables. | `encrypt` | `encrypt` |
| `KmsMetadataCacheEnabled` | Boolean | No | Whether encryption metadata is cached. Disabling it makes every statement re-read the metadata. | `false` | `true` |
| `KmsMetadataCacheExpirationMinutes` | Integer | No | How long a cached copy of the metadata is used before it is read again. | `30` | `60` |
| `KmsDataKeyCacheEnabled` | Boolean | No | Whether decrypted data keys are cached. Disabling it makes an AWS KMS `Decrypt` call for **every** statement that touches an encrypted column. | `false` | `true` |
| `KmsDataKeyCacheMaxSize` | Integer | No | Maximum number of data keys held in memory. | `100` | `1000` |
| `KmsDataKeyCacheExpirationMs` | Integer | No | How long a decrypted data key is kept in memory. | `600000` | `3600000` |

> [!NOTE]\
> Leave both caches enabled in production. Turning either off shortens how long key material stays in
> memory, but the cost is a KMS call per statement, which adds latency and can reach KMS request-rate
> limits. Treat it as a deliberate throughput-versus-key-exposure tradeoff.

### Example Connection String

```csharp
using System.Data.Common;
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using Npgsql;

ConnectionPluginChainBuilder.RegisterPluginFactory<KmsEncryptionPluginFactory>(
    PluginCodes.KmsEncryption);

var connectionString =
    "Host=your-cluster.cluster-xyz.us-east-1.rds.amazonaws.com;Port=5432;Database=mydb;" +
    "Username=username;Password=password;" +
    "Plugins=kmsEncryption;KmsRegion=us-east-1";

await using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);
await connection.OpenAsync();
```

## Usage

Once a column is registered, nothing in your code changes. Bind the value as a parameter, and it is encrypted
when the statement runs - the plugin reads the statement to work out which parameter belongs to the encrypted
column, then sends a substitute in its place. This is why a value has to be a parameter: a literal in the SQL
text has already been written by the time the plugin sees the statement.

```csharp
await using DbCommand insert = connection.CreateCommand();
insert.CommandText = "INSERT INTO users (name, ssn) VALUES (@name, @ssn)";

DbParameter name = insert.CreateParameter();
name.ParameterName = "@name";
name.Value = "Jane Doe";
insert.Parameters.Add(name);

DbParameter ssn = insert.CreateParameter();
ssn.ParameterName = "@ssn";
ssn.Value = "123-45-6789";
insert.Parameters.Add(ssn);

// @ssn is encrypted here, as the statement runs. Your own parameter is left untouched.
await insert.ExecuteNonQueryAsync();
```

Reading it back returns the original value, as the original type:

```csharp
await using DbCommand select = connection.CreateCommand();
select.CommandText = "SELECT name, ssn FROM users WHERE name = @name";

DbParameter lookup = select.CreateParameter();
lookup.ParameterName = "@name";
lookup.Value = "Jane Doe";
select.Parameters.Add(lookup);

await using DbDataReader reader = await select.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    string ssn = reader.GetString(1);   // "123-45-6789", decrypted on the way in
}
```

> [!IMPORTANT]\
> Look rows up by a column that is **not** encrypted, as above. A `WHERE` clause on an encrypted column
> never matches - see [What the column can no longer do](#what-the-column-can-no-longer-do).

## Keys and where they live

| Key | Purpose | Stored |
|---|---|---|
| Master key | Protects the data key. Never encrypts your data directly. | Inside AWS Key Management Service; never leaves it |
| Data key | Encrypts your values | `key_storage.encrypted_data_key`, sealed by the master key. Plaintext form exists only in memory. |
| HMAC key | Signs each encrypted value so tampering is detectable | `key_storage.hmac_key`, **unencrypted** |

> [!IMPORTANT]\
> The HMAC key is stored without protection, so anyone who can read `key_storage` can forge the signature
> on an encrypted value. The confidentiality of your data does not depend on it: the data key is sealed by
> the master key, and without that key the ciphertext cannot be read. Restrict access to the key storage
> table accordingly.

## Metadata schema

Create these tables before registering any column for encryption. The schema name is configurable with
`KmsEncryptionMetadataSchema` and defaults to `encrypt`.

```sql
CREATE TABLE key_storage (
    id                  SERIAL PRIMARY KEY,
    key_id              VARCHAR(255) UNIQUE NOT NULL,
    name                VARCHAR(255) NOT NULL,
    master_key_arn      VARCHAR(512) NOT NULL,
    encrypted_data_key  TEXT NOT NULL,
    hmac_key            BYTEA NOT NULL,
    key_spec            VARCHAR(50) DEFAULT 'AES_256',
    created_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encryption_metadata (
    table_name           VARCHAR(255) NOT NULL,
    column_name          VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id               INTEGER NOT NULL,
    created_at           TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES key_storage(id)
);
```

Each column has its own entry and therefore its own key. A value encrypted for one column cannot be read
as another.

## Preparing a column for encryption

An encrypted value is a sequence of bytes, so the column must be a binary type. Enabling encryption on an
existing column is a schema migration, and it must be done before any value is written.

### Column type

| Server | Type to use | If the column is still text |
|---|---|---|
| PostgreSQL | `encrypt.encrypted_data` (see below) or `bytea` | Fails: `column "ssn" is of type character varying but expression is of type bytea` |
| MySQL | `VARBINARY(n)` or `BLOB` | Usually fails with `Incorrect string value`, because ciphertext is rarely valid UTF-8 |

> [!WARNING]\
> PostgreSQL rejects the write outright. MySQL may not: a `VARCHAR` column with a permissive character set
> such as `latin1` accepts arbitrary bytes and mangles them on the way back out, with no error. Always
> convert the column before enabling encryption.

On PostgreSQL, prefer a domain over bare `bytea`. It documents the column's purpose in the schema and
enforces the minimum length in the database:

```sql
CREATE DOMAIN encrypt.encrypted_data AS bytea
  CHECK (length(VALUE) >= 61);
```

61 is the shortest an encrypted value can be — 32 signature, 1 type label, 12 single-use number, and a
16-byte cipher tag that is always present, even for an empty value.

Converting existing columns:

```sql
-- PostgreSQL
ALTER TABLE users ALTER COLUMN ssn TYPE encrypt.encrypted_data USING ssn::bytea;

-- MySQL
ALTER TABLE users MODIFY ssn VARBINARY(256);
```

### Supported value types

Every type ADO.NET has a `DbDataReader` accessor for can be encrypted, plus the three date and time types
that have no accessor of their own.

| Written as | Read it with | `GetValue` returns |
|---|---|---|
| `string` | `GetString` | `string` |
| `int` | `GetInt32` | `int` |
| `long` | `GetInt64` | `long` |
| `float` | `GetFloat` | `float` |
| `double` | `GetDouble` | `double` |
| `decimal` | `GetDecimal` | `decimal` |
| `bool` | `GetBoolean` | `bool` |
| `byte[]` | `GetBytes`, `GetStream` | `byte[]` |
| `DateTime` | `GetDateTime` | `DateTime` |
| `DateTimeOffset` | `GetFieldValue<DateTimeOffset>` | `DateTimeOffset` |
| `DateOnly` | `GetFieldValue<DateOnly>` | `DateOnly` |
| `TimeOnly` | `GetFieldValue<TimeOnly>` | `TimeOnly` |
| `short` | `GetInt16` | **`int`** |
| `byte` | `GetByte` | **`int`** |
| `char` | `GetChar` | **`string`** |
| `Guid` | `GetGuid` | **`string`** |

The last four are stored under a wider type's marker rather than one of their own, because the marker is part
of the stored format and a value invented here could not be read by the other AWS wrappers. They round-trip
correctly through the accessor in the middle column, which converts; only `GetValue` and `GetFieldValue<T>`
for the original type show the widening. So read a `short` with `GetInt16`, not `(short)reader.GetValue(0)`.

Reading with an accessor converts wherever there is one unambiguous answer, so `GetString` works on any
stored type and `GetDateTime` works on a `DateTimeOffset` (returned as UTC) or a `DateOnly` (returned as
midnight). The one case with no answer is a `TimeOnly` read with `GetDateTime`: it carries no date, so it
raises an error naming the column rather than inventing one.

**Anything else is refused** at the point the value is written, with an error naming the type. That covers
`TimeSpan`, `sbyte`, `uint`, `ulong`, enums and arbitrary objects. Convert before binding — a `TimeSpan` as
its `Ticks`, an enum as its underlying `int` — and convert back after reading.

> [!NOTE]\
> This set is narrower than the column itself accepts. The column is binary (`bytea` or `VARBINARY`), so the
> database will hold anything; the restriction belongs to the plugin and applies to the value you bind.

A column may also hold values written by the AWS Advanced JDBC Wrapper's `setDate` or `setTime`. Those read
back as `DateOnly` and `TimeOnly`. A `Guid` written here is stored as its canonical 36-character text, which
is what Java's `UUID.toString` produces, so the other wrappers read it as the same string.

### Sizing

Stored size is **61 bytes plus the length of the value**. An 11-character national identifier occupies 72
bytes. `bytea` is unbounded, so PostgreSQL needs no sizing; MySQL does.

> [!WARNING]\
> Keep MySQL's `STRICT_TRANS_TABLES` enabled. With strict mode on, a `VARBINARY` that is too small raises
> an error. With it off, MySQL truncates silently and the stored value is destroyed.

### What the column can no longer do

Once a column holds ciphertext the server can no longer reason about its contents:

- **Equality lookups match nothing.** Every value is encrypted with a fresh single-use number, so the same
  input never produces the same bytes. `WHERE ssn = @ssn` returns no rows however the value is supplied.
  The plugin logs a warning when it sees such a comparison, but it does not stop the statement. Look rows
  up by a column that is not encrypted.
- **`LIKE`, ranges, and `ORDER BY` are meaningless** — they operate on ciphertext.
- **An index only indexes ciphertext**, so it cannot serve lookups on the value.
- **A unique constraint no longer prevents duplicates**, because two identical values produce different
  bytes. Check for unique indexes before encrypting a column. An upsert whose conflict target is the
  encrypted column is affected by the same thing: `ON CONFLICT (ssn)` and a unique index on `ssn` never
  detect the conflict, so the statement inserts another row instead of updating the existing one. Key the
  conflict on a column that is not encrypted.
- **`CHECK` constraints on the content stop working**, and collation no longer applies.

### Existing rows

`ALTER TABLE` converts the column type, not the stored data. Rows written before encryption was enabled
hold readable bytes in a binary column, and reading them raises an error naming the column. Migrate them
by reading each value with the plugin disabled and writing it back with the plugin enabled.

## Preventing unencrypted writes

> [!IMPORTANT]\
> **Add the server-side constraint described in this section.** It is not optional hardening. The plugin
> does not refuse a write it cannot encrypt - it logs a warning and lets the statement run - so the
> constraint on the column is the only thing that guarantees the column holds nothing but ciphertext.

The plugin encrypts values that arrive as **parameters on a command it can match to a column**. These
writes reach the column unencrypted, and the plugin logs a warning at `Warning` level for the ones it can
see:

| Write | Encrypted? | Plugin logs a warning? |
|---|---|---|
| Parameter on a command with the plugin enabled | Yes | – |
| A literal in the statement, such as `VALUES ('123-45-6789')` | **No** | Yes |
| A value the server computes, such as `VALUES (upper(@ssn))` | **No** | Yes |
| An unnamed placeholder, `VALUES (?)` | **No** | Yes |
| A statement whose columns cannot be matched to its parameters, such as `INSERT INTO users VALUES (@a, @b)` | **No** | Yes |
| A statement the plugin cannot read, such as one using a PostgreSQL `E''` escape string | **No** | Yes |
| A `MERGE`, which is not analysed | **No** | Yes |
| A statement kind that carries values some other way — `CALL`, `EXECUTE … USING`, `COPY … FROM STDIN`, `PREPARE`, or any `CommandType.StoredProcedure` | **No** | Yes |
| Parameter on a `DbBatch` command with the plugin enabled | Yes | – |
| Any application connecting without the plugin | **No** | No |
| A database client, migration tool, or data-fix script | **No** | No |
| Rows that already existed when the column was registered | **No** | No |

Reads eventually detect these: an encrypted value is never shorter than 61 bytes, and its signature is
verified before it is decrypted, so unencrypted data raises an error naming the column. But that happens
whenever the row is next read, which may be long after it was written, and by then the readable value has
been sitting in the database.

A `BEFORE INSERT`/`BEFORE UPDATE` trigger closes the gap by rejecting the write at the source, turning a
warning in your application log into a failed statement the caller cannot ignore.

> [!NOTE]\
> A trigger prevents mistakes; it is not a security boundary. Anyone able to drop the trigger can drop it,
> and anyone able to read `key_storage` holds the signing key. Its value is in catching migrations,
> scripts, and applications that were not configured with the plugin - and in enforcing what the plugin
> only warns about.

### PostgreSQL

With `pgcrypto`, the trigger can verify the signature itself, because it can read the same HMAC key the
plugin uses. `pgcrypto` is available on Aurora PostgreSQL and RDS for PostgreSQL.

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION encrypt.require_encrypted_users_ssn()
RETURNS trigger AS $$
DECLARE
  v        bytea := NEW.ssn;
  hkey     bytea;
  expected bytea;
BEGIN
  IF v IS NULL THEN
    RETURN NEW;                       -- a SQL NULL is left as NULL and is not encrypted
  END IF;

  -- 32 signature + 1 type label + 12 single-use number + 16 cipher tag
  IF length(v) < 61 THEN
    RAISE EXCEPTION 'users.ssn was written unencrypted (% bytes)', length(v);
  END IF;

  IF get_byte(v, 32) NOT IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14) THEN
    RAISE EXCEPTION 'users.ssn has an unrecognised type label';
  END IF;

  SELECT ks.hmac_key INTO hkey
    FROM encrypt.encryption_metadata em
    JOIN encrypt.key_storage ks ON ks.id = em.key_id
   WHERE em.table_name = 'users' AND em.column_name = 'ssn';

  -- The signature covers everything from the type label onwards.
  expected := hmac(substring(v FROM 33), hkey, 'sha256');
  IF substring(v FOR 32) <> expected THEN
    RAISE EXCEPTION 'users.ssn failed its integrity check on write';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_ssn_encrypted
  BEFORE INSERT OR UPDATE OF ssn ON users
  FOR EACH ROW EXECUTE FUNCTION encrypt.require_encrypted_users_ssn();
```

For bulk loads, look the key up once rather than joining per row: read `hmac_key` when the trigger is
created and inline it as a constant, or wrap the lookup in a `STABLE` function.

### MySQL

MySQL has no built-in `HMAC()` function, but the signature can still be verified: HMAC-SHA256 can be built
from `SHA2()` by combining the key with the two padding constants a byte at a time, using `SUBSTRING` and
`ASCII` to read each byte, `^` to combine it, and `CHAR` to reassemble. A stored function of that shape
makes full verification possible without any extension or user-defined function.

Writing that function by hand is not advisable — a subtly wrong implementation would reject valid data or
accept invalid data. It should be generated, so that it and the metadata cannot drift apart.

The length and type-label checks below need no helper function and catch the common accident, a
human-readable value. MySQL requires a separate trigger per event.

```sql
CREATE TRIGGER users_ssn_encrypted_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
  IF NEW.ssn IS NOT NULL THEN
    IF LENGTH(NEW.ssn) < 61 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'users.ssn was written unencrypted';
    END IF;
    IF ORD(SUBSTRING(NEW.ssn, 33, 1)) NOT IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14) THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'users.ssn has an unrecognised type label';
    END IF;
  END IF;
END;

CREATE TRIGGER users_ssn_encrypted_update
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
  IF NEW.ssn IS NOT NULL THEN
    IF LENGTH(NEW.ssn) < 61 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'users.ssn was written unencrypted';
    END IF;
  END IF;
END;
```

Note the cost: the signature-verifying version loops 64 times per row, so on bulk loads measure it before
adopting it. The length check alone is nearly free.

> [!WARNING]\
> These triggers encode the stored byte layout. That layout is shared with the AWS Advanced JDBC Wrapper, so
> it will not change - but if it ever did, the triggers would need updating alongside it.

## Security Considerations

### KMS key permissions

The application needs only these two actions on the master key:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kms:GenerateDataKey",
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
        }
    ]
}
```

`kms:GenerateDataKey` is needed only to register a column. An application that only reads and writes already
registered columns needs `kms:Decrypt` alone.

### Data protection

- Values are encrypted and decrypted inside your application. The plaintext never reaches the server, and
  neither does any data key in usable form.
- Only key material is managed by AWS Key Management Service, never the data.
- Anyone who can **write** `key_storage` can defeat the protection - substituting a known HMAC key lets them
  forge values that pass a server-side trigger, and repointing a column at a key they control lets them read
  or replace its data. Restrict write access to the metadata tables.
- Use different master keys for different environments.

### Performance

- AWS Key Management Service is called to decrypt a data key, not per value. With the data key cache on, a
  steady-state application makes no KMS calls at all.
- Encryption itself is AES-256-GCM in your process, so the cost is proportional to the value's size.
- Each encrypted value costs 61 bytes of storage on top of the value - see [Sizing](#sizing).

## Troubleshooting

| Symptom | Cause |
|---|---|
| `The value stored in <column> is not encrypted: it is N bytes` | The row was written without the plugin, as a literal, or by another tool. Migrate it, and add the [server-side constraint](#preventing-unencrypted-writes). |
| `The value stored in <column> failed its integrity check` | A different key is configured for the column than the one it was written with, or the stored bytes were modified. |
| A query on an encrypted column returns no rows | Expected. Encryption is randomized, so an equality comparison can never match - see [What the column can no longer do](#what-the-column-can-no-longer-do). |
| `AWS KMS could not decrypt the data key` | The application lacks `kms:Decrypt` on the master key, the credentials have expired, or `KmsRegion` does not match the key's region. |
| Values are stored readable, with a warning in the log | The statement bound the value in a way the plugin cannot intercept. The warning names the reason - see [Preventing unencrypted writes](#preventing-unencrypted-writes). |
| Values are stored readable, with nothing in the log | The application connected without the plugin enabled, the plugin was not in `Plugins`, or the write came from a tool, a migration, or another application. The plugin warns about every write it sees and cannot encrypt, so silence means it did not see the write. |

Enable `Warning` level logging for `AwsWrapperDataProvider.Plugin.KmsEncryption` to see every write the
plugin could not encrypt. Nothing the plugin logs contains a column value.

## Limitations

- One master key and one data key per column. A value encrypted for one column cannot be read as another.
- Only bind parameters are encrypted, and only on an `INSERT`, `UPDATE` or `DELETE` the plugin can match to
  a column. Literals, server-computed expressions, unnamed placeholders, `MERGE`, `CALL`,
  `EXECUTE … USING`, `COPY … FROM STDIN` and stored procedures are not — each is warned about but still
  executed. [Preventing unencrypted writes](#preventing-unencrypted-writes) lists every shape.
- `TimeSpan`, `sbyte`, `uint`, `ulong` and enums cannot be encrypted. Every type with a `DbDataReader`
  accessor can be. See [Supported value types](#supported-value-types).
- An encrypted column cannot be searched, sorted, indexed by value, or covered by a unique constraint.
- Rows written before a column was registered are not readable through the plugin until they are migrated.
- Encrypted columns cannot be configured through `AwsWrapperDataSource`; use a connection string.

## Best Practices

1. **Install the server-side constraint** on every encrypted column. It, not the plugin, is what guarantees
   the column holds nothing but ciphertext.
2. **Enable the plugin in every application** that writes an encrypted column.
3. **Register a column before inserting data into it**, or migrate the rows that are already there.
4. **Always bind values as parameters**, never as literals in the SQL text.
5. **Look rows up by a column that is not encrypted.**
6. **Restrict access to the metadata tables**, particularly write access.
7. **Use separate master keys per environment**, and enable automatic key rotation on them.
8. **Back up the metadata tables** with the data; without `key_storage` the ciphertext cannot be read.
9. **Remember that administrative tools bypass the plugin entirely.** The triggers are what catch that.

## Example Application

See [PGKmsEncryption.cs](../../examples/AwsWrapperDataProviderExample/PGKmsEncryption.cs) for a runnable
example that registers the plugin factory, writes an encrypted value as a bind parameter, and reads it
back. It expects the metadata schema and column registration described in
[Metadata schema](#metadata-schema) and [Preparing a column for encryption](#preparing-a-column-for-encryption)
to be in place already.
