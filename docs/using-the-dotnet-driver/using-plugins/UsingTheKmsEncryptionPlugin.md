# Using the KMS Encryption Plugin

> [!NOTE]\
> This plugin is under active development and is not yet available for use. The metadata schema and the
> write-protection guidance below are settled; the configuration and usage sections will follow.

The KMS Encryption Plugin encrypts configured columns inside your application, so the database only ever
stores ciphertext. Values are encrypted before they are sent and decrypted after they are read, with no
change to your application code.

Encryption uses envelope encryption: a master key held in AWS Key Management Service protects per-column
data keys, and those data keys perform the encryption locally. Data keys are cached in memory, so the
common path does not call AWS Key Management Service.

> [!IMPORTANT]\
> **The plugin does not refuse writes it cannot encrypt.** Some statements bind a value in a way the plugin
> cannot intercept - a literal in the SQL text, a value the server computes, an unnamed placeholder, a
> `DbBatch`. In those cases the plugin logs a warning at `Warning` level and runs the statement anyway, so
> the readable value is stored. Enable `Warning` logging, and add the server-side constraint in
> [Preventing unencrypted writes](#preventing-unencrypted-writes) - that constraint, not the driver, is
> what guarantees the column only ever holds ciphertext.

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

## Operational requirements

- Every application that writes an encrypted column must enable the plugin.
- Register a column for encryption **before** inserting data into it, or migrate the existing rows.
- Write encrypted columns using parameters, never literals.
- Administrative tools bypass encryption entirely; use the triggers above to catch that.
