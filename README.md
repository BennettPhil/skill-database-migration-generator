# database-migration-generator

Generate up/down SQL migrations from JSON schema diffs for PostgreSQL and SQLite.

## Quick Start

```bash
./scripts/run.py \
  --from-file schema-old.json \
  --to-file schema-new.json \
  --dialect postgres \
  --out migration.sql
```

## Prerequisites

- Python 3.9+
- Valid schema JSON files with `tables` and `columns`