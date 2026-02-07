---
name: database-migration-generator
description: Generate up/down SQL migrations from JSON schema diffs for PostgreSQL and SQLite.
version: 0.1.0
license: Apache-2.0
---

# Database Migration Generator

## Purpose

Generate reversible SQL migrations by diffing two schema snapshots so agents can safely evolve PostgreSQL or SQLite schemas with explicit safeguards for destructive changes.

## Instructions

1. Create two schema files in JSON format (`old` and `new`) using the expected `tables -> columns` structure.
2. Run `./scripts/run.py --from-file <old.json> --to-file <new.json> --dialect <postgres|sqlite>`.
3. Review warnings and destructive-operation checks in the output.
4. Re-run with `--allow-destructive` only after confirming destructive changes are intentional.
5. Use `--dry-run` to preview detected operations without generating SQL text.
6. Use `--out <path>` to write migration output to a file, or omit it to print to stdout.
7. Apply the `-- UP` section for forward migration and `-- DOWN` section for rollback.

## Inputs

- `--from-file`: Path to current schema JSON.
- `--to-file`: Path to target schema JSON.
- `--dialect`: `postgres` or `sqlite`.
- `--out` (optional): Output file path.
- `--dry-run` (optional): Show operation summary only.
- `--allow-destructive` (optional): Permit drops and destructive changes.

## Outputs

- Dry-run summary of planned operations and safety notes.
- Or generated migration text containing `-- UP` and `-- DOWN` SQL sections.
- Exit code `0` on success, non-zero on validation or safety failures.

## Constraints

- Input schemas must be valid JSON with table/column metadata.
- SQLite output only auto-generates additive safe operations; unsupported destructive/alter operations are flagged for manual handling.
- Destructive operations are blocked unless `--allow-destructive` is explicitly provided.