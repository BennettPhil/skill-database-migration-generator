#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


class Operation:
    def __init__(
        self,
        op_type: str,
        table: str,
        column: str | None = None,
        payload: Dict[str, Any] | None = None,
        destructive: bool = False,
    ) -> None:
        self.op_type = op_type
        self.table = table
        self.column = column
        self.payload = payload or {}
        self.destructive = destructive


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate up/down SQL migrations from schema JSON diffs."
    )
    parser.add_argument("--from-file", required=True, help="Path to source schema JSON.")
    parser.add_argument("--to-file", required=True, help="Path to target schema JSON.")
    parser.add_argument(
        "--dialect", required=True, choices=["postgres", "sqlite"], help="SQL dialect."
    )
    parser.add_argument("--out", help="Optional output path. Defaults to stdout.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show operation summary only; do not print migration SQL.",
    )
    parser.add_argument(
        "--allow-destructive",
        action="store_true",
        help="Allow destructive operations such as drops and narrowing changes.",
    )
    return parser.parse_args()


def load_schema(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise ValueError(f"schema file not found: {path}")
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {path}: {exc}") from exc

    tables = raw.get("tables")
    if not isinstance(tables, dict):
        raise ValueError(f"{path} must contain object key 'tables'")

    for table_name, table_def in tables.items():
        if not isinstance(table_def, dict):
            raise ValueError(f"table '{table_name}' must be an object")
        columns = table_def.get("columns")
        if not isinstance(columns, dict):
            raise ValueError(f"table '{table_name}' must contain object key 'columns'")
        for col_name, col_def in columns.items():
            if not isinstance(col_def, dict):
                raise ValueError(f"column '{table_name}.{col_name}' must be an object")
            if "type" not in col_def:
                raise ValueError(f"column '{table_name}.{col_name}' missing 'type'")
    return raw


def column_sql(col_name: str, col_def: Dict[str, Any], dialect: str) -> str:
    parts = [f'"{col_name}"', str(col_def["type"])]
    if col_def.get("primary_key"):
        parts.append("PRIMARY KEY")
    if col_def.get("nullable") is False:
        parts.append("NOT NULL")
    if "default" in col_def and col_def["default"] is not None:
        default_val = col_def["default"]
        if isinstance(default_val, str) and not default_val.upper().startswith("CURRENT_"):
            default_sql = f"'{default_val}'"
        else:
            default_sql = str(default_val)
        parts.append(f"DEFAULT {default_sql}")
    if dialect == "sqlite" and col_def.get("autoincrement"):
        parts.append("AUTOINCREMENT")
    return " ".join(parts)


def diff_schemas(source: Dict[str, Any], target: Dict[str, Any]) -> List[Operation]:
    ops: List[Operation] = []
    src_tables = source["tables"]
    tgt_tables = target["tables"]

    for table in sorted(tgt_tables.keys() - src_tables.keys()):
        ops.append(Operation("create_table", table, payload=tgt_tables[table]))

    for table in sorted(src_tables.keys() - tgt_tables.keys()):
        ops.append(Operation("drop_table", table, destructive=True))

    for table in sorted(src_tables.keys() & tgt_tables.keys()):
        src_cols = src_tables[table]["columns"]
        tgt_cols = tgt_tables[table]["columns"]

        for col in sorted(tgt_cols.keys() - src_cols.keys()):
            ops.append(
                Operation(
                    "add_column", table, col, payload={"column_def": tgt_cols[col]}
                )
            )

        for col in sorted(src_cols.keys() - tgt_cols.keys()):
            ops.append(Operation("drop_column", table, col, destructive=True))

        for col in sorted(src_cols.keys() & tgt_cols.keys()):
            src_def = src_cols[col]
            tgt_def = tgt_cols[col]
            if src_def != tgt_def:
                destructive = False
                if src_def.get("nullable", True) and tgt_def.get("nullable") is False:
                    destructive = True
                if src_def.get("type") != tgt_def.get("type"):
                    destructive = True
                ops.append(
                    Operation(
                        "alter_column",
                        table,
                        col,
                        payload={"from": src_def, "to": tgt_def},
                        destructive=destructive,
                    )
                )
    return ops


def pg_up_sql(op: Operation) -> List[str]:
    if op.op_type == "create_table":
        cols = op.payload["columns"]
        col_sql = ",\n  ".join(column_sql(name, cols[name], "postgres") for name in sorted(cols))
        return [f'CREATE TABLE "{op.table}" (\n  {col_sql}\n);']
    if op.op_type == "drop_table":
        return [f'DROP TABLE "{op.table}";']
    if op.op_type == "add_column":
        return [
            f'ALTER TABLE "{op.table}" ADD COLUMN {column_sql(op.column or "", op.payload["column_def"], "postgres")};'
        ]
    if op.op_type == "drop_column":
        return [f'ALTER TABLE "{op.table}" DROP COLUMN "{op.column}";']
    if op.op_type == "alter_column":
        to_def = op.payload["to"]
        lines = [
            f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" TYPE {to_def["type"]};'
        ]
        if to_def.get("nullable", True):
            lines.append(f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" DROP NOT NULL;')
        else:
            lines.append(f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" SET NOT NULL;')
        if "default" in to_def and to_def["default"] is not None:
            default_val = to_def["default"]
            if isinstance(default_val, str) and not default_val.upper().startswith("CURRENT_"):
                default_sql = f"'{default_val}'"
            else:
                default_sql = str(default_val)
            lines.append(
                f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" SET DEFAULT {default_sql};'
            )
        else:
            lines.append(f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" DROP DEFAULT;')
        return lines
    return [f"-- unsupported operation: {op.op_type}"]


def pg_down_sql(op: Operation) -> List[str]:
    if op.op_type == "create_table":
        return [f'DROP TABLE "{op.table}";']
    if op.op_type == "drop_table":
        return [f'-- manual rollback required: recreate dropped table "{op.table}"']
    if op.op_type == "add_column":
        return [f'ALTER TABLE "{op.table}" DROP COLUMN "{op.column}";']
    if op.op_type == "drop_column":
        return [f'-- manual rollback required: re-add dropped column "{op.table}.{op.column}"']
    if op.op_type == "alter_column":
        from_def = op.payload["from"]
        lines = [
            f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" TYPE {from_def["type"]};'
        ]
        if from_def.get("nullable", True):
            lines.append(f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" DROP NOT NULL;')
        else:
            lines.append(f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" SET NOT NULL;')
        if "default" in from_def and from_def["default"] is not None:
            default_val = from_def["default"]
            if isinstance(default_val, str) and not default_val.upper().startswith("CURRENT_"):
                default_sql = f"'{default_val}'"
            else:
                default_sql = str(default_val)
            lines.append(
                f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" SET DEFAULT {default_sql};'
            )
        else:
            lines.append(f'ALTER TABLE "{op.table}" ALTER COLUMN "{op.column}" DROP DEFAULT;')
        return lines
    return [f"-- unsupported operation: {op.op_type}"]


def sqlite_up_sql(op: Operation) -> List[str]:
    if op.op_type == "create_table":
        cols = op.payload["columns"]
        col_sql = ",\n  ".join(column_sql(name, cols[name], "sqlite") for name in sorted(cols))
        return [f'CREATE TABLE "{op.table}" (\n  {col_sql}\n);']
    if op.op_type == "drop_table":
        return [f'DROP TABLE "{op.table}";']
    if op.op_type == "add_column":
        return [
            f'ALTER TABLE "{op.table}" ADD COLUMN {column_sql(op.column or "", op.payload["column_def"], "sqlite")};'
        ]
    if op.op_type in {"drop_column", "alter_column"}:
        return [f'-- manual action required in sqlite: {op.op_type} on "{op.table}.{op.column}"']
    return [f"-- unsupported operation: {op.op_type}"]


def sqlite_down_sql(op: Operation) -> List[str]:
    if op.op_type == "create_table":
        return [f'DROP TABLE "{op.table}";']
    if op.op_type == "drop_table":
        return [f'-- manual rollback required: recreate dropped table "{op.table}"']
    if op.op_type == "add_column":
        return [f'-- manual rollback required in sqlite: drop column "{op.table}.{op.column}"']
    if op.op_type in {"drop_column", "alter_column"}:
        return [f'-- manual rollback required in sqlite: {op.op_type} on "{op.table}.{op.column}"']
    return [f"-- unsupported operation: {op.op_type}"]


def render_sql(ops: List[Operation], dialect: str) -> str:
    up_lines: List[str] = []
    down_lines: List[str] = []
    for op in ops:
        if dialect == "postgres":
            up_lines.extend(pg_up_sql(op))
            down_lines.extend(pg_down_sql(op))
        else:
            up_lines.extend(sqlite_up_sql(op))
            down_lines.extend(sqlite_down_sql(op))

    return "\n".join(
        [
            "-- Generated by database-migration-generator",
            "-- UP",
            *up_lines,
            "",
            "-- DOWN",
            *reversed(down_lines),
            "",
        ]
    )


def dry_run_summary(ops: List[Operation]) -> str:
    lines = ["Dry-run summary:", f"  total_operations: {len(ops)}"]
    by_type: Dict[str, int] = {}
    destructive = 0
    for op in ops:
        by_type[op.op_type] = by_type.get(op.op_type, 0) + 1
        if op.destructive:
            destructive += 1
    for key in sorted(by_type):
        lines.append(f"  {key}: {by_type[key]}")
    lines.append(f"  destructive_operations: {destructive}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    try:
        source = load_schema(args.from_file)
        target = load_schema(args.to_file)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    operations = diff_schemas(source, target)

    destructive_ops = [op for op in operations if op.destructive]
    if destructive_ops and not args.allow_destructive:
        print("Error: destructive operations detected. Re-run with --allow-destructive.", file=sys.stderr)
        for op in destructive_ops:
            col = f".{op.column}" if op.column else ""
            print(f"  - {op.op_type}: {op.table}{col}", file=sys.stderr)
        return 2

    if args.dry_run:
        output = dry_run_summary(operations)
    else:
        output = render_sql(operations, args.dialect)

    if args.out:
        Path(args.out).write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())