"""로컬 파일 적재 — <path>/<층>/<표>.csv|parquet. 설치·계정 없이 도는 기본 대상이자 자가 검증 대상."""
from __future__ import annotations

import json
from pathlib import Path

from .base import Sink, SinkError


class LocalSink(Sink):
    kind = "local"
    TYPE_MAP = {"string": "string", "int": "int", "bigint": "bigint", "double": "double", "boolean": "boolean",
                "date": "date", "timestamp": "timestamp", "json": "string(json)"}

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.root = Path(cfg.get("path") or "")
        self.fmt = (cfg.get("format") or "csv").lower()
        if self.fmt not in ("csv", "parquet"):
            raise SinkError(f"local format 은 csv 또는 parquet: {self.fmt}")

    def describe(self) -> str:
        return f"local · {self.root} · {self.fmt}"

    def fq(self, layer, table):
        return str(self.root / self.ns(layer) / f"{self.tname(table)}.{self.fmt}")

    def ddl_namespace(self, layer):
        return f"-- 폴더 {self.root / self.ns(layer)}"

    def ddl_table(self, layer, table):
        cols = ", ".join(f"{c['name']}:{self.TYPE_MAP[c['type']]}" for c in table["columns"])
        return f"-- 파일 {self.fq(layer, table['name'])}  ({cols})"

    def ensure_namespace(self, layer):
        if not str(self.root):
            raise SinkError("local 대상에는 path 가 필요합니다")
        (self.root / self.ns(layer)).mkdir(parents=True, exist_ok=True)

    def ensure_table(self, layer, table):
        side = self.root / self.ns(layer) / f"{self.tname(table['name'])}.schema.json"
        side.write_text(json.dumps({"table": table["name"], "columns": table["columns"], "partition": table.get("partition", [])},
                                   ensure_ascii=False, indent=1), encoding="utf-8")

    def _read(self, p: Path):
        import pandas as pd
        if not p.exists():
            return None
        return pd.read_parquet(p) if self.fmt == "parquet" else pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8")

    def replace(self, layer, table, df, run_id):
        import pandas as pd
        p = Path(self.fq(layer, table["name"]))
        old = self._read(p)
        if old is not None and "_run_id" in old.columns:
            old = old[old["_run_id"] != run_id]
            out = pd.concat([old.astype(object), df.astype(object)], ignore_index=True) if len(old) else df
        else:
            out = df
        if self.fmt == "parquet":
            out.to_parquet(p, index=False)
        else:
            out.to_csv(p, index=False, encoding="utf-8")
        return len(df)

    def count(self, layer, table, run_id):
        df = self._read(Path(self.fq(layer, table["name"])))
        if df is None:
            return 0
        return int((df["_run_id"].astype(str) == run_id).sum()) if "_run_id" in df.columns else 0
