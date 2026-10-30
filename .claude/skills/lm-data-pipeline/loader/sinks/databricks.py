"""Databricks(Unity Catalog) 적재 — <catalog>.<층>.<표> Delta 테이블.

연결: databricks-sql-connector (`pip install databricks-sql-connector`) · SQL Warehouse
  환경변수 DATABRICKS_HOST(예 dbc-xxxx.cloud.databricks.com) · DATABRICKS_HTTP_PATH · DATABRICKS_TOKEN
싣는 방법:
  staging_volume 이 있으면  PUT(파일 올림) → COPY INTO  — 큰 표에 권장. Volume 은 미리 만들어 둔다
  없으면                    INSERT … VALUES 묶음(batch_rows 행씩)
"""
from __future__ import annotations

import os
import tempfile
import uuid

from .base import Sink, SinkError, env


class DatabricksSink(Sink):
    kind = "databricks"
    TYPE_MAP = {"string": "STRING", "int": "INT", "bigint": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN",
                "date": "DATE", "timestamp": "TIMESTAMP", "json": "STRING"}

    def __init__(self, cfg):
        super().__init__(cfg)
        self.catalog = cfg.get("catalog")
        if not self.catalog:
            raise SinkError("databricks 대상에는 catalog 가 필요합니다(미리 만들어 둔 Unity Catalog)")
        self.volume = cfg.get("staging_volume")
        self.batch = int(cfg.get("batch_rows", 500))
        self.conn = None

    def describe(self):
        return f"databricks · catalog {self.catalog}" + (f" · staging {self.volume}" if self.volume else "")

    def fq(self, layer, table):
        return f"`{self.catalog}`.`{self.ns(layer)}`.`{self.tname(table)}`"

    def ddl_namespace(self, layer):
        return f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.ns(layer)}`"

    def ddl_suffix(self, table):
        part = [c for c in table.get("partition", []) if c in {x["name"] for x in table["columns"]}]
        return " USING DELTA" + (f" PARTITIONED BY ({', '.join(self.q(c) for c in part)})" if part else "")

    def connect(self):
        try:
            from databricks import sql
        except ImportError as e:
            raise SinkError("pip install databricks-sql-connector 가 필요합니다") from e
        kw = {"server_hostname": env("DATABRICKS_HOST").replace("https://", "").rstrip("/"),
              "http_path": env("DATABRICKS_HTTP_PATH"), "access_token": env("DATABRICKS_TOKEN")}
        if self.volume:
            kw["staging_allowed_local_path"] = tempfile.gettempdir()
        self.conn = sql.connect(**kw)

    def close(self):
        if self.conn:
            self.conn.close()

    def _x(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params) if params is not None else cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return None

    def ping(self):
        return str(self._x("SELECT current_catalog(), current_user()"))

    def ensure_namespace(self, layer):
        self._x(self.ddl_namespace(layer))

    def ensure_table(self, layer, table):
        self._x(self.ddl_table(layer, table))

    def replace(self, layer, table, df, run_id):
        fq = self.fq(layer, table["name"])
        self._x(f"DELETE FROM {fq} WHERE `_run_id` = ?", [run_id])
        if not len(df):
            return 0
        cols = [c["name"] for c in table["columns"]]
        if self.volume:
            tmp = os.path.join(tempfile.gettempdir(), f"lmdp_{uuid.uuid4().hex}.parquet")
            df[cols].to_parquet(tmp, index=False)
            remote = f"{self.volume.rstrip('/')}/{self.ns(layer)}/{self.tname(table['name'])}_{uuid.uuid4().hex[:8]}.parquet"
            try:
                self._x(f"PUT '{tmp}' INTO '{remote}' OVERWRITE")
                self._x(f"COPY INTO {fq} FROM '{remote}' FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'false')")
                self._x(f"REMOVE '{remote}'")
            finally:
                os.remove(tmp)
            return len(df)
        cl = ", ".join(self.q(c) for c in cols)
        rows = df[cols].astype(object).where(df[cols].notna(), None).values.tolist()
        for i in range(0, len(rows), self.batch):
            chunk = rows[i:i + self.batch]
            ph = ", ".join("(" + ", ".join("?" for _ in cols) + ")" for _ in chunk)
            self._x(f"INSERT INTO {fq} ({cl}) VALUES {ph}", [v for r in chunk for v in r])
        return len(rows)

    def count(self, layer, table, run_id):
        r = self._x(f"SELECT COUNT(*) FROM {self.fq(layer, table['name'])} WHERE `_run_id` = ?", [run_id])
        return int(r[0][0]) if r else 0
