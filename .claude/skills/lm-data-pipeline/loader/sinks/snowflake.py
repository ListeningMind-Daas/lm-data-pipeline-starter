"""Snowflake 적재 — <DATABASE>.<층>.<표>. 이름은 대문자.

연결: snowflake-connector-python[pandas] (`pip install "snowflake-connector-python[pandas]"`)
  환경변수 SNOWFLAKE_ACCOUNT · SNOWFLAKE_USER · 인증 둘 중 하나(SNOWFLAKE_PRIVATE_KEY_PATH 키페어 · SNOWFLAKE_PASSWORD)
  선택 SNOWFLAKE_ROLE · SNOWFLAKE_WAREHOUSE (설정 파일의 warehouse·role 이 먼저)
싣는 방법: DELETE(_run_id) → write_pandas(내부 스테이지 → COPY)
"""
from __future__ import annotations

from .base import Sink, SinkError, env


class SnowflakeSink(Sink):
    kind = "snowflake"
    upper = True
    TYPE_MAP = {"string": "VARCHAR", "int": "INTEGER", "bigint": "NUMBER(38,0)", "double": "FLOAT", "boolean": "BOOLEAN",
                "date": "DATE", "timestamp": "TIMESTAMP_NTZ", "json": "VARCHAR"}

    def __init__(self, cfg):
        super().__init__(cfg)
        self.database = (cfg.get("database") or "").upper()
        if not self.database:
            raise SinkError("snowflake 대상에는 database 가 필요합니다")
        self.conn = None

    def describe(self):
        return f"snowflake · database {self.database}"

    def q(self, name):
        return f'"{name}"'

    def fq(self, layer, table):
        return f'{self.database}.{self.ns(layer)}.{self.tname(table)}'

    def ddl_namespace(self, layer):
        return f"CREATE SCHEMA IF NOT EXISTS {self.database}.{self.ns(layer)}"

    def connect(self):
        try:
            import snowflake.connector
        except ImportError as e:
            raise SinkError('pip install "snowflake-connector-python[pandas]" 가 필요합니다') from e
        kw = {"account": env("SNOWFLAKE_ACCOUNT"), "user": env("SNOWFLAKE_USER"),
              "warehouse": self.cfg.get("warehouse") or env("SNOWFLAKE_WAREHOUSE", required=False),
              "role": self.cfg.get("role") or env("SNOWFLAKE_ROLE", required=False)}
        key = env("SNOWFLAKE_PRIVATE_KEY_PATH", required=False)
        if key:
            from cryptography.hazmat.primitives import serialization
            with open(key, "rb") as f:
                pk = serialization.load_pem_private_key(f.read(), password=(env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", required=False) or "").encode() or None)
            kw["private_key"] = pk.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        else:
            kw["password"] = env("SNOWFLAKE_PASSWORD")
        self.conn = snowflake.connector.connect(**{k: v for k, v in kw.items() if v})
        self._x(f"CREATE DATABASE IF NOT EXISTS {self.database}") if self.cfg.get("create_database") else None

    def close(self):
        if self.conn:
            self.conn.close()

    def _x(self, sql, params=None):
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            try:
                return cur.fetchall()
            except Exception:
                return None
        finally:
            cur.close()

    def ping(self):
        return str(self._x("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_WAREHOUSE()"))

    def ensure_namespace(self, layer):
        self._x(self.ddl_namespace(layer))

    def ensure_table(self, layer, table):
        self._x(self.ddl_table(layer, table))

    def replace(self, layer, table, df, run_id):
        from snowflake.connector.pandas_tools import write_pandas
        self._x(f'DELETE FROM {self.fq(layer, table["name"])} WHERE "_RUN_ID" = %s', (run_id,))
        if not len(df):
            return 0
        out = df[[c["name"] for c in table["columns"]]].copy()
        out.columns = [self.cname(c) for c in out.columns]
        ok, _, n, _ = write_pandas(self.conn, out, self.tname(table["name"]), database=self.database, schema=self.ns(layer),
                                   quote_identifiers=True, auto_create_table=False, use_logical_type=True)
        if not ok:
            raise SinkError(f"write_pandas 실패: {self.fq(layer, table['name'])}")
        return int(n)

    def count(self, layer, table, run_id):
        r = self._x(f'SELECT COUNT(*) FROM {self.fq(layer, table["name"])} WHERE "_RUN_ID" = %s', (run_id,))
        return int(r[0][0]) if r else 0
