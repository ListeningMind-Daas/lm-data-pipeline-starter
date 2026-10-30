"""적재 대상 공통 인터페이스 — 로컬 파일 · Databricks · Snowflake · BigQuery 가 같은 순서로 불린다.

  ensure_namespace(층)      → 스키마(데이터셋·폴더)가 없으면 만든다
  ensure_table(층, 표)      → 설계안 컬럼·타입으로 테이블이 없으면 만든다
  replace(층, 표, df, run)  → 같은 _run_id 행을 지우고 싣는다(재실행해도 한 번만 실린다)
  count(층, 표, run)        → 검증용 행 수

DDL 은 연결 없이 문자열로 만든다(ddl_namespace · ddl_table) — `lmdp.py ddl`·`plan` 이 쓴다.
드라이버(databricks-sql-connector · snowflake-connector-python · google-cloud-bigquery)는 connect() 에서만 불러온다.
"""
from __future__ import annotations

import os
import re

TYPES = ("string", "int", "bigint", "double", "boolean", "date", "timestamp", "json")


class SinkError(RuntimeError):
    pass


def env(name: str, required: bool = True) -> str | None:
    v = os.environ.get(name)
    if required and not v:
        raise SinkError(f"환경변수 {name} 가 비어 있습니다 — 비밀값은 설정 파일이 아니라 환경변수로만 넘깁니다")
    return v


def ident(s: str) -> str:
    """스키마·테이블 이름 — 영문·숫자·밑줄만(대상 저장소 공통으로 안전한 이름)."""
    out = re.sub(r"[^0-9A-Za-z_]", "_", s)
    if not out or out[0].isdigit():
        out = "t_" + out
    return out


class Sink:
    kind = "base"
    TYPE_MAP: dict[str, str] = {}
    upper = False          # Snowflake 처럼 이름을 대문자로 쓰는 대상

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.prefix = cfg.get("schema_prefix", "") or ""

    # ── 이름 ──
    def ns(self, layer: str) -> str:
        n = ident(f"{self.prefix}{layer}")
        return n.upper() if self.upper else n.lower()

    def tname(self, table: str) -> str:
        n = ident(table)
        return n.upper() if self.upper else n.lower()

    def cname(self, col: str) -> str:
        return col.upper() if self.upper else col

    def fq(self, layer: str, table: str) -> str:
        raise NotImplementedError

    def q(self, name: str) -> str:
        """컬럼 이름 인용 — 예약어(group 등)를 안전하게."""
        return f"`{name}`"

    # ── DDL (연결 없이) ──
    def ddl_namespace(self, layer: str) -> str:
        raise NotImplementedError

    def ddl_table(self, layer: str, table: dict) -> str:
        cols = ",\n  ".join(f"{self.q(self.cname(c['name']))} {self.TYPE_MAP[c['type']]}" for c in table["columns"])
        return f"CREATE TABLE IF NOT EXISTS {self.fq(layer, table['name'])} (\n  {cols}\n){self.ddl_suffix(table)}"

    def ddl_suffix(self, table: dict) -> str:
        return ""

    def describe(self) -> str:
        return self.kind

    # ── 실행 ──
    def connect(self):
        pass

    def close(self):
        pass

    def ping(self) -> str:
        return "ok"

    def ensure_namespace(self, layer: str):
        raise NotImplementedError

    def ensure_table(self, layer: str, table: dict):
        raise NotImplementedError

    def replace(self, layer: str, table: dict, df, run_id: str) -> int:
        raise NotImplementedError

    def count(self, layer: str, table: dict, run_id: str) -> int:
        raise NotImplementedError
