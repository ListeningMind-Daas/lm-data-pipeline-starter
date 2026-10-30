"""적재 대상 4종 — 설정의 target.type 으로 고른다."""
from .base import Sink, SinkError, TYPES
from .local import LocalSink
from .databricks import DatabricksSink
from .snowflake import SnowflakeSink
from .bigquery import BigQuerySink

KINDS = {"local": LocalSink, "databricks": DatabricksSink, "snowflake": SnowflakeSink, "bigquery": BigQuerySink}


def make(cfg: dict) -> Sink:
    kind = (cfg.get("type") or "").lower()
    if kind not in KINDS:
        raise SinkError(f"target.type 은 {', '.join(KINDS)} 중 하나: {kind!r}")
    return KINDS[kind](cfg)


__all__ = ["Sink", "SinkError", "TYPES", "KINDS", "make"]
