"""BigQuery 적재 — <project>.<층 데이터셋>.<표>. BigQuery 는 스키마 대신 데이터셋이 층 하나다.

연결: google-cloud-bigquery + pyarrow (`pip install google-cloud-bigquery pyarrow db-dtypes`)
  인증: 애플리케이션 기본 인증(`gcloud auth application-default login`) 또는 GOOGLE_APPLICATION_CREDENTIALS(서비스 계정 키 파일 경로)
  설정 project(필수) · location(기본 asia-northeast3)
싣는 방법: DELETE(_run_id) DML → load_table_from_dataframe(APPEND)
※ 이 대상은 실제 계정으로 적재를 확인하지 않았다 — DDL·계획·자가 검증 테스트만 통과한 상태
"""
from __future__ import annotations

from .base import Sink, SinkError


class BigQuerySink(Sink):
    kind = "bigquery"
    TYPE_MAP = {"string": "STRING", "int": "INT64", "bigint": "INT64", "double": "FLOAT64", "boolean": "BOOL",
                "date": "DATE", "timestamp": "TIMESTAMP", "json": "STRING"}

    def __init__(self, cfg):
        super().__init__(cfg)
        self.project = cfg.get("project")
        if not self.project:
            raise SinkError("bigquery 대상에는 project 가 필요합니다")
        self.location = cfg.get("location") or "asia-northeast3"
        self.client = None

    def describe(self):
        return f"bigquery · project {self.project} · {self.location}"

    def fq(self, layer, table):
        return f"`{self.project}.{self.ns(layer)}.{self.tname(table)}`"

    def ddl_namespace(self, layer):
        return f"CREATE SCHEMA IF NOT EXISTS `{self.project}.{self.ns(layer)}` OPTIONS(location = '{self.location}')"

    def ddl_suffix(self, table):
        dates = [c for c in table.get("partition", []) for x in table["columns"] if x["name"] == c and x["type"] in ("date", "timestamp")]
        rest = [c for c in table.get("partition", []) if c not in dates][:4]
        s = f"\nPARTITION BY {self.q(dates[0])}" if dates else ""
        return s + (f"\nCLUSTER BY {', '.join(self.q(c) for c in rest)}" if rest else "")

    def connect(self):
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise SinkError("pip install google-cloud-bigquery pyarrow db-dtypes 가 필요합니다") from e
        self.bq = bigquery
        self.client = bigquery.Client(project=self.project, location=self.location)

    def _x(self, sql, params=None):
        cfg = None
        if params:
            cfg = self.bq.QueryJobConfig(query_parameters=[self.bq.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()])
        return list(self.client.query(sql, job_config=cfg).result())

    def ping(self):
        return str(self._x("SELECT SESSION_USER()"))

    def ensure_namespace(self, layer):
        self._x(self.ddl_namespace(layer))

    def ensure_table(self, layer, table):
        self._x(self.ddl_table(layer, table))

    def replace(self, layer, table, df, run_id):
        fq = self.fq(layer, table["name"])
        self._x(f"DELETE FROM {fq} WHERE `_run_id` = @r", {"r": run_id})
        if not len(df):
            return 0
        schema = [self.bq.SchemaField(c["name"], self.TYPE_MAP[c["type"]].replace("INT64", "INTEGER").replace("FLOAT64", "FLOAT").replace("BOOL", "BOOLEAN"))
                  for c in table["columns"]]
        job = self.client.load_table_from_dataframe(df[[c["name"] for c in table["columns"]]], fq.strip("`"),
                                                    job_config=self.bq.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"))
        job.result()
        return len(df)

    def count(self, layer, table, run_id):
        r = self._x(f"SELECT COUNT(*) AS n FROM {self.fq(layer, table['name'])} WHERE `_run_id` = @r", {"r": run_id})
        return int(r[0][0]) if r else 0
