# -*- coding: utf-8 -*-
# ============================================================================
# local_engine.py — Databricks 미사용 로컬 적재 엔진
#
# 메달리온 변환(bronze → silver → gold · dashboard_mart)을 pandas 로 구현해
# 프로젝트 폴더에 스키마별 테이블(CSV)을 전량 적재한다.
#
#   projects/{gl}_{cat}_{run_date}/tables/{schema}/{table}.csv
#
# CSV 직렬화 규약: NULL→빈칸, boolean→true/false, date→YYYY-MM-DD,
# timestamp→"YYYY-MM-DD HH:MM:SS.ffffff", array<string>→JSON 배열 문자열.
# 인코딩은 utf-8-sig(Excel 한글 안전). 읽기 시 정본 스키마 타입으로 복원한다.
#
# 정본 스키마: scripts/_schemas.txt (+ 코드 내 bronze_raw/quarantine 보강).
# 모든 적재는 conform(정본 컬럼 투영·캐스팅) 을 거친다 — 스키마 변형 금지.
# 레이블 4종(실제데이터/추정/가정/데이터공백) 규칙(절대 불변).
#
# 실행 순서(DAG 등가): 00 ref → 01 bronze → 01b parse → 02 silver → 03 gold
#                     → 08 seed → 04 qc(BLOCK 게이트) → 05 reports → mart
# ============================================================================

import os, re, sys, csv, json, glob, math, hashlib
from datetime import datetime, timezone, date

import pandas as pd

# Windows 콘솔 cp949 인코딩 에러 방지 — UTF-8 강제 + 치환
for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

# bronze payload(수 MB JSON 문자열) 셀 허용 — 기본 한도 128KB 초과 대비
_lim = 2 ** 31 - 1
while _lim > 2 ** 20:
    try:
        csv.field_size_limit(_lim)
        break
    except OverflowError:
        _lim //= 2

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def projects_root() -> str:
    """run 폴더 루트 — LM_PROJECTS_ROOT 가 있으면 그것, 없으면 <ROOT>/projects.
    부를 때마다 계산한다: 테스트가 ROOT 를 임시 폴더로 바꾸면 그대로 따라간다(모듈 적재 때 고정하면 실데이터에 쓴다)."""
    return os.environ.get("LM_PROJECTS_ROOT") or os.path.join(ROOT, "projects")

# ── 4종 데이터 레이블 상수 (절대 불변) ─────────────────────────────────────────
LABEL_REAL, LABEL_EST, LABEL_ASSUM, LABEL_GAP = "실제데이터", "추정", "가정", "데이터공백"
LABEL_DOMAIN = {
    "label_volume":      {LABEL_REAL, LABEL_GAP},
    "label_intent":      {LABEL_REAL, LABEL_EST, LABEL_GAP},
    "label_demography":  {LABEL_REAL, LABEL_EST, LABEL_GAP},
    "label_derived":     {LABEL_EST, LABEL_ASSUM},
    "data_quality_flag": {LABEL_REAL, LABEL_GAP},
}
ALLOWED_GL = ("kr", "us", "jp")

CAT_NAME: dict = {}  # 품목 표시 이름은 프로젝트 설정 category_name(LM_CATEGORY_NAME)으로 받는다

# ── 정본 스키마 레지스트리 ─────────────────────────────────────────────────────
# _schemas.txt 에 없는 테이블(사전 생성 대상 아님)만 코드로 보강한다.
_BRONZE_COLS = ("source_file:string, source_file_hash:string, gl:string, run_date:date, "
                "category_key:string, api_version:string, request_at:timestamp, "
                "payload:string, ingested_at:timestamp")
EXTRA_SCHEMAS = {
    "bronze_raw.keyword_info_raw":   _BRONZE_COLS,
    "bronze_raw.intent_finder_raw":  _BRONZE_COLS,
    "bronze_raw.cluster_finder_raw": _BRONZE_COLS,
    "bronze_raw.path_finder_raw":    _BRONZE_COLS,
    "ops.silver_quarantine": ("gl:string, run_date:date, category_key:string, keyword_id:string, "
                              "keyword:string, community_id:string, month:string, row_json:string, "
                              "quarantine_reason:string, quarantined_at:timestamp, source_run_id:string"),
}

def load_schemas():
    """scripts/_schemas.txt → {'schema.table': [(col, type), ...]}"""
    out, name = {}, None
    txt = open(os.path.join(ROOT, "scripts", "_schemas.txt"), encoding="utf-8-sig").read()
    for line in txt.splitlines():
        line = line.strip()
        m = re.match(r"^###\s+(\S+)\s+###$", line)
        if m:
            name = m.group(1)
        elif line and name:
            out[name] = [tuple(c.strip().split(":", 1)) for c in line.split(", ")]
            name = None
    for fqn, cols in EXTRA_SCHEMAS.items():
        out[fqn] = [tuple(c.strip().split(":", 1)) for c in cols.split(", ")]
    return out

SCHEMAS = load_schemas()

def _isnull(v):
    if v is None:
        return True
    if isinstance(v, (list, tuple, dict)):
        return False
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False

def _coerce(v, t):
    if _isnull(v):
        return None
    if t == "string":
        return v if isinstance(v, str) else str(v)
    if t in ("bigint", "int"):
        return int(round(float(v)))
    if t == "double":
        return float(v)
    if t == "boolean":
        return bool(v)
    if t == "date":
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        return date.fromisoformat(str(v)[:10])
    if t == "timestamp":
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo else v
        try:
            d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            return d.astimezone(timezone.utc).replace(tzinfo=None) if d.tzinfo else d
        except ValueError:
            return None
    if t == "array<string>":
        return [str(x) for x in v]
    raise ValueError(f"미지원 타입: {t}")

def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def to_records(df_or_rows):
    """pandas DF / dict 리스트 → NaN 을 None 으로 정리한 dict 리스트."""
    rows = df_or_rows.to_dict("records") if isinstance(df_or_rows, pd.DataFrame) else list(df_or_rows)
    return [{k: (None if _isnull(v) else v) for k, v in r.items()} for r in rows]


# 스키마 → 폴더명 (파이프라인 실행 순서 프리픽스 — 탐색 편의)
SCHEMA_DIRS = {
    "seed_design":    "00_seed_design",
    "bronze_raw":     "01_bronze_raw",
    "silver_master":  "02_silver_master",
    "gold_analytics": "03_gold_analytics",
    "gold_reports":   "04_gold_reports",
    "cost":           "11_cost",
    "usage":          "12_usage",
    "ref":            "98_ref",
    "ops":            "99_ops",
}


class LocalTables:
    """프로젝트 폴더 tables/ 아래 정본 스키마 CSV 적재기."""

    def __init__(self, project_dir):
        self.base = os.path.join(project_dir, "tables")

    def path(self, fqn):
        schema, table = fqn.split(".")
        return os.path.join(self.base, SCHEMA_DIRS.get(schema, schema), f"{table}.csv")

    def conform(self, fqn, rows):
        """정본 스키마 투영·캐스팅 — 타깃 외 컬럼 제거, 부재 컬럼 NULL."""
        cols = SCHEMAS[fqn]
        return [{c: _coerce(r.get(c), t) for c, t in cols} for r in to_records(rows)]

    @staticmethod
    def _ser(v, t):
        """CSV 셀 직렬화 — NULL→빈칸, boolean→true/false, array→JSON."""
        if v is None:
            return ""
        if t == "boolean":
            return "true" if v else "false"
        if t == "date":
            return v.isoformat()
        if t == "timestamp":
            return v.isoformat(sep=" ")
        if t == "array<string>":
            return json.dumps(v, ensure_ascii=False)
        return str(v)

    @staticmethod
    def _de(s, t):
        """CSV 셀 역직렬화 — 정본 타입으로 복원 (빈칸→NULL)."""
        if s is None or s == "":
            return None
        if t == "string":
            return s
        if t in ("bigint", "int"):
            return int(s)
        if t == "double":
            return float(s)
        if t == "boolean":
            return s.strip().lower() == "true"
        if t == "date":
            return date.fromisoformat(s[:10])
        if t == "timestamp":
            return datetime.fromisoformat(s)
        if t == "array<string>":
            return json.loads(s)
        raise ValueError(f"미지원 타입: {t}")

    def write(self, fqn, rows):
        """정본 스키마로 투영·캐스팅 후 전체 덮어쓰기(프로젝트=단일 파티션이라 멱등)."""
        recs = self.conform(fqn, rows)
        cols = SCHEMAS[fqn]
        p = self.path(fqn)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow([c for c, _ in cols])
            for r in recs:
                w.writerow([self._ser(r[c], t) for c, t in cols])
        return len(recs)

    def append(self, fqn, rows):
        """로그성 테이블(ops.*) append — 기존 행 유지."""
        old = self.read_records(fqn)
        return self.write(fqn, old + self.conform(fqn, rows))

    def read_records(self, fqn):
        p = self.path(fqn)
        if not os.path.exists(p):
            return []
        types = dict(SCHEMAS[fqn])
        with open(p, encoding="utf-8-sig", newline="") as f:
            return [{k: self._de(v, types[k]) for k, v in row.items() if k in types}
                    for row in csv.DictReader(f)]

    def read_df(self, fqn):
        recs = self.read_records(fqn)
        if recs:
            return pd.DataFrame(recs)
        return pd.DataFrame(columns=[c for c, _ in SCHEMAS[fqn]])

    def count(self, fqn):
        p = self.path(fqn)
        if not os.path.exists(p):
            return 0
        # 본문에 개행 포함 셀(md 섹션 등)이 있어 csv 파서로 행수 계산
        with open(p, encoding="utf-8-sig", newline="") as f:
            return max(sum(1 for _ in csv.reader(f)) - 1, 0)


class QCBlocked(Exception):
    """QC BLOCK 체크 실패 — 후속 단계(05 reports·mart) 차단."""


class Engine:
    def __init__(self, gl, category_key, run_date, run_id=None,
                 qc_noise_pct_max=30.0, qc_generic_pct_max=40.0, qc_coverage_pct_min=80.0,
                 qc_cluster_coverage_pct_min=20.0,  # 실측 사례 상한 20%대 — 60 은 비현실
                 qc_dup_volume_pct_max=10.0,  # [가정] 실측 사례 0.2%~62.7%
                 focus_brand=None, rival_brand=None):  # 마트 헤드라인에 따로 뽑을 관심·경쟁 브랜드(브랜드 룰북 이름)
        if gl not in ALLOWED_GL:
            raise ValueError(f"gl 값이 허용 범위를 벗어났습니다: {gl!r} (허용: {ALLOWED_GL})")
        self.gl, self.cat, self.run_date = gl, category_key, run_date
        yy, mm = run_date.split("-")[:2]
        self.run_date_dt = date(int(yy), int(mm), 1)
        self.run_id = run_id or f"mi-{gl}-{run_date.replace('-', '')}01-01"
        self.proj = f"{gl}_{category_key}_{run_date}"
        self.pdir = os.path.join(projects_root(), self.proj)
        self.tables = LocalTables(self.pdir)
        self.focus_brand = focus_brand or os.environ.get("LM_FOCUS_BRAND") or None
        self.rival_brand = rival_brand or os.environ.get("LM_RIVAL_BRAND") or None
        self.qc_noise_pct_max = qc_noise_pct_max
        self.qc_generic_pct_max = qc_generic_pct_max
        self.qc_coverage_pct_min = qc_coverage_pct_min
        self.qc_cluster_coverage_pct_min = qc_cluster_coverage_pct_min
        self.qc_dup_volume_pct_max = qc_dup_volume_pct_max
        self._loaded = []          # data_freshness 대상 (fqn 순서 기록)

    # ── 공통 헬퍼 ────────────────────────────────────────────────────────────
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

    def lineage(self, rows):
        out = []
        for r in to_records(rows):
            r.update(gl=self.gl, run_date=self.run_date_dt, category_key=self.cat,
                     source_run_id=self.run_id, ingested_at=now_utc())
            out.append(r)
        return out

    def save(self, fqn, rows):
        n = self.tables.write(fqn, rows)
        if fqn not in self._loaded:
            self._loaded.append(fqn)
        self.log(f"  ✔ {fqn:<38} {n} 행")
        return n

    def log_run_step(self, phase, status, rows=None, note=""):
        msg = note or ""
        if rows is not None:
            msg = f"rows={rows}" if not msg else f"{msg}; rows={rows}"
        self.tables.append("ops.pipeline_runs", [{
            "run_id": self.run_id, "gl": self.gl, "run_date": self.run_date_dt,
            "category_key": self.cat, "phase": phase, "status": status,
            "started_at": None, "finished_at": now_utc(), "duration_sec": None,
            "error_message": msg, "triggered_by": self.run_id, "ingested_at": now_utc()}])
        if "ops.pipeline_runs" not in self._loaded:
            self._loaded.append("ops.pipeline_runs")

    @staticmethod
    def kid(k):
        return hashlib.sha1(k.encode("utf-8")).hexdigest()[:16]

    # ── [00] load_ref — ref/*/ CSV → ref.* ─────────────────────────────────
    RULEBOOKS = [
        ("noise_patterns",    "noise_patterns",    ["gl", "pattern", "action", "version"], True),
        ("brand_rules",       "brand_rules",       ["gl", "pattern", "brand", "version"],  True),
        ("category_rules",    "category_rules",    ["gl", "pattern", "category_lvl1", "version"], True),
        ("seed_layer_config", "seed_layer_config", ["gl"], False),
        ("market_calendar",   "market_calendar",   ["gl"], False),
    ]

    def stage_load_ref(self):
        self.log("[00] load_ref — 룰북 CSV → ref.*")
        summary = {}
        try:
            for subdir, table, req_cols, required in self.RULEBOOKS:
                files = sorted(glob.glob(os.path.join(ROOT, "ref", subdir, "*.csv")))
                if not files:
                    if required:
                        raise FileNotFoundError(f"필수 룰북 누락: ref/{subdir}/")
                    self.log(f"  [SKIP] ref/{subdir}/ 룰북 없음")
                    continue
                if len(files) > 1:
                    self.log(f"  [WARN] ref/{subdir}/ CSV {len(files)}개 — union 적재(폴더당 1개 권장)")
                df = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False)
                                .replace({"": None}) for f in files], ignore_index=True)
                missing = [c for c in req_cols if c not in df.columns]
                if missing:
                    raise ValueError(f"ref.{table}: 필수 컬럼 누락 {missing}")
                # run 범위만 싣는다 — 이 run 의 gl + (이 품목 또는 공통 '*'). 룰북 CSV 는 시장·품목 공용이라
                # 통째로 실으면 다른 시장 행이 섞인다(kr run 에 us seed_layer 등). 태깅(_load_rule)과 같은 기준이라 결과는 불변.
                keep = df["gl"] == self.gl
                if "category_key" in df.columns:
                    keep &= df["category_key"].isin([self.cat, "*"])
                if (~keep).any():
                    self.log(f"  ref.{table}: run 범위 밖 {int((~keep).sum())}행 제외 (gl={self.gl} · 품목={self.cat}|*)")
                df = df[keep].reset_index(drop=True)
                df["loaded_at"] = now_utc()
                summary[f"ref.{table}"] = self.save(f"ref.{table}", df)
            self.log_run_step("load_ref", "SUCCESS", rows=sum(summary.values()), note=str(summary))
        except Exception as e:
            self.log_run_step("load_ref", "FAILED", note=str(e)[:500])
            raise

    # ── [01] ingest_bronze — bronze/{api}/*.json → bronze_raw.* ─────────────
    BRONZE_SOURCES = [
        ("keyword_info",   "keyword_info_raw"),
        ("intent_finder",  "intent_finder_raw"),
        ("cluster_finder", "cluster_finder_raw"),
        ("path_finder",    "path_finder_raw"),
    ]

    def stage_ingest_bronze(self):
        self.log("[01] ingest_bronze — API 원시 JSON → bronze_raw.*")
        total = 0
        try:
            for api_folder, table in self.BRONZE_SOURCES:
                src = os.path.join(self.pdir, "bronze", api_folder)
                files = sorted(glob.glob(os.path.join(src, "*.json")))
                if not files:
                    self.log(f"  [SKIP] bronze/{api_folder}/ 없음·비어 있음")
                    self.save(f"bronze_raw.{table}", [])
                    continue
                rows, seen = [], set()
                for f in files:
                    payload = open(f, encoding="utf-8").read()
                    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                    if h in seen:          # 동일 파일 재인입 차단 (멱등)
                        continue
                    seen.add(h)
                    try:
                        obj = json.loads(payload)
                    except json.JSONDecodeError:
                        obj = {}
                    rows.append({
                        "source_file": f.replace("\\", "/"),
                        "source_file_hash": h,
                        "api_version": obj.get("api_version") or obj.get("version"),
                        "request_at": obj.get("request_at"),
                        "payload": payload})
                total += self.save(f"bronze_raw.{table}", self.lineage(rows))
            self.log_run_step("ingest_bronze", "SUCCESS", rows=total)
        except Exception as e:
            self.log_run_step("ingest_bronze", "FAILED", note=str(e)[:500])
            raise

    # ── [01b] parse_to_silver — bronze payload → silver 프레임 ───────────────
    @staticmethod
    def _primary_intent(it):
        m = {"I": it.get("i", 0) or 0, "N": it.get("n", 0) or 0,
             "C": it.get("c", 0) or 0, "T": it.get("t", 0) or 0}
        return max(m, key=m.get)

    def stage_parse_to_silver(self):
        self.log("[01b] parse_to_silver — payload 파싱")
        ff = lambda x: float(x) if x is not None else None
        payloads = [r["payload"] for r in self.tables.read_records("bronze_raw.keyword_info_raw")]
        seen, km_rows, mo_rows, kf_rows = set(), [], [], []
        for p in payloads:
            for d in json.loads(p).get("data", []):
                k = d.get("keyword")
                if not k:
                    continue
                ki = self.kid(k)
                if ki in seen:
                    continue
                seen.add(ki)
                a   = d.get("ads_metrics") or {}
                it  = d.get("intents") or {}
                dem = d.get("demography") or {}
                ft  = d.get("features") or {}
                va  = a.get("volume_avg")
                has_dem = any((dem.get(x) or 0) for x in
                              ["f_gender_ratio", "m_gender_ratio", "a30_ratio", "a40_ratio", "a50_ratio"])
                km_rows.append({
                    "keyword_id": ki, "keyword": k,
                    "volume_avg": ff(va), "volume_total": ff(a.get("volume_total")), "volume_trend": ff(a.get("volume_trend")),
                    "gg_volume_avg": ff(a.get("gg_volume_avg")), "gg_volume_total": ff(a.get("gg_volume_total")), "gg_volume_trend": ff(a.get("gg_volume_trend")),
                    "nv_volume_avg": ff(a.get("nv_volume_avg")), "nv_volume_total": ff(a.get("nv_volume_total")), "nv_volume_trend": ff(a.get("nv_volume_trend")),
                    "competition": a.get("competition"), "competition_index": ff(a.get("competition_index")),
                    "cpc": ff(a.get("cpc")), "low_bid_micros": ff(a.get("low_bid_micros")), "high_bid_micros": ff(a.get("high_bid_micros")),
                    "intent_i": ff(it.get("i")), "intent_n": ff(it.get("n")), "intent_c": ff(it.get("c")), "intent_t": ff(it.get("t")),
                    "intent_primary": self._primary_intent(it),
                    "gender_m_ratio": ff(dem.get("m_gender_ratio")), "gender_f_ratio": ff(dem.get("f_gender_ratio")),
                    "age_0_ratio": ff(dem.get("a0_ratio")), "age_13_ratio": ff(dem.get("a13_ratio")),
                    "age_20_ratio": ff(dem.get("a20_ratio")), "age_25_ratio": ff(dem.get("a25_ratio")),
                    "age_30_ratio": ff(dem.get("a30_ratio")), "age_40_ratio": ff(dem.get("a40_ratio")), "age_50_ratio": ff(dem.get("a50_ratio")),
                    # growth_3m 폴백: 스냅샷 비교월이 없는 첫 회차는 API volume_trend(3개월 증감)로 채움
                    # (label_derived=추정 유지). 다음 회차부터 스냅샷 diff 가 있으면 그 값이 우선.
                    "growth_3m": ff(a.get("volume_trend")),
                    "label_volume": (LABEL_REAL if va is not None else LABEL_GAP),
                    "label_intent": LABEL_REAL,
                    "label_demography": (LABEL_REAL if has_dem else LABEL_GAP),
                    "label_derived": LABEL_EST, "data_quality_flag": LABEL_REAL})
                for m in (d.get("monthly_volume") or []):
                    mo_rows.append({"keyword_id": ki, "keyword": k, "month": m.get("month"),
                                    "gg_volume": ff(m.get("gg")), "nv_volume": ff(m.get("nv")),
                                    "total_volume": ff(m.get("total")), "label_volume": LABEL_REAL})
                if ft:
                    frow = {"keyword_id": ki, "keyword": k}
                    for fk, fv in ft.items():
                        if fk.startswith("f_"):
                            frow[fk] = ff(fv) if isinstance(fv, (int, float)) else None
                    kf_rows.append(frow)
        self.log(f"  파싱: payload {len(payloads)} → keyword {len(km_rows)} (dedup), "
                 f"monthly {len(mo_rows)}, features {len(kf_rows)}")
        if not km_rows:
            self.log_run_step("parse_to_silver", "FAILED", note="NO_DATA: bronze payload 없음")
            raise RuntimeError("NO_DATA: bronze payload 없음 — collect 단계 먼저 실행 필요")
        self.log_run_step("parse_to_silver", "SUCCESS", rows=len(km_rows),
                          note=f"monthly={len(mo_rows)},features={len(kf_rows)}")

        # cluster_finder communities → cluster_master + membership
        # 다센터 병합: community_id 는 호출별 로컬 ID → "{center}:{cid}" 네임스페이스
        # (center = payload request_detail.keyword). 동일 네임스페이스 재등장은 스킵(멱등).
        cm_rows, mb_rows, TOP_N, seen_comm = [], [], 20, set()
        for r in self.tables.read_records("bronze_raw.cluster_finder_raw"):
            obj = json.loads(r["payload"])
            data = (obj.get("data") or {})
            center = ((obj.get("request_detail") or {}).get("keyword") or "").strip()
            prefix = f"{center}:" if center else ""
            comms = data.get("communities") or {}
            deg = {}
            for e in (data.get("rels") or []):
                for k in e:
                    deg[k] = deg.get(k, 0) + 1
            for cid, kws in comms.items():
                ns_cid = f"{prefix}{cid}"
                if ns_cid in seen_comm:
                    continue
                seen_comm.add(ns_cid)
                kws = [k for k in kws if k]
                cm_rows.append({"community_id": ns_cid,
                                "cluster_label": (kws[0] if kws else f"community_{cid}"),
                                "top_keywords": kws[:TOP_N], "keyword_count": len(kws),
                                "is_empty": (len(kws) == 0),
                                "data_quality_flag": (LABEL_REAL if kws else LABEL_GAP)})
                for k in kws:
                    mb_rows.append({"keyword_id": self.kid(k), "keyword": k,
                                    "community_id": ns_cid, "weight": float(deg.get(k, 1))})
        if cm_rows:
            self.log_run_step("parse_cluster", "SUCCESS", rows=len(cm_rows), note=f"membership={len(mb_rows)}")
        else:
            self.log("  [SKIP] cluster_finder communities 없음 (rels-only 수집)")
        return {"keywords_master": km_rows, "keyword_monthly": mo_rows,
                "keyword_features": kf_rows, "cluster_master": cm_rows, "membership": mb_rows}

    # ── [02] build_silver — expectation + ref 태깅 → silver_master.* ─────────
    def _quarantine(self, df, reason):
        rows = []
        for r in to_records(df):
            rows.append({
                **{c: r.get(c) for c in ["gl", "run_date", "category_key", "keyword_id",
                                         "keyword", "community_id", "month"]},
                "row_json": json.dumps({k: (str(v) if isinstance(v, (date, datetime)) else v)
                                        for k, v in r.items()}, ensure_ascii=False, default=str),
                "quarantine_reason": reason, "quarantined_at": now_utc(),
                "source_run_id": self.run_id})
        self.tables.append("ops.silver_quarantine", rows)
        if "ops.silver_quarantine" not in self._loaded:
            self._loaded.append("ops.silver_quarantine")
        self.log(f"  [QUARANTINE] {reason}: {len(rows)} 행 격리")

    def _apply_expectations(self, df, label_cols):
        bad_gl = df[~df["gl"].isin(ALLOWED_GL)]
        if len(bad_gl):
            raise ValueError(f"FAIL UPDATE: gl 도메인 위반 — {sorted(df['gl'].unique())}")
        good = df
        if "label_volume" in good.columns:
            null_lbl = good[good["label_volume"].isna()]
            if len(null_lbl):
                self._quarantine(null_lbl, "label_volume IS NULL")
            good = good[good["label_volume"].notna()]
        for lc in label_cols:
            domain = LABEL_DOMAIN.get(lc)
            if not domain or lc not in good.columns:
                continue
            viol = good[good[lc].notna() & ~good[lc].isin(domain)]
            if len(viol):
                self._quarantine(viol, f"{lc} 값 도메인 위반")
            good = good[good[lc].isna() | good[lc].isin(domain)]
        return good

    def _load_rule(self, table):
        df = self.tables.read_df(f"ref.{table}")
        if df.empty:
            self.log(f"  [WARN] 룰북 부재: ref.{table} — 해당 차원 태깅 생략")
            return None
        df = df[(df["gl"] == self.gl) & (df["category_key"].isin([self.cat, "*"]))]
        if "priority" in df.columns:
            df["priority"] = pd.to_numeric(df["priority"], errors="coerce")
        return df.reset_index(drop=True)

    @staticmethod
    def _rule_version(rules):
        if rules is None or "version" not in rules.columns:
            return "v0"
        v = rules["version"].dropna()
        return v.iloc[0] if len(v) else "v0"

    def _match_mask(self, kw, rule):
        """noise match_type(regex/contains/exact) 존중. brand/category 는 regex(rlike)."""
        mt = rule.get("match_type") or "regex"
        pat = rule["pattern"]
        if mt == "exact":
            return kw == pat
        if mt == "contains":
            return kw.str.contains(pat, regex=False, na=False)
        try:
            return kw.str.contains(pat, regex=True, na=False)
        except re.error:
            self.log(f"  [WARN] 잘못된 패턴 스킵: {pat!r}")
            return pd.Series(False, index=kw.index)

    def _tag_dimension(self, df, rules, rule_cols):
        """priority 내림차순·최장 패턴 우선 — 미배정 행에 순서대로 부여 (rlike 등가)."""
        for c in rule_cols:
            df[c] = None
        if rules is None or rules.empty:
            return df
        order = rules.assign(_plen=rules["pattern"].str.len()) \
                     .sort_values(["priority", "_plen"], ascending=False, na_position="last")
        unassigned = pd.Series(True, index=df.index)
        for _, rule in order.iterrows():
            if not unassigned.any():
                break
            m = self._match_mask(df["keyword"], rule) & unassigned
            if m.any():
                for c in rule_cols:
                    df.loc[m, c] = rule.get(c)
                unassigned &= ~m
        return df

    def _enrich_tags(self, df):
        brand_rules = self._load_rule("brand_rules")
        cat_rules   = self._load_rule("category_rules")
        noise_rules = self._load_rule("noise_patterns")

        # 노이즈: flag → 표시(행 유지), drop → 격리 후 제거
        df["is_noise"], df["noise_reason"] = False, None
        if noise_rules is not None:
            for _, rule in noise_rules[noise_rules["action"] == "flag"].iterrows():
                m = self._match_mask(df["keyword"], rule)
                df.loc[m & ~df["is_noise"], "noise_reason"] = rule.get("reason")
                df.loc[m, "is_noise"] = True
            drop_mask = pd.Series(False, index=df.index)
            for _, rule in noise_rules[noise_rules["action"] == "drop"].iterrows():
                drop_mask |= self._match_mask(df["keyword"], rule)
            if drop_mask.any():
                self._quarantine(df[drop_mask], "noise drop rule")
                df = df[~drop_mask].copy()

        df = self._tag_dimension(df, brand_rules, ["brand"])
        df["brand"] = df["brand"].fillna("Generic")
        df = self._tag_dimension(df, cat_rules, ["category_lvl1", "category_lvl2"])
        df["noise_filter_version"] = self._rule_version(noise_rules)
        df["brand_rules_version"]  = self._rule_version(brand_rules)
        return df

    def _series_dedup(self, km, monthly):
        """시계열이 완전히 같은 행 묶음 → (canonical_keyword, is_variant, 요약). 규칙은 series_dedup.py."""
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from series_dedup import dedup_groups
        cat_rules = self._load_rule("category_rules")
        default_segs = (set(cat_rules.loc[cat_rules["pattern"] == ".*", "category_lvl2"].dropna())
                        if cat_rules is not None and "category_lvl2" in cat_rules.columns else set())
        return dedup_groups(km, monthly, default_segments=default_segs)

    @staticmethod
    def _counted(km):
        """Gold 가 세는 행 — 노이즈 아님 · 같은 검색 중복(is_variant) 아님."""
        if not len(km):
            return km
        m = ~km["is_noise"].fillna(False).astype(bool)
        if "is_variant" in km.columns:
            m &= ~km["is_variant"].fillna(False).astype(bool)
        return km[m]

    SILVER_SPECS = [
        ("keywords_master",  ["label_volume", "label_intent", "label_demography",
                              "label_derived", "data_quality_flag"]),
        ("keyword_monthly",  ["label_volume"]),
        ("keyword_features", []),
        ("cluster_master",   ["data_quality_flag"]),
        ("membership",       []),
    ]

    def stage_build_silver(self, silver):
        self.log("[02] build_silver — expectation + ref 태깅 → silver_master.*")
        total, out = 0, {}
        try:
            for table, label_cols in self.SILVER_SPECS:
                rows = silver.get(table) or []
                if not rows:
                    self.log(f"  [SKIP] {table} 없음")
                    self.save(f"silver_master.{table}", [])
                    out[table] = pd.DataFrame(columns=[c for c, _ in SCHEMAS[f"silver_master.{table}"]])
                    continue
                df = pd.DataFrame(to_records(rows))
                df["gl"], df["run_date"], df["category_key"] = self.gl, self.run_date_dt, self.cat
                df["source_run_id"], df["collected_date"] = self.run_id, date.today()
                good = self._apply_expectations(df, label_cols)
                if table == "keywords_master":
                    good = self._enrich_tags(good.copy())
                    # 데이터 인프라 통합(2026-09-22) — 트렌드 파이프라인과의 통합 keywords_master
                    # 통합 스키마 계약.
                    # 같은 검색의 중복(시계열 완전 일치, 2026-09-29): 행은 지우지 않고 중복 행에
                    # canonical_keyword=대표·is_variant=true 를 표시한다. Gold 는 is_variant=false 만 센다.
                    good["pipeline_source"] = "mi"
                    good["canonical_keyword"], good["is_variant"], dd = self._series_dedup(
                        good, silver.get("keyword_monthly"))
                    self.log(f"  같은 검색 중복: {dd['groups']}묶음 · 중복 행 {dd['variant_rows']} "
                             f"(is_variant=true — Gold 집계 제외) [추정]")
                good["ingested_at"] = now_utc()
                total += self.save(f"silver_master.{table}", good)
                # 후속 단계(03/04)는 conform 된 정본 스키마 프레임을 본다 (테이블 재조회 등가)
                out[table] = self.tables.read_df(f"silver_master.{table}")
            self.log_run_step("build_silver", "SUCCESS", rows=total)
        except Exception as e:
            self.log_run_step("build_silver", "FAILED", note=str(e)[:500])
            raise
        return out

    # ── [03] build_gold_analytics — silver → gold 6종 ────────────────────────
    def stage_build_gold(self, sv):
        self.log("[03] build_gold_analytics — gold_analytics.* 6종")
        km  = sv["keywords_master"]
        kmo = sv["keyword_monthly"]
        mem = sv["membership"]
        # 집계 대상 = 노이즈 아님 · 같은 검색 중복 아님 — TAM·SoV·persona·opportunity 가 이 프레임만 센다
        signal = self._counted(km)
        variant = (km["is_variant"].fillna(False).astype(bool) if len(km) and "is_variant" in km.columns
                   else pd.Series(False, index=km.index))
        # 중복 행 keyword_id → 대표 행 keyword_id (journey 가 같은 검색을 한 단계로 합칠 때 쓴다)
        rep_kid = {}
        if variant.any():
            by_kw = dict(zip(km.loc[~variant, "keyword"], km.loc[~variant, "keyword_id"]))
            rep_kid = {kid: by_kw.get(ck, kid) for kid, ck in
                       zip(km.loc[variant, "keyword_id"], km.loc[variant, "canonical_keyword"])}
        stamp = {"gl": self.gl, "run_date": self.run_date_dt, "category_key": self.cat}
        counts = {}
        try:
            # 3-1 TAM — 카테고리 세그먼트(category_lvl2)별 시장규모
            # (기존: brand 축 — SOV 와 중복이고 세그먼트 관점이 골드에서 소실되던 결함. 2026-07-29 수정)
            # growth_3m 집계는 median — 신생 키워드의 극단 trend(수천 배)로 mean 이 왜곡되는 것 방지
            g = (signal.groupby("category_lvl2", dropna=False)
                 .agg(keyword_count=("keyword", "size"), total_volume=("volume_avg", "sum"),
                      growth_3m=("growth_3m", "median")).reset_index()
                 if len(signal) else pd.DataFrame(columns=["category_lvl2", "keyword_count", "total_volume", "growth_3m"]))
            gt, mx = g["total_volume"].sum(), (g["total_volume"].max() if len(g) else None)
            tam = [{**stamp, "segment": r["category_lvl2"], "keyword_count": r["keyword_count"],
                    "total_volume": r["total_volume"],
                    "volume_share_pct": round(r["total_volume"] / gt * 100, 2) if gt else None,
                    "tam_index": round(r["total_volume"] / mx * 100, 2) if mx else None,
                    "growth_3m": round(r["growth_3m"], 3) if pd.notna(r["growth_3m"]) else None,
                    "label_derived": LABEL_EST, "ingested_at": now_utc()} for _, r in g.iterrows()]
            counts["tam_analysis"] = self.save("gold_analytics.tam_analysis", tam)

            # 3-2 SOV — 세그먼트×브랜드 점유율 + 순위 (segment="ALL" = 카테고리 전체)
            # sov_pct·rank 는 세그먼트 내 기준. 하위 소비자(mart 등)는 segment=="ALL" 로 기존 뷰 유지.
            #
            # 단독 기업명 쿼리 제외 (2026-07-29): "키워드 = 기업/사명 별칭 그 자체"인 쿼리(예:
            # 주요 브랜드명)는 TV 의도인지 주가·채용·타제품 의도인지 불명이라 대형 브랜드일수록
            # 과대 측정됨 → SoV 집계에서만 제외(가정으로 명시). "<브랜드> tv"·"<브랜드> <제품>" 같은 카테고리
            # 결합형은 진짜 브랜드 수요라 유지. 기업/사명 판정 = 별칭 토큰 == 브랜드명 또는 룰 note에
            # "사명" 표기. bravia·fire tv 등 제품 별칭 단독은 카테고리 의도가 명확해 유지.
            corp_alias = set()
            br = self._load_rule("brand_rules")
            if br is not None:
                for _, rule in br.iterrows():
                    brand_lc = str(rule.get("brand") or "").strip().lower()
                    is_samyeong = "사명" in str(rule.get("note") or "")
                    for tok in str(rule.get("pattern") or "").split("|"):
                        tok = re.sub(r"\\b|\\", "", tok).replace(" ?", " ").strip().lower()
                        if tok and (tok == brand_lc or is_samyeong):
                            corp_alias.add(tok)
            sov_signal = signal
            if len(signal) and corp_alias:
                standalone = (signal["keyword"].str.strip().str.lower().isin(corp_alias)
                              & (signal["brand"] != "Generic"))
                if standalone.any():
                    excl = signal[standalone]
                    self.log(f"  sov: 단독 기업명 쿼리 {len(excl)}건 SoV 제외 — "
                             + ", ".join(f"{r['keyword']}({r['volume_avg']:,.0f}/월)"
                                         for r in to_records(excl.nlargest(5, 'volume_avg'))))
                    sov_signal = signal[~standalone]
            # Generic 분리 (2026-09-28): 브랜드 행의 sov_pct·rank 는 브랜드 검색량만 분모로 한다
            # (listeningmind-universal jobs.md 잡4 · entity-dictionary.md §3 "Generic 행은 분리 표기").
            # Generic 행은 rank=NULL 로 남기고 sov_pct 는 세그먼트 전체(브랜드+Generic) 대비 비중.
            def _sov_rows(df, seg_name):
                gg = (df.groupby("brand", dropna=False)
                      .agg(keyword_count=("keyword", "size"), total_volume=("volume_avg", "sum"))
                      .reset_index().sort_values("total_volume", ascending=False).reset_index(drop=True))
                is_gen = gg["brand"] == "Generic"
                brand_t, seg_t = gg.loc[~is_gen, "total_volume"].sum(), gg["total_volume"].sum()
                rows = []
                for i, r in enumerate(gg[~is_gen].itertuples(index=False)):
                    rows.append({**stamp, "segment": seg_name, "brand": r.brand,
                                 "sov_keyword_cnt": r.keyword_count, "sov_volume": r.total_volume,
                                 "sov_pct": round(r.total_volume / brand_t * 100, 2) if brand_t else None,
                                 "rank": i + 1, "label_derived": LABEL_EST, "ingested_at": now_utc()})
                for r in gg[is_gen].itertuples(index=False):
                    rows.append({**stamp, "segment": seg_name, "brand": r.brand,
                                 "sov_keyword_cnt": r.keyword_count, "sov_volume": r.total_volume,
                                 "sov_pct": round(r.total_volume / seg_t * 100, 2) if seg_t else None,
                                 "rank": None, "label_derived": LABEL_EST, "ingested_at": now_utc()})
                return rows
            sov = _sov_rows(sov_signal, "ALL") if len(sov_signal) else []
            if len(sov_signal):
                for seg, seg_df in sov_signal.groupby("category_lvl2", dropna=False):
                    sov += _sov_rows(seg_df, seg)
            counts["sov_analysis"] = self.save("gold_analytics.sov_analysis", sov)
            # 선두 SoV = 카테고리 전체(ALL)의 브랜드 1위 — Generic 은 rank NULL 이라 제외됨
            top_sov = next((r["sov_pct"] for r in sov
                            if r["segment"] == "ALL" and r["rank"] == 1), None)

            # 3-3 Trend — 월별 검색량 + 성장·가속 (같은 검색 중복 행은 대표 1행만 — 월 합산 이중 계상 방지)
            if len(kmo) and rep_kid:
                kmo = kmo[~kmo["keyword_id"].isin(rep_kid)]
            t = kmo.merge(km[["keyword_id", "growth_3m", "acceleration"]]
                          if len(km) else pd.DataFrame(columns=["keyword_id", "growth_3m", "acceleration"]),
                          on="keyword_id", how="left") if len(kmo) else kmo
            trend = [{**stamp, "keyword": r.get("keyword"), "month": r.get("month"),
                      "total_volume": r.get("total_volume"), "gg_volume": r.get("gg_volume"),
                      "nv_volume": r.get("nv_volume"), "growth_3m": r.get("growth_3m"),
                      "acceleration": r.get("acceleration"), "label_volume": LABEL_REAL,
                      "label_derived": LABEL_EST, "ingested_at": now_utc()} for r in to_records(t)]
            counts["trend_analysis"] = self.save("gold_analytics.trend_analysis", trend)

            # 3-4 Persona — 성별×연령대×인텐트
            # demography 는 gl=kr 전용 사양(us/jp 는 API 가 미반환, 2026-07-29 확인). 실측이 전무하면
            # 성별/연령을 기본값(M/20s)으로 조작하지 않고 인텐트 단일 축(ALL-{intent})으로만 산출한다.
            persona = []
            if len(signal):
                b = signal.copy()
                z = lambda c: b[c].fillna(0) if c in b.columns else pd.Series(0.0, index=b.index)
                has_demo = bool((z("gender_m_ratio") + z("gender_f_ratio")).gt(0).any())
                gt_p = b["volume_avg"].sum()
                if has_demo:
                    b["gender"] = (z("gender_m_ratio") >= z("gender_f_ratio")).map({True: "M", False: "F"})
                    a20, a30, a40 = z("age_20_ratio"), z("age_30_ratio"), z("age_40_ratio")
                    mxage = pd.concat([a20, a30, a40], axis=1).max(axis=1)
                    b["age_group"] = "40s"
                    b.loc[mxage == a30, "age_group"] = "30s"
                    b.loc[mxage == a20, "age_group"] = "20s"
                    keys = ["gender", "age_group", "intent_primary"]
                else:
                    self.log("  persona: demography 실측 0건 (gl=kr 전용 사양) → 인텐트 단일 축으로 산출")
                    b["gender"], b["age_group"] = None, None
                    keys = ["intent_primary"]
                for gk, grp in b.groupby(keys, dropna=False):
                    gk = gk if isinstance(gk, tuple) else (gk,)
                    gd, ag, ip = (gk[0], gk[1], gk[2]) if has_demo else (None, None, gk[0])
                    seg = f"{gd}-{ag}-{ip}" if has_demo else f"ALL-{ip}"
                    tops = grp.sort_values("volume_avg", ascending=False, na_position="last")["keyword"].head(5).tolist()
                    persona.append({**stamp,
                        "persona_segment": seg, "gender": gd, "age_group": ag,
                        "intent_primary": ip, "keyword_count": len(grp),
                        "volume_share_pct": round(grp["volume_avg"].sum() / gt_p * 100, 2) if gt_p else None,
                        "top_keywords": tops,
                        "label_demography": (grp["label_demography"].max() if has_demo else LABEL_GAP),
                        "label_intent": grp["label_intent"].max(), "ingested_at": now_utc()})
            counts["persona_analysis"] = self.save("gold_analytics.persona_analysis", persona)

            # 3-5 Journey — 클러스터 기반 퍼널 경로 + path_finder 실경로 병합
            journey = []
            stage_map = {"T": ("BOFU", 3), "C": ("MOFU", 2), "I": ("TOFU", 1), "N": ("TOFU", 1)}
            if len(mem):
                # 같은 검색 중복 → 대표 키워드 한 단계로 합친다 (한 클러스터에 대표·변형이 함께 있으면 1행)
                if rep_kid:
                    mem = mem.assign(keyword_id=mem["keyword_id"].map(lambda k: rep_kid.get(k, k)))
                    mem = mem.drop_duplicates(["community_id", "keyword_id"])
                j = mem.merge(km[["keyword_id", "keyword", "volume_avg", "intent_primary", "label_intent"]]
                              .rename(columns={"keyword": "_kw_km"}),
                              on="keyword_id", how="left")
                j["_st"] = j["intent_primary"].astype("string").str.upper().map(
                    lambda x: stage_map.get(x, ("UNKNOWN", 4)))
                j["intent_stage"] = j["_st"].map(lambda v: v[0])
                j["_ord"] = j["_st"].map(lambda v: v[1])
                j["keyword"] = j["_kw_km"].fillna(j["keyword"])
                j = j.sort_values(["community_id", "_ord", "volume_avg"],
                                  ascending=[True, True, False], na_position="last")
                j["step_order"] = j.groupby("community_id").cumcount() + 1
                journey = [{**stamp, "journey_id": r["community_id"], "step_order": r["step_order"],
                            "keyword": r["keyword"], "intent_stage": r["intent_stage"],
                            "volume_avg": r.get("volume_avg"), "label_intent": r.get("label_intent"),
                            "ingested_at": now_utc()} for r in to_records(j)]
            # path_finder 실경로 → journey_id = "path:{center}:{seq}" (스키마 무변경 병합)
            kmap = {r["keyword_id"]: r for r in to_records(
                km[["keyword_id", "volume_avg", "intent_primary", "label_intent"]])} if len(km) else {}
            n_paths = 0
            for r in self.tables.read_records("bronze_raw.path_finder_raw"):
                obj = json.loads(r["payload"])
                center = ((obj.get("request_detail") or {}).get("keyword") or "path").strip()
                for i, path in enumerate(obj.get("data") or [], 1):
                    if not isinstance(path, list):
                        continue
                    n_paths += 1
                    for step, kw in enumerate((k for k in path if k), 1):
                        m = kmap.get(self.kid(kw))
                        st = stage_map.get(str((m or {}).get("intent_primary") or "").upper(),
                                           ("UNKNOWN", 4))[0]
                        journey.append({**stamp, "journey_id": f"path:{center}:{i:03d}",
                                        "step_order": step, "keyword": kw, "intent_stage": st,
                                        "volume_avg": (m or {}).get("volume_avg"),
                                        "label_intent": (m["label_intent"] if m else LABEL_GAP),
                                        "ingested_at": now_utc()})
            if n_paths:
                self.log(f"  journey: path_finder {n_paths}경로 병합 (journey_id=path:*)")
            counts["journey_analysis"] = self.save("gold_analytics.journey_analysis", journey)

            # 3-6 Opportunity — log(volume)×(1-경쟁)×(1+성장)×인텐트가중 정규화 스코어
            # 인텐트 가중(2026-07-29 추가): 구매 신호(T/C) 우대, 시청·정보성(I/N) 감쇠 —
            # 편성표·채널 등 비구매 Informational 쿼리가 볼륨×저경쟁만으로 상위를 독식하던 결함 보정.
            INTENT_W = {"T": 1.0, "C": 0.9, "I": 0.5, "N": 0.3}
            opp = []
            s = signal[signal["volume_avg"].notna()].copy() if len(signal) else signal
            if len(s):
                # 성장 항은 [-1, 3] 캡 — 신생 키워드의 극단 trend(예: 33,100)가 정규화를 붕괴시키는 것 방지
                raw = s.apply(lambda r: (math.log(max(r["volume_avg"], 1))
                                         * (1 - (r["competition_index"] if pd.notna(r["competition_index"]) else 50) / 100.0)
                                         * (1 + min(max((r["growth_3m"] if pd.notna(r.get("growth_3m")) else 0), -1.0), 3.0))
                                         * INTENT_W.get(str(r.get("intent_primary") or "").upper(), 0.5)), axis=1)
                lo, hi = raw.min(), raw.max()
                for (_, r), rs in zip(s.iterrows(), raw):
                    opp.append({**stamp, "keyword": r["keyword"],
                        "opportunity_score": round((rs - lo) / (hi - lo) * 100, 1) if hi != lo else None,
                        "volume_avg": r["volume_avg"], "competition": r.get("competition"),
                        "competition_index": r.get("competition_index"),
                        "intent_primary": r.get("intent_primary"), "growth_3m": r.get("growth_3m"),
                        "sov_gap": round(100 - (top_sov or 0), 2),
                        "label_volume": LABEL_REAL, "label_derived": LABEL_EST, "ingested_at": now_utc()})
            counts["opportunity_analysis"] = self.save("gold_analytics.opportunity_analysis", opp)

            self.log_run_step("build_gold_analytics", "SUCCESS",
                              rows=sum(counts.values()), note=str(counts))
        except Exception as e:
            self.log_run_step("build_gold_analytics", "FAILED", note=str(e)[:500])
            raise
        return counts

    # ── [08] ingest_seed_design — seed_design/*.{parquet|csv|json} ──────────
    SEED_TABLES = ["seed_runs", "seed_keywords", "seed_communities", "seed_blind_spots"]

    def _read_seed(self, basename):
        base = os.path.join(self.pdir, "seed_design", basename)
        if os.path.exists(base + ".parquet"):
            import pyarrow.parquet as pq
            return pq.read_table(base + ".parquet").to_pylist()
        if os.path.exists(base + ".csv"):
            return to_records(pd.read_csv(base + ".csv", dtype=str, keep_default_na=False).replace({"": None}))
        if os.path.exists(base + ".json"):
            obj = json.load(open(base + ".json", encoding="utf-8"))
            return obj if isinstance(obj, list) else [obj]
        self.log(f"  [SKIP] {basename} 산출물 없음")
        return None

    def stage_ingest_seed(self):
        self.log("[08] ingest_seed_design — seed_design.*")
        total = 0
        try:
            for table in self.SEED_TABLES:
                rows = self._read_seed(table)
                if rows is None:
                    self.save(f"seed_design.{table}", [])
                    continue
                stamped = []
                for r in self.lineage(rows):
                    r.setdefault("run_id", self.run_id)
                    if r.get("run_id") is None:
                        r["run_id"] = self.run_id
                    stamped.append(r)
                total += self.save(f"seed_design.{table}", stamped)
            self.log_run_step("ingest_seed_design", "SUCCESS", rows=total)
        except Exception as e:
            self.log_run_step("ingest_seed_design", "FAILED", note=str(e)[:500])
            raise

    # ── [cost] ingest_cost — API 호출 로그 → cost.api_call_log ───────────────
    # 소스 우선순위: metadata/api_call_log.json → (부재 시) bronze payload 의
    # cost_detail 직접 파싱. api_version/user_type 은 bronze payload 에서 보강.
    def _bronze_payload_meta(self):
        """bronze payload 파싱 → 파일 basename 기준 (api, request_at, cost_detail, version, user_type, used_credits)."""
        out = {}
        for api_folder, table in self.BRONZE_SOURCES:
            for r in self.tables.read_records(f"bronze_raw.{table}"):
                try:
                    obj = json.loads(r["payload"])
                except (json.JSONDecodeError, TypeError):
                    obj = {}
                base = os.path.basename(r["source_file"] or "")
                out[base] = {
                    "endpoint": api_folder,
                    "request_at": obj.get("request_at") or r.get("request_at"),
                    "result": obj.get("result"),
                    "cost_detail": obj.get("cost_detail") or {},
                    "used_credits": obj.get("used_credits"),
                    "api_version": obj.get("api_version") or obj.get("version") or r.get("api_version"),
                    "user_type": obj.get("user_type")}
        return out

    def stage_ingest_cost(self):
        self.log("[cost] ingest_cost — API 호출 로그 → cost.api_call_log")
        meta = self._bronze_payload_meta()
        log_path = os.path.join(self.pdir, "metadata", "api_call_log.json")
        rows = []
        try:
            if os.path.exists(log_path):
                entries = json.load(open(log_path, encoding="utf-8"))
                for e in entries:
                    cd = e.get("cost_detail") or {}
                    m = meta.get(os.path.basename(e.get("file") or ""), {})
                    rows.append({
                        "endpoint": e.get("api") or m.get("endpoint"),
                        "request_at": e.get("request_at") or m.get("request_at"),
                        "result": e.get("result"),
                        **{k: cd.get(k) for k in ["input_cost", "input_count", "input_total_cost",
                                                  "output_cost", "output_count", "output_total_cost", "total_cost"]},
                        "used_credits": e.get("used_credits_snapshot") or e.get("used_credits") or m.get("used_credits"),
                        "api_version": m.get("api_version"), "user_type": m.get("user_type")})
                src = "metadata/api_call_log.json"
            else:
                for m in meta.values():
                    cd = m["cost_detail"]
                    rows.append({
                        "endpoint": m["endpoint"], "request_at": m["request_at"], "result": m.get("result"),
                        **{k: cd.get(k) for k in ["input_cost", "input_count", "input_total_cost",
                                                  "output_cost", "output_count", "output_total_cost", "total_cost"]},
                        "used_credits": m.get("used_credits"),
                        "api_version": m.get("api_version"), "user_type": m.get("user_type")})
                src = "bronze payload cost_detail (api_call_log.json 부재 폴백)"
            rows.sort(key=lambda r: (str(r.get("request_at") or ""), str(r.get("endpoint"))))
            for i, r in enumerate(rows, 1):
                r["log_id"] = f"{self.run_id}-{i:03d}-{r.get('endpoint')}"
            self.save("cost.api_call_log", self.lineage(rows))
            self.log(f"  소스: {src}")
            self.log_run_step("ingest_cost", "SUCCESS", rows=len(rows), note=src)
        except Exception as e:
            self.log_run_step("ingest_cost", "FAILED", note=str(e)[:500])
            raise
        return rows

    # ── [usage] ingest_usage — /usage/summary 스냅샷 → usage.* 4종 ───────────
    # metadata/usage_summary.json(collect 단계 저장) 우선. 부재 시 cost.api_call_log
    # 집계 폴백 — by_platform 은 소스 없음 → 0행(가짜 데이터 생성 금지).
    @staticmethod
    def _pick(d, *keys):
        for k in keys:
            if d.get(k) is not None:
                return d.get(k)
        return None

    def stage_ingest_usage(self, cost_rows):
        self.log("[usage] ingest_usage — usage.* 4종")
        us_path = os.path.join(self.pdir, "metadata", "usage_summary.json")
        try:
            if os.path.exists(us_path):
                j = json.load(open(us_path, encoding="utf-8"))
                data = j.get("data") if isinstance(j.get("data"), dict) else j
                snap = {"snapshot_date": self._pick(data, "snapshot_date") or date.today(),
                        "from_date": self._pick(data, "from_date", "from"),
                        "to_date": self._pick(data, "to_date", "to")}
                sm = data.get("summary") if isinstance(data.get("summary"), dict) else data
                daily = [{**snap, "user_type": self._pick(sm, "user_type"),
                          "total_calls": self._pick(sm, "total_calls", "calls"),
                          "error_count": self._pick(sm, "error_count", "errors"),
                          "error_rate": self._pick(sm, "error_rate"),
                          "total_credits": self._pick(sm, "total_credits", "credits")}]
                bye = [{**snap, "endpoint": self._pick(e, "endpoint", "name"),
                        "calls": e.get("calls"), "ratio": e.get("ratio"),
                        "err_4xx": e.get("err_4xx"), "rate_4xx": e.get("rate_4xx"),
                        "err_5xx": e.get("err_5xx"), "rate_5xx": e.get("rate_5xx"),
                        "error_rate": e.get("error_rate"), "credits": e.get("credits"),
                        "credits_ratio": e.get("credits_ratio")}
                       for e in (data.get("endpoints") or [])]
                byp = [{**snap, "platform": self._pick(p, "platform", "name"),
                        "calls": p.get("calls"), "ratio": p.get("ratio"),
                        "err_4xx": p.get("err_4xx"), "rate_4xx": p.get("rate_4xx"),
                        "err_5xx": p.get("err_5xx"), "rate_5xx": p.get("rate_5xx"),
                        "error_rate": p.get("error_rate"), "credits": p.get("credits"),
                        "credits_ratio": p.get("credits_ratio")}
                       for p in (data.get("platforms") or [])]
                ts = [{**snap, "ref_date": self._pick(t, "ref_date", "date"),
                       "calls": t.get("calls"), "errors": t.get("errors"), "credits": t.get("credits")}
                      for t in (data.get("timeseries") or [])]
                src = "metadata/usage_summary.json"
            else:
                # 폴백: 프로젝트 호출 로그 집계 (계정 전체가 아닌 이 프로젝트 범위)
                days = sorted({str(r["request_at"])[:10] for r in cost_rows if r.get("request_at")})
                snap = {"snapshot_date": date.fromisoformat(days[-1]) if days else self.run_date_dt,
                        "from_date": days[0] if days else None, "to_date": days[-1] if days else None}
                is_err = lambda r: (r.get("result") or "OK") != "OK"
                tot_calls = len(cost_rows)
                tot_err   = sum(1 for r in cost_rows if is_err(r))
                tot_cred  = sum(r.get("total_cost") or 0 for r in cost_rows)
                daily = [{**snap, "user_type": next((r["user_type"] for r in cost_rows if r.get("user_type")), None),
                          "total_calls": tot_calls, "error_count": tot_err,
                          "error_rate": round(tot_err / tot_calls * 100, 2) if tot_calls else None,
                          "total_credits": tot_cred}] if cost_rows else []
                bye = []
                for ep in sorted({r.get("endpoint") for r in cost_rows if r.get("endpoint")}):
                    grp = [r for r in cost_rows if r.get("endpoint") == ep]
                    err = sum(1 for r in grp if is_err(r))
                    cred = sum(r.get("total_cost") or 0 for r in grp)
                    bye.append({**snap, "endpoint": ep, "calls": len(grp),
                                "ratio": round(len(grp) / tot_calls * 100, 2) if tot_calls else None,
                                "err_4xx": None, "rate_4xx": None, "err_5xx": None, "rate_5xx": None,
                                "error_rate": round(err / len(grp) * 100, 2),
                                "credits": cred,
                                "credits_ratio": round(cred / tot_cred * 100, 2) if tot_cred else None})
                byp = []  # 소스 없음 — 가짜 플랫폼 분해 금지
                ts = []
                for d in days:
                    grp = [r for r in cost_rows if str(r.get("request_at") or "")[:10] == d]
                    ts.append({**snap, "ref_date": d, "calls": len(grp),
                               "errors": sum(1 for r in grp if is_err(r)),
                               "credits": sum(r.get("total_cost") or 0 for r in grp)})
                src = "cost.api_call_log 집계 폴백 (usage_summary.json 부재 — by_platform 0행)"
            now = now_utc()
            for rows in (daily, bye, byp, ts):
                for r in rows:
                    r["ingested_at"] = now
            n = (self.save("usage.summary_daily", daily) + self.save("usage.by_endpoint", bye)
                 + self.save("usage.by_platform", byp) + self.save("usage.timeseries", ts))
            self.log(f"  소스: {src}")
            self.log_run_step("ingest_usage", "SUCCESS", rows=n, note=src)
        except Exception as e:
            self.log_run_step("ingest_usage", "FAILED", note=str(e)[:500])
            raise

    # ── [04] run_qc — 품질 게이트 → ops.qc_results ───────────────────────────
    GOLD_TABLES = ["tam_analysis", "sov_analysis", "trend_analysis",
                   "persona_analysis", "journey_analysis", "opportunity_analysis"]

    def stage_run_qc(self, sv):
        self.log("[04] run_qc — 품질 게이트")
        km = sv["keywords_master"]
        results = []  # (check_name, passed, severity, actual, expected, detail)

        def add(name, passed, metric, threshold, severity="BLOCK", detail=""):
            results.append((name, bool(passed), severity, str(metric), str(threshold), detail))
            icon = "PASS" if passed else ("WARN" if severity == "WARN" else "FAIL")
            self.log(f"  [{icon}] {name:<20} ({severity}) metric={metric} (기준 {threshold})")

        label4 = ["label_volume", "label_intent", "label_demography", "label_derived"]
        null_cnt = {c: int(km[c].isna().sum()) if c in km.columns else len(km) for c in label4}
        total_null = sum(null_cnt.values())
        add("label_no_null", total_null == 0, total_null, "0", detail=str(null_cnt))

        dom_viol = 0
        for lc, domain in LABEL_DOMAIN.items():
            if lc in km.columns:
                dom_viol += int((km[lc].notna() & ~km[lc].isin(domain)).sum())
        add("label_domain", dom_viol == 0, dom_viol, "0")

        gl_distinct = km["gl"].nunique() if len(km) else 0
        add("single_gl", gl_distinct == 1, gl_distinct, "1")

        km_rows = len(km)
        add("silver_nonempty", km_rows > 0, km_rows, ">0")

        gold_counts = {t: self.tables.count(f"gold_analytics.{t}") for t in self.GOLD_TABLES}
        missing = [t for t, c in gold_counts.items() if c == 0]
        add("gold_complete", len(missing) == 0,
            f"{len(self.GOLD_TABLES) - len(missing)}/{len(self.GOLD_TABLES)}", "6/6", detail=str(gold_counts))

        # SoV 순위에 Generic 이 끼면 설계 위반(브랜드 검색 대비 점유) — 헤드라인·sov_gap 이 오염된다
        sov_df = self.tables.read_df("gold_analytics.sov_analysis")
        gen_ranked = (sov_df[(sov_df["brand"] == "Generic") & sov_df["rank"].notna()]
                      if len(sov_df) else sov_df)
        add("sov_generic_unranked", len(gen_ranked) == 0, len(gen_ranked), "0",
            detail=("Generic 이 순위에 포함된 세그먼트: "
                    + ", ".join(str(s) for s in gen_ranked["segment"].unique()[:5]))
                   if len(gen_ranked) else "")

        # 멱등성 — 직전 qc_snapshot 과 행수·검색량 합계 비교
        cur_sum = km["volume_avg"].sum() if "volume_avg" in km.columns else 0
        cur_snapshot = f"rows={km_rows};vol_sum={int(cur_sum or 0)}"
        prev = [r for r in self.tables.read_records("ops.pipeline_runs")
                if r["phase"] == "qc_snapshot" and r["gl"] == self.gl
                and r["run_date"] == self.run_date_dt and r["category_key"] == self.cat]
        if prev:
            prev_metric = sorted(prev, key=lambda r: r["ingested_at"])[-1]["error_message"]
            add("idempotency_hash", prev_metric == cur_snapshot, cur_snapshot, prev_metric,
                detail="직전 실행 대비 일치 여부")
        else:
            add("idempotency_hash", True, cur_snapshot, "최초실행(기준없음)", detail="최초 실행 — baseline 기록")
        self.log_run_step("qc_snapshot", "SNAPSHOT", note=cur_snapshot)

        pair_viol = int(((km["volume_avg"].notna() & km["label_volume"].isna()) |
                         (km["intent_i"].notna() & km["label_intent"].isna())).sum()) if len(km) else 0
        add("numeric_has_label", pair_viol == 0, pair_viol, "0")

        has_tags = all(c in km.columns for c in ["is_noise", "brand", "category_lvl1"])
        if not has_tags:
            add("category_tagged", False, "태깅 컬럼 없음", "존재", severity="WARN",
                detail="02 태깅 스테이지 미적용 빌드")
        else:
            null_cat   = int(km["category_lvl1"].isna().sum())
            noise_rows = int(km["is_noise"].fillna(False).astype(bool).sum())
            nonnoise   = km[~km["is_noise"].fillna(False).astype(bool)]
            sig        = self._counted(km)          # Gold 집계와 같은 기준(중복 제외)
            vol_nonnoise = nonnoise["volume_avg"].sum() or 0
            vol_signal = sig["volume_avg"].sum() or 0
            vol_generic = sig.loc[sig["brand"] == "Generic", "volume_avg"].sum() or 0
            noise_pct = round(noise_rows / max(km_rows, 1) * 100, 1)
            gen_pct   = round(vol_generic / max(vol_signal, 1) * 100, 1)
            add("category_tagged", null_cat == 0, null_cat, "0")
            add("noise_not_total", noise_pct < 95.0, f"{noise_pct}%", "< 95%")
            add("noise_pct", noise_pct <= self.qc_noise_pct_max, f"{noise_pct}%",
                f"<= {self.qc_noise_pct_max}%", severity="WARN",
                detail="노이즈 과다 — 룰북 점검 권장" if noise_pct > self.qc_noise_pct_max else "")
            # 볼륨 기준 노이즈 비율 — 키워드수 기준(noise_pct)이 볼륨 오염을 과소평가하는 것 탐지
            # (실측 사례: 키워드수 27% vs 볼륨 ~93% — 시청·스트리밍 쿼리의 대용량 편중)
            vol_total = km["volume_avg"].sum() or 0
            noise_vol_pct = round((vol_total - vol_nonnoise) / max(vol_total, 1) * 100, 1)
            add("noise_volume_pct", noise_vol_pct <= 50.0, f"{noise_vol_pct}%", "<= 50%",
                severity="WARN",
                detail="볼륨 과반이 노이즈 — 총량 지표는 반드시 노이즈 제외 기준으로 산출·해석할 것"
                       if noise_vol_pct > 50.0 else "")
            add("generic_brand_pct", gen_pct <= self.qc_generic_pct_max, f"{gen_pct}%",
                f"<= {self.qc_generic_pct_max}%", severity="WARN",
                detail="미태깅 고검색량 다수 — brand_rules 보강 권장" if gen_pct > self.qc_generic_pct_max else "")
            # 같은 검색 중복 볼륨 비중 — 노이즈 아닌 볼륨 중 is_variant 행 몫 (Gold 는 이미 제외하고 센다).
            # 높으면 Silver 를 직접 합산하는 소비자가 is_variant=false 필터 없이 쓸 때 그만큼 부풀려진다.
            dup_pct = round((vol_nonnoise - vol_signal) / max(vol_nonnoise, 1) * 100, 1)
            n_var = int(km["is_variant"].fillna(False).astype(bool).sum()) if "is_variant" in km.columns else 0
            add("series_dup_volume_pct", dup_pct <= self.qc_dup_volume_pct_max, f"{dup_pct}%",
                f"<= {self.qc_dup_volume_pct_max}%", severity="WARN",
                detail=(f"같은 검색 중복 {n_var}행 — Gold 는 대표 1행만 집계. Silver 직접 합산 시 "
                        f"is_variant=false 필터 필수" if dup_pct > self.qc_dup_volume_pct_max
                        else f"같은 검색 중복 {n_var}행"))

        # 커버리지 (08 적재분) — WARN
        sr = [r for r in self.tables.read_records("seed_design.seed_runs")
              if r["gl"] == self.gl and r["market"] == self.cat]
        covs = [r["coverage_pct"] for r in sr if r.get("coverage_pct") is not None]
        if covs:
            cov = max(covs)
            bs_cnt = len([r for r in self.tables.read_records("seed_design.seed_blind_spots")
                          if r["gl"] == self.gl and r["market"] == self.cat])
            add("coverage_pct", cov >= self.qc_coverage_pct_min, f"{round(cov, 1)}%",
                f">= {self.qc_coverage_pct_min}%", severity="WARN",
                detail=(f"커버리지 미달 — 업스트림 재수집 권장 (Blind Spot {bs_cnt}건)"
                        if cov < self.qc_coverage_pct_min else f"Blind Spot {bs_cnt}건"))
        else:
            self.log("  [INFO] seed_runs coverage_pct 없음 — 커버리지 체크 생략")

        # 클러스터 볼륨 커버리지 — membership 이 keywords_master 볼륨을 얼마나 덮는지 (WARN)
        # 분모는 L4(인접)·L5(수리) 시드 계열 제외: seed_keywords L4/L5 키워드를 포함하는 키워드 제외 (휴리스틱)
        # label=추정 행은 쓰지 않는다 — --seeds 만 쓴 run 은 L1·L2 밖 유니버스 전체가 추정 L5 라 분모가 비워진다 (2026-09-29)
        mem = sv.get("membership")
        if km is not None and len(km) and km["volume_avg"].notna().any():
            # 분모·분자 모두 노이즈·중복 제외 — Gold 집계(is_noise=false · is_variant=false)와 동일 기준
            base = self._counted(km)
            base = base[base["volume_avg"].notna()]
            seed_l45 = [r for r in self.tables.read_records("seed_design.seed_keywords")
                        if r.get("layer") in ("L4", "L5") and r.get("gl") == self.gl
                        and r.get("market") == self.cat and r.get("keyword")]
            l45 = [r["keyword"] for r in seed_l45 if r.get("label") != "추정"]
            if len(l45) < len(seed_l45):
                self.log(f"  [INFO] 커버리지 분모 제외 시드: 추정 레이어 {len(seed_l45) - len(l45)}행 무시 "
                         f"(실측 L4/L5 {len(l45)}행만 사용)")
            if l45:
                pat = "|".join(re.escape(k) for k in l45)
                base = base[~base["keyword"].str.contains(pat, regex=True, na=False)]
            denom = base["volume_avg"].sum()
            mem_ids = set(mem["keyword_id"]) if mem is not None and len(mem) else set()
            # 중복 행만 클러스터에 속해도 그 검색(대표)은 덮인 것으로 본다
            ck = km["canonical_keyword"] if "canonical_keyword" in km.columns else km["keyword"]
            mem_canon = set(ck[km["keyword_id"].isin(mem_ids)])
            covered = base[base["keyword"].isin(mem_canon)]["volume_avg"].sum()
            ccov = round(covered / denom * 100, 1) if denom else 0.0
            add("cluster_coverage_volume_pct", ccov >= self.qc_cluster_coverage_pct_min,
                f"{ccov}%", f">= {self.qc_cluster_coverage_pct_min}%", severity="WARN",
                detail=("클러스터 센터 부족 — 미커버 볼륨 상위 sub_group 대표로 --cluster-centers 보강 권장"
                        if ccov < self.qc_cluster_coverage_pct_min else ""))

        qc_rows = [{"qc_id": f"{self.run_id}-{name}", "run_id": self.run_id, "gl": self.gl,
                    "run_date": self.run_date_dt, "category_key": self.cat, "check_name": name,
                    "passed": passed, "expected": expected, "actual": actual,
                    "severity": severity, "checked_at": now_utc()}
                   for (name, passed, severity, actual, expected, _) in results]
        self.tables.append("ops.qc_results", qc_rows)
        if "ops.qc_results" not in self._loaded:
            self._loaded.append("ops.qc_results")

        blocked  = [r[0] for r in results if not r[1] and r[2] == "BLOCK"]
        warnings = [r[0] for r in results if not r[1] and r[2] == "WARN"]
        if warnings:
            self.log(f"  ⚠️ WARN (비차단): {warnings}")
        if blocked:
            self.log_run_step("run_qc", "FAILED", note=f"blocked={blocked}; warnings={warnings}")
            raise QCBlocked(f"QC 게이트 차단: BLOCK 체크 실패 — {blocked}")
        self.log_run_step("run_qc", "SUCCESS", rows=len(results),
                          note=f"overall.passed=True; warnings={warnings}")

    # ── [05] publish_gold_reports — 보고서 파일 메타/섹션 ─────────────────────
    @staticmethod
    def _split_markdown(text):
        sections, cur = [], {"level": 0, "title": "_preamble", "body": []}
        for line in text.splitlines():
            m = re.match(r"^(#{1,3})\s+(.*)$", line)
            if m:
                if cur["body"] or cur["title"] != "_preamble":
                    sections.append(cur)
                cur = {"level": len(m.group(1)), "title": m.group(2).strip(), "body": []}
            else:
                cur["body"].append(line)
        sections.append(cur)
        out = []
        for i, s in enumerate(sections):
            body = "\n".join(s["body"]).strip()
            if s["title"] == "_preamble" and not body:
                continue
            out.append((i, s["level"], s["title"], body))
        return out

    def stage_publish_reports(self):
        self.log("[05] publish_gold_reports — gold_reports.*")
        root = os.path.join(self.pdir, "gold_reports")
        files = []
        for dirpath, _, names in os.walk(root):
            for n in names:
                ext = n.rsplit(".", 1)[-1].lower() if "." in n else ""
                if ext in ("docx", "md", "csv"):
                    p = os.path.join(dirpath, n)
                    files.append((p, n, ext, os.path.getsize(p)))
        try:
            meta = [{"report_id": f"{self.run_id}-{n}", "report_type": ext, "title": n,
                     "s3_path": p.replace("\\", "/"), "file_format": ext,
                     "file_size_kb": sz // 1024, "created_at": now_utc()}
                    for (p, n, ext, sz) in files]
            n_meta = self.save("gold_reports.report_meta", self.lineage(meta))
            secs = []
            for (p, n, ext, _) in files:
                if ext != "md":
                    continue
                for (order, level, title, body) in self._split_markdown(open(p, encoding="utf-8").read()):
                    secs.append({"report_id": f"{self.run_id}-{n}", "section_order": order,
                                 "section_title": title, "content_md": body})
            n_sec = self.save("gold_reports.report_sections", self.lineage(secs))
            self.log_run_step("publish_gold_reports", "SUCCESS", rows=n_meta, note=f"sections={n_sec}")
        except Exception as e:
            self.log_run_step("publish_gold_reports", "FAILED", note=str(e)[:500])
            raise

    # ── [mart] dashboard_mart — gold 6종 헤드라인 KPI 1행 ───────────────────
    def stage_build_mart(self):
        self.log("[mart] dashboard_mart — 카테고리 헤드라인 KPI")
        tam = self.tables.read_df("gold_analytics.tam_analysis")
        sov = self.tables.read_df("gold_analytics.sov_analysis")
        opp = self.tables.read_df("gold_analytics.opportunity_analysis")
        per = self.tables.read_df("gold_analytics.persona_analysis")
        if tam.empty:
            self.save("gold_analytics.dashboard_mart", [])
            return
        # SOV 는 세그먼트×브랜드 그레인 — 마트 헤드라인은 카테고리 전체(segment=="ALL") 기준
        if len(sov) and "segment" in sov.columns:
            sov = sov[sov["segment"] == "ALL"]
        top  = sov[sov["rank"] == 1].head(1) if len(sov) else sov
        # 관심·경쟁 브랜드는 run 설정(focus_brand·rival_brand) — 없으면 비워 둔다(특정 브랜드를 코드에 박지 않는다)
        fb   = sov[sov["brand"] == self.focus_brand] if (len(sov) and self.focus_brand) else sov.iloc[0:0]
        rb   = sov[sov["brand"] == self.rival_brand] if (len(sov) and self.rival_brand) else sov.iloc[0:0]
        opp1 = opp.sort_values("opportunity_score", ascending=False).head(1) if len(opp) else opp
        per1 = per.sort_values("volume_share_pct", ascending=False).head(1) if len(per) else per
        first = lambda df, c: (df.iloc[0][c] if len(df) and pd.notna(df.iloc[0][c]) else None)
        row = {
            "gl": self.gl, "run_date": self.run_date_dt, "category_key": self.cat,
            "category_name": os.environ.get("LM_CATEGORY_NAME") or CAT_NAME.get(self.cat),   # 프로젝트 설정이 먼저
            "tam_total_volume": tam["total_volume"].sum(),
            "tam_index": tam["tam_index"].max(),
            "keyword_count": tam["keyword_count"].sum(),
            "growth_3m": tam["growth_3m"].mean() if tam["growth_3m"].notna().any() else None,
            "top_brand": first(top, "brand"), "top_brand_sov_pct": first(top, "sov_pct"),
            "focus_brand": self.focus_brand, "focus_brand_sov_pct": fb["sov_pct"].max() if len(fb) else None,
            "rival_brand": self.rival_brand, "rival_brand_sov_pct": rb["sov_pct"].max() if len(rb) else None,
            "top_opportunity_keyword": first(opp1, "keyword"),
            "top_opportunity_score": first(opp1, "opportunity_score"),
            "persona_primary": first(per1, "persona_segment"),
            "primary_intent": first(per1, "intent_primary"),
            # volume 레이블은 silver 집계 시 반영(플레이스홀더) — ddl_dashboard_share.sql 동일
            "label_volume": "ㅤ",
            "label_derived": first(tam, "label_derived"),
            "label_demography": first(per1, "label_demography"),
            "label_intent": first(per1, "label_intent"),
            "data_quality_flag": "ㅤ",
            "ingested_at": now_utc()}
        self.save("gold_analytics.dashboard_mart", [row])
        self.log_run_step("build_mart", "SUCCESS", rows=1)

    # ── data_freshness + 매니페스트 ──────────────────────────────────────────
    def finalize_freshness(self):
        rows = {(fqn.split(".")[0], fqn.split(".")[1]):
                {"schema_name": fqn.split(".")[0], "table_name": fqn.split(".")[1],
                 "gl": self.gl, "latest_run_date": self.run_date_dt,
                 "row_count": self.tables.count(fqn), "checked_at": now_utc()}
                for fqn in self._loaded}
        self.tables.write("ops.data_freshness", list(rows.values()))
        if "ops.data_freshness" not in self._loaded:
            self._loaded.append("ops.data_freshness")
        # 카탈로그 이름 = 프로젝트명 — 테이블 FQN 은 {catalog}.{schema}.{table}
        manifest = {"catalog": self.proj, "project": self.proj, "run_id": self.run_id,
                    "generated_at": now_utc().isoformat(),
                    "tables": {f"{self.proj}.{fqn}": self.tables.count(fqn)
                               for fqn in sorted(set(self._loaded))}}
        with open(os.path.join(self.tables.base, "_manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        self.log(f"  ✔ ops.data_freshness {len(rows)} 테이블 스냅샷 + _manifest.json (catalog={self.proj})")

    # ── 전체 실행 ────────────────────────────────────────────────────────────
    def run(self):
        self.log(f"=== 로컬 적재 시작: {self.proj} (run_id={self.run_id}) ===")
        try:
            self.stage_load_ref()
            self.stage_ingest_bronze()
            silver_frames = self.stage_parse_to_silver()
            sv = self.stage_build_silver(silver_frames)
            self.stage_build_gold(sv)
            self.stage_ingest_seed()
            cost_rows = self.stage_ingest_cost()
            self.stage_ingest_usage(cost_rows)
            self.stage_run_qc(sv)          # BLOCK 실패 → 이후 단계 차단
            self.stage_publish_reports()
            self.stage_build_mart()
        finally:
            self.finalize_freshness()
        self.log(f"=== 로컬 적재 완료: {self.pdir}\\tables ===")


def run(gl, category_key, run_date, **kw):
    Engine(gl, category_key, run_date, **kw).run()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="로컬 적재 엔진 (Databricks 미사용)")
    ap.add_argument("--gl", default="kr")
    ap.add_argument("--category", required=True)
    ap.add_argument("--run-date", required=True, help="YYYY-MM")
    ap.add_argument("--focus-brand", help="마트 헤드라인의 관심 브랜드(브랜드 룰북 이름) — LM_FOCUS_BRAND 로도 지정")
    ap.add_argument("--rival-brand", help="마트 헤드라인의 경쟁 브랜드 — LM_RIVAL_BRAND 로도 지정")
    a = ap.parse_args()
    run(a.gl, a.category, a.run_date, focus_brand=a.focus_brand, rival_brand=a.rival_brand)
