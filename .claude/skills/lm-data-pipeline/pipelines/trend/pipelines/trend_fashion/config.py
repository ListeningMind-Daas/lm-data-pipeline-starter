#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py — trend_fashion 파이프라인 중앙 설정 (STAGE 00)

모든 번호순 스테이지(01~08)가 import 하는 단일 소스.
새 프로젝트/시장은 환경변수만 바꿔 동일 골격을 재현한다:

    TREND_FASHION_GL=kr TREND_FASHION_RUN_DATE=2026-07-22 python3 run_all.py

재현성 원칙:
- 경로/임계값/스키마 컬럼을 코드에 흩지 않고 여기서만 정의.
- 스키마 컬럼 순서는 ref/trend_fashion_schema_table_all.csv(단일 소스)에서 로드.
- ref_version = ref/ 전체 content hash 12자 → RUN_MANIFEST에 기록(어느 룰북으로 생성됐는지 추적).
"""
import os
import csv
import hashlib
from pathlib import Path
from collections import defaultdict

# ── 런 식별 (환경변수 우선, 기본값 = 예시 run) ────────────────
GL            = os.getenv("TREND_FASHION_GL", "kr")
CATEGORY_KEY  = os.getenv("TREND_FASHION_CATEGORY", "trend_fashion")
RUN_DATE      = os.getenv("TREND_FASHION_RUN_DATE", "2026-07-22")
RUN_ID        = f"{GL}_{CATEGORY_KEY}_{RUN_DATE}"

# ── 경로 ─────────────────────────────────────────────────────
REPO   = Path(__file__).resolve().parent.parent.parent
REF    = REPO / "ref"
# run 폴더 루트 — 기본은 trend/projects. 스타터 워크스페이스는 90_Output/<프로젝트> 로 돌린다
RUN    = Path(os.getenv("LM_PROJECTS_ROOT") or (REPO / "projects")) / RUN_ID
BRONZE = RUN / "bronze"
SILVER = RUN / "silver"
GOLD   = RUN / "gold"
COST   = RUN / "cost"
META   = RUN / "meta"
FRAME_DESIGN = RUN / "frame_design"

def ensure_dirs():
    for d in (BRONZE, SILVER, GOLD, COST, META, FRAME_DESIGN):
        d.mkdir(parents=True, exist_ok=True)

# ── ref 룰북 파일 ────────────────────────────────────────────
FRAME_JSON      = REF / "frame" / f"{CATEGORY_KEY}_kr_frame.json"
DIMENSIONS_JSON = REF / "dimensions" / f"{CATEGORY_KEY}_dimensions.json"
BRAND_JSON      = REF / "brand_rules" / f"{CATEGORY_KEY}.json"
CATEGORY_JSON   = REF / "category_rules" / f"{CATEGORY_KEY}.json"
NOISE_JSON      = REF / "noise_patterns" / "common.json"
SCHEMA_DUMP     = REF / "trend_fashion_schema_table_all.csv"

# ── ListeningMind DaaS Data API (API Key 직접 호출) ─
# 라이브 스펙: https://listeningmind-data-api.ascentlab.io/openapi.json (2026-10-30 확인 v1.0.15)
# 인증: LM-API-KEY 헤더. 키는 환경변수 LM_API_KEY (커밋 금지).
LM_API_BASE   = os.getenv("LM_API_BASE", "https://listeningmind-data-api.ascentlab.io")
LM_API_KEY    = os.getenv("LM_API_KEY", "")
LM_TIMEOUT    = int(os.getenv("LM_API_TIMEOUT", "60"))
LM_API_RETRIES = int(os.getenv("LM_API_RETRIES", "1"))   # 실패/FAILED 재시도 횟수 (수집 규약 §재시도)
LM_API_BACKOFF = float(os.getenv("LM_API_BACKOFF", "2.0"))  # 재시도 간 대기(초)
KEYWORD_INFO_BATCH = 1000   # /keyword_info 최대 1,000개/콜
INTENT_MAX_SEEDS   = 100    # /intent_finder 최대 입력 100개
L1_LIMIT_PER_NODE  = 150    # 노드당 후보 상한 (ref/frame l1_expansion_policy)
L1_VOLUME_THRESHOLD = 100

# ── 속도 게이팅 임계값 (ref/frame gating_policy 계승) ─────────
GATE = {
    "STABLE_MIN_VOL":   300,     # vol_avg >= 300
    "EMERGING_MIN_VOL":  50,     # vol_avg >= 50 AND |trend| >= 0.3
    "EMERGING_MIN_TREND": 0.3,
    "BREAKOUT_MIN_TREND": 0.5,   # trend >= 0.5 AND vol_avg >= 100
    "BREAKOUT_MIN_VOL":  100,
}

# ── trend winsorize 범위 (API 아티팩트 집계 왜곡 방지) ───────
TREND_WINSOR = (-1.0, 3.0)

# ── 동음이의 비패션 노이즈 규칙 (07_clean_noise) ─────────────
# 토큰/접미사 단위 — substring 오탐 방지. '에피소드'는 패션 브랜드명이라 숫자+화 정규식만 사용.
NOISE_TOKENS = {
    "번역": "번역", "ost": "음악", "bgm": "음악", "가사": "음악",
    "코인": "금융", "etf": "금융", "주식": "금융", "나스닥": "금융", "배당": "금융", "비트코인": "금융",
    "다시보기": "불법", "토렌트": "불법",
    "대본": "애니", "대본집": "애니", "결말": "애니", "줄거리": "애니", "스포일러": "애니",
    "만렙": "게임", "공략": "게임", "길드": "게임",
}
NOISE_PHRASES = {"이층버스": "교통"}
NOISE_SUFFIX  = ("뜻", "유래")          # 사전 질의
NOISE_EPISODE = r"\d+\s*화\b"           # 에피소드 번호

# ── 26 테이블 카탈로그 (스키마 → 테이블 → 출력 폴더) ────────
CATALOG = {
    "BRONZE": {"RAW_API_RESPONSES": BRONZE},
    "SILVER": {t: SILVER for t in
               ["KEYWORDS_MASTER", "KEYWORDS_MONTHLY", "KEYWORDS_FEATURES",
                "KEYWORDS_TAGGED", "KEYWORDS_EXPANDED", "BRAND_MASTER"]},
    "GOLD":   {t: GOLD for t in
               ["TREND_RANKING", "TREND_MONITOR", "KEYWORD_SCORECARD", "MONTHLY_TREND",
                "EXPANSION_SCORECARD", "DISCOVERY_CANDIDATES_V2", "DISCOVERY_GATED",
                "NEW_TREND_CANDIDATES", "UNKNOWN_UNKNOWNS", "COVERAGE_GAP_AUDIT",
                "DIMENSION_CROSSTAB", "GOLD_TAG_MOMENTUM", "BRAND_SOV", "BRAND_MONITOR",
                "BRAND_TREND_LINK"]},
    "COST":   {t: COST for t in ["API_CALL_LOG", "COST_SUMMARY"]},
    "META":   {t: META for t in ["RUN_MANIFEST", "SEED_DESIGN"]},
}

def schema_columns(table):
    """스키마 덤프에서 테이블의 컬럼을 ORDINAL_POSITION 순으로 반환 (단일 소스)."""
    cols = defaultdict(list)
    with open(SCHEMA_DUMP, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            cols[r["TABLE_NAME"]].append((int(r["ORDINAL_POSITION"]), r["COLUMN_NAME"], r["DATA_TYPE"]))
    return [(c, t) for _, c, t in sorted(cols.get(table, []))]

def ref_version():
    """ref/ 전체 content hash 앞 12자 — 재현성 지문."""
    h = hashlib.sha256()
    for p in sorted(REF.rglob("*")):
        if p.is_file() and not p.name.startswith("."):
            h.update(p.relative_to(REF).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()[:12]

# ── 공용 유틸 ────────────────────────────────────────────────
import re
def norm(k):
    return re.sub(r"\s+", "", k or "")

def winsor(v):
    try: t = float(v)
    except (TypeError, ValueError): return 0.0
    lo, hi = TREND_WINSOR
    return hi if t > hi else (lo if t < lo else t)

def write_csv(path, columns, rows):
    """스키마 컬럼 순서대로 CSV 기록 (utf-8-sig)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})
    return len(rows)

if __name__ == "__main__":
    print(f"RUN_ID       : {RUN_ID}")
    print(f"REPO         : {REPO}")
    print(f"RUN dir      : {RUN}")
    print(f"ref_version  : {ref_version()}")
    print(f"catalog      : {sum(len(v) for v in CATALOG.values())} tables")
    print(f"schema dump  : {'OK' if SCHEMA_DUMP.exists() else 'MISSING'}")
