#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rebuild_monthly.py — 저장된 bronze/raw 에서 silver/KEYWORDS_MONTHLY 만 다시 만든다 (0크레딧 · 보수 도구).

언제: 05 가 monthly_volume 을 잘못 읽은 채로 Silver 가 만들어진 런(예: 월 검색량이 모두 0 인 런 —
모든 월 검색량이 0, 같은 월이 원본 파일 수만큼 반복)을 05→07 전체 재실행 없이 고칠 때.
05 를 다시 돌리면 KEYWORDS_MASTER·FEATURES 까지 새로 써지고 07·06 을 다시 거쳐야 하므로,
이 도구는 MONTHLY 한 파일만 바꾼다.

규칙:
  - 월 값 해석·중복 제거는 05 와 같은 monthly.py 를 쓴다 (없는 값 = null, nv_code 만 있는 달 = null).
  - 대상 키워드 = 현재 silver/KEYWORDS_MASTER 의 keyword (07 노이즈 정제가 끝난 집합과 같다).
  - canonical_keyword·segment 는 KEYWORDS_MASTER 행의 값을 그대로 쓴다 (테이블 간 정합).

이어서 08 을 돌리면 MONTHLY_TREND 가 새 MONTHLY 로 다시 집계된다:
    python3 rebuild_monthly.py && python3 run_all.py --stage 08
"""
import csv, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
import monthly

SILVER = config.SILVER
MONTHLY = SILVER / "KEYWORDS_MONTHLY.csv"


def count_positive(path):
    n = rows = 0
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows += 1
            try:
                n += float(r["total_volume"]) > 0
            except (TypeError, ValueError):
                pass
    return rows, n


print("=" * 70)
print(f"REBUILD KEYWORDS_MONTHLY — {config.RUN_ID}")
print("=" * 70)

before = count_positive(MONTHLY) if MONTHLY.exists() else (0, 0)
with open(SILVER / "KEYWORDS_MASTER.csv", encoding="utf-8-sig") as f:
    master = {r["keyword"]: r for r in csv.DictReader(f)}

rows, stats = monthly.extract_monthly(config.BRONZE / "raw", gl=config.GL)
out = []
for (kw, _), r in rows.items():
    m = master.get(kw)
    if m is None:
        continue
    r["canonical_keyword"] = m.get("canonical_keyword") or kw
    r["segment"] = m.get("segment", "")
    out.append(r)
monthly.write_monthly(MONTHLY, out)
after = count_positive(MONTHLY)

no_monthly = len(set(master) - {r["keyword"] for r in out})
print(f"  원본: 키워드 항목 {stats['keyword_entries']:,} · 중복 월 {stats['duplicate_months']:,} 제거 · "
      f"값 불일치 {stats['conflicts']} · 지표 없는 파일 {stats['no_payload_files']}")
print(f"  MASTER 키워드 {len(master):,} 중 월 시계열 없음 {no_monthly}")
print(f"  전: {before[0]:,}행 · total_volume > 0 {before[1]:,}행")
print(f"  후: {after[0]:,}행 · total_volume > 0 {after[1]:,}행 · "
      f"total_volume null {sum(1 for r in out if r['total_volume'] is None):,}행")
