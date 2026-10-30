#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monthly.py — keyword_info L3 원본의 monthly_volume → KEYWORDS_MONTHLY 행 (05·rebuild_monthly 공용)

원본 레이아웃 (bronze/raw 실측):
  ① 맨 리스트          [ {keyword, monthly_volume, ...}, ... ]
  ② dict + data[]      {"result", "cost_detail", "data": [ {...}, ... ], ...}
  ③ 키워드 목록만 남은 기록 {"data": {"keywords": [...], "params": ...}} · {"response_keywords": [...]}
     — 지표가 없다. 건너뛰고 개수만 센다(값을 지어내지 않는다).

월 항목 표기 규칙:
  - gg·nv·total 은 원본에 있는 값만 쓴다. 키가 없으면 0 이 아니라 **빈 값(null)** 이다.
  - {"month", "nv_code": 1} 또는 {"month", "gg", "nv_code": 1}: 네이버 월 검색량이 제공되지 않은 달.
    nv_volume = null, total_volume = null (gg 만 알아도 합계는 모른다), gg_volume 은 있으면 그 값.
    0 으로 채우면 "검색이 없었다"와 "값이 없다"가 섞여 시계열·합산이 왜곡된다.
  - 같은 키워드가 재시도·복사본으로 여러 파일에 들어 있다 → (keyword, month) 한 행만 남긴다.
    먼저 읽은 파일(파일명 정렬순)이 이긴다. 값이 서로 다르면 conflicts 로 센다(실측 0건).
"""
import gzip
import json


def iter_l3_items(raw_dir, stats=None):
    """bronze/raw 의 keyword_info L3 파일에서 키워드 항목(dict)을 파일명 순으로 낸다."""
    for f in sorted(raw_dir.glob("call_*_keyword_info_l3*.json.gz")):
        try:
            d = json.load(gzip.open(f, "rt", encoding="utf-8"))
        except Exception:
            if stats is not None:
                stats["unreadable_files"] += 1
            continue
        items = d if isinstance(d, list) else (d.get("data") if isinstance(d, dict) else None)
        if not isinstance(items, list):
            if stats is not None:
                stats["no_payload_files"] += 1
            continue
        for item in items:
            if isinstance(item, dict) and (item.get("keyword") or "").strip():
                yield f, item


def _num(entry, *keys):
    for k in keys:
        v = entry.get(k)
        if v is not None and v != "":
            return v
    return None


def month_values(entry):
    """월 항목 1개 → (gg, nv, total). 없는 값은 None (nv_code 만 있는 달도 여기서 None 이 된다)."""
    return (_num(entry, "gg", "gg_volume"), _num(entry, "nv", "nv_volume"),
            _num(entry, "total", "volume"))


def extract_monthly(raw_dir, segment_of=lambda kw: "", gl="kr"):
    """(keyword, month) 단위로 중복을 없앤 KEYWORDS_MONTHLY 행과 통계를 돌려준다."""
    from collections import Counter
    stats = Counter()
    rows = {}
    for _, item in iter_l3_items(raw_dir, stats):
        kw = item["keyword"].strip()
        mv = item.get("monthly_volume") or []
        if mv:
            stats["keyword_entries"] += 1
        for entry in mv:
            month = entry.get("month", "")
            if not month:
                continue
            gg, nv, total = month_values(entry)
            if "nv_code" in entry:
                stats["nv_code_months"] += 1
            key = (kw, month)
            if key in rows:
                stats["duplicate_months"] += 1
                prev = rows[key]
                if (prev["gg_volume"], prev["nv_volume"], prev["total_volume"]) != (gg, nv, total):
                    stats["conflicts"] += 1
                continue
            rows[key] = {"canonical_keyword": kw, "segment": segment_of(kw), "keyword": kw, "gl": gl,
                         "month": month, "gg_volume": gg, "nv_volume": nv, "total_volume": total}
    return rows, stats


MONTHLY_COLS = ["canonical_keyword", "segment", "keyword", "gl", "month", "gg_volume", "nv_volume", "total_volume"]


def write_monthly(path, rows):
    import csv
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=MONTHLY_COLS)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["canonical_keyword"], x["month"])):
            w.writerow({c: ("" if r.get(c) is None else r[c]) for c in MONTHLY_COLS})
