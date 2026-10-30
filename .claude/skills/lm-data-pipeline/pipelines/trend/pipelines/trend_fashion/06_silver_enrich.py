#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
누락된 SILVER 테이블 3종 생성 (적재 스키마와 컬럼 일치)

  - KEYWORDS_TAGGED    (SOURCE, CANONICAL, KEYWORD, DIM_BODY, DIM_OCCASION, DIM_MODIFIER, DIM_GENDER, DIM_SEASON)
  - KEYWORDS_EXPANDED  (CANONICAL_KEYWORD, TYPE, DIMENSION, VOLUME_TOTAL, VOLUME_AVG, VOLUME_TREND_WAVG, NV_SHARE, VARIANTS)
  - BRAND_MASTER       (BRAND, SEGMENT, VOL_AVG, VOL_TOTAL, TREND)

입력:
  - silver/KEYWORDS_MASTER.csv (35-col, v2 최종)
  - frame_design/frame_nodes.json (프레임 노드 → axis/group)
  - ref/dimensions/trend_fashion_dimensions.json
  - ref/brand_rules/trend_fashion.json

로직은 v1 build_dimension_tags.py / build_expansion.py / build_brand_axis.py 계승.
"""
import csv
import json
import re
import os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

REPO = config.REPO
RUN = config.RUN
SILVER = config.SILVER
FRAME_DESIGN = config.FRAME_DESIGN

print("=" * 70)
print("누락 SILVER 테이블 3종 생성")
print("=" * 70)

def norm(k):
    return re.sub(r"\s+", "", k or "")

# ── 참조 데이터 로드 ──────────────────────────────────────────
DIM = json.load(open(config.DIMENSIONS_JSON, encoding="utf-8"))["dimensions"]
BODY = DIM["BODY_TYPE"]["members"]
OCC = DIM["OCCASION_TPO"]["members"] + ["하객", "수학여행", "놀이공원", "삿포로", "일본 여행"]
MOD = DIM["STYLE_MODIFIER"]["members"]

BRAND_RULES = json.load(open(config.BRAND_JSON, encoding="utf-8"))
# canonical → (segment, [norm aliases + norm canonical])
BRAND_LOOKUP = []
for b in BRAND_RULES["brands"]:
    names = [b["canonical"]] + b.get("aliases", [])
    BRAND_LOOKUP.append((b["canonical"], b["segment"], [norm(n) for n in names]))

# 프레임 노드: norm_keyword → (axis, group)
frame_axis_by_norm = {}
for node in json.load(open(FRAME_DESIGN / "frame_nodes.json", encoding="utf-8")):
    frame_axis_by_norm[norm(node["keyword"])] = (node["axis"], node.get("group", ""))

# ── 태깅 함수 (v1 계승) ──────────────────────────────────────
def first_hit(text, members):
    n = norm(text)
    for m in members:
        if norm(m) in n:
            return m
    return None

def gender(t):
    n = norm(t)
    if "남자" in n or "남성" in n:
        return "M"
    if "여자" in n or "여성" in n:
        return "F"
    return None

def season(t):
    for s in ["봄", "여름", "가을", "겨울"]:
        if s in t:
            return s
    return None

# ── 마스터 로드 ──────────────────────────────────────────────
print("\n[1/4] KEYWORDS_MASTER 로드...")
with open(SILVER / "KEYWORDS_MASTER.csv", encoding="utf-8-sig") as f:
    master = list(csv.DictReader(f))
print(f"✓ {len(master)}개 키워드")

def to_int(v):
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0

def to_float(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def winsor_trend(v):
    """volume_trend를 [-1, 3]으로 winsorize — API 아티팩트(trend>=100 등) 집계 왜곡 방지.
    실제 검색 트렌드는 이 범위를 벗어나지 않으며, 극단값은 저볼륨 기저의 비율 폭발."""
    t = to_float(v)
    if t > 3.0:
        return 3.0
    if t < -1.0:
        return -1.0
    return t

# ============================================================
# 1. KEYWORDS_TAGGED
# ============================================================
print("\n[2/4] KEYWORDS_TAGGED 생성...")
tagged_rows = []
tag_hits = 0
for r in master:
    kw = r["keyword"]
    canon = r.get("canonical_keyword") or kw
    db = first_hit(kw, BODY)
    do = first_hit(kw, OCC)
    dm = first_hit(kw, MOD)
    dg = gender(kw)
    ds = season(kw)
    if any([db, do, dm, dg, ds]):
        tag_hits += 1
    tagged_rows.append({
        "SOURCE": "panel",
        "CANONICAL": canon,
        "KEYWORD": kw,
        "DIM_BODY": db or "",
        "DIM_OCCASION": do or "",
        "DIM_MODIFIER": dm or "",
        "DIM_GENDER": dg or "",
        "DIM_SEASON": ds or "",
    })

with open(SILVER / "KEYWORDS_TAGGED.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["SOURCE", "CANONICAL", "KEYWORD",
                                      "DIM_BODY", "DIM_OCCASION", "DIM_MODIFIER",
                                      "DIM_GENDER", "DIM_SEASON"])
    w.writeheader()
    w.writerows(tagged_rows)
print(f"✓ KEYWORDS_TAGGED.csv: {len(tagged_rows)}행 (차원 태그 적중 {tag_hits}행)")

# ============================================================
# 2. KEYWORDS_EXPANDED  (프레임 노드 canonical 롤업)
# ============================================================
print("\n[3/4] KEYWORDS_EXPANDED 생성...")
# 각 키워드를 프레임 노드에 매칭(정규화 substring, 최장 노드 우선) → canonical 그룹
frame_nodes_sorted = sorted(frame_axis_by_norm.keys(), key=len, reverse=True)

def match_canonical(kw):
    nk = norm(kw)
    # 정확 매칭 우선
    if nk in frame_axis_by_norm:
        return nk, frame_axis_by_norm[nk][0]
    # 최장 프레임 노드 substring 매칭
    for fn in frame_nodes_sorted:
        if len(fn) >= 2 and fn in nk:
            return fn, frame_axis_by_norm[fn][0]
    return None, None

groups = defaultdict(lambda: {"type": None, "dim": None, "vt": 0, "va": 0,
                               "tr_w": 0.0, "nvt": 0, "variants": []})
for r in master:
    kw = r["keyword"]
    fn_norm, axis = match_canonical(kw)
    if fn_norm:
        # canonical = 대표 프레임 노드 표기 복원
        canon = next((n["keyword"] for n in json.load(open(FRAME_DESIGN / "frame_nodes.json", encoding="utf-8"))
                      if norm(n["keyword"]) == fn_norm), fn_norm) if False else fn_norm
        typ = axis
    else:
        canon = norm(kw)
        typ = "EXPANDED"
    # 차원 태그
    dim = None
    if first_hit(kw, OCC):
        dim = "OCCASION_TPO"
    elif first_hit(kw, MOD):
        dim = "STYLE_MODIFIER"
    elif first_hit(kw, BODY):
        dim = "BODY_TYPE"

    vt = to_int(r.get("volume_total"))
    g = groups[canon]
    if g["type"] is None:
        g["type"] = typ
        g["dim"] = dim
    g["vt"] += vt
    g["va"] += to_int(r.get("volume_avg"))
    g["tr_w"] += winsor_trend(r.get("volume_trend")) * vt
    g["nvt"] += to_int(r.get("nv_volume_total"))
    g["variants"].append(kw)

# 프레임 노드 표기 복원 맵
canon_display = {norm(n["keyword"]): n["keyword"]
                 for n in json.load(open(FRAME_DESIGN / "frame_nodes.json", encoding="utf-8"))}

expanded_rows = []
for canon, g in groups.items():
    vt = g["vt"] or 1
    expanded_rows.append({
        "CANONICAL_KEYWORD": canon_display.get(canon, canon),
        "TYPE": g["type"],
        "DIMENSION": g["dim"] or "",
        "VOLUME_TOTAL": g["vt"],
        "VOLUME_AVG": g["va"],
        "VOLUME_TREND_WAVG": round(g["tr_w"] / vt, 4),
        "NV_SHARE": round(g["nvt"] / vt, 4),
        "VARIANTS": "|".join(g["variants"][:50]),  # 상위 50개 변형만
    })
expanded_rows.sort(key=lambda x: -x["VOLUME_TOTAL"])

with open(SILVER / "KEYWORDS_EXPANDED.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["CANONICAL_KEYWORD", "TYPE", "DIMENSION",
                                      "VOLUME_TOTAL", "VOLUME_AVG", "VOLUME_TREND_WAVG",
                                      "NV_SHARE", "VARIANTS"])
    w.writeheader()
    w.writerows(expanded_rows)

from collections import Counter
type_dist = Counter(r["TYPE"] for r in expanded_rows)
print(f"✓ KEYWORDS_EXPANDED.csv: {len(expanded_rows)}개 canonical")
print(f"  TYPE 분포: {dict(type_dist)}")

# ============================================================
# 3. BRAND_MASTER  (코퍼스 내 브랜드 검출 + 볼륨 집계)
# ============================================================
print("\n[4/4] BRAND_MASTER 생성...")
brand_agg = defaultdict(lambda: {"segment": None, "va": 0, "vt": 0, "tr_w": 0.0})

for r in master:
    nk = norm(r["keyword"])
    for canonical, segment, aliases in BRAND_LOOKUP:
        if any(a in nk for a in aliases if len(a) >= 2):
            vt = to_int(r.get("volume_total"))
            g = brand_agg[canonical]
            g["segment"] = segment
            g["va"] += to_int(r.get("volume_avg"))
            g["vt"] += vt
            g["tr_w"] += winsor_trend(r.get("volume_trend")) * vt
            break  # 키워드당 첫 매칭 브랜드만

brand_rows = []
for brand, g in brand_agg.items():
    vt = g["vt"] or 1
    brand_rows.append({
        "BRAND": brand,
        "SEGMENT": g["segment"],
        "VOL_AVG": g["va"],
        "VOL_TOTAL": g["vt"],
        "TREND": round(g["tr_w"] / vt, 4),
    })
brand_rows.sort(key=lambda x: -x["VOL_AVG"])

with open(SILVER / "BRAND_MASTER.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["BRAND", "SEGMENT", "VOL_AVG", "VOL_TOTAL", "TREND"])
    w.writeheader()
    w.writerows(brand_rows)
print(f"✓ BRAND_MASTER.csv: {len(brand_rows)}개 브랜드 (코퍼스 검출)")
seg_dist = Counter(r["SEGMENT"] for r in brand_rows)
print(f"  세그먼트 분포: {dict(seg_dist)}")

print("\n" + "=" * 70)
print("✓ 누락 SILVER 테이블 3종 생성 완료!")
print("=" * 70)
print(f"\n  KEYWORDS_TAGGED:   {len(tagged_rows):,}행")
print(f"  KEYWORDS_EXPANDED: {len(expanded_rows):,}행")
print(f"  BRAND_MASTER:      {len(brand_rows):,}행")
