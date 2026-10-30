#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v2 L1 통합기 — bronze/raw 의 intent_finder 응답 전수를 읽어
후보 유니버스를 중복제거하고 L2 게이팅 배치를 생성한다.

입력 : <run 폴더>/bronze/raw/call_0*_intent_finder_*.json.gz
       frame_design/frame_nodes.json (L0 노드 — 후보에서 제외, 이미 풀 수집)
출력 : frame_design/l1_candidates.csv        (후보 전수 + 출처 시드·볼륨 힌트)
       frame_design/l2_gate_batches/*.json   (keyword_info ads_metrics 배치, 400kw/콜)
       frame_design/l1_consolidation_stats.json
"""
import json, gzip, glob, os, csv, re

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
RUN = str(config.RUN)
RAW = os.path.join(RUN, "bronze", "raw")
OUT_BATCH_DIR = os.path.join(RUN, "frame_design", "l2_gate_batches")
os.makedirs(OUT_BATCH_DIR, exist_ok=True)

_frame_list = json.load(open(os.path.join(RUN, "frame_design", "frame_nodes.json"), encoding="utf-8"))
frame_nodes = {n["keyword"] for n in _frame_list}
def norm(k):  # 공백 제거 정규화 (표기변형 1차 접기용 키 — 원표기는 보존)
    return re.sub(r"\s+", "", k)
frame_norm = {norm(k) for k in frame_nodes}

# ── L1 수집 (API Key 직접 호출) — 노드별 intent_finder ───────
# python3 02_expand_l1.py --collect   (LM_API_KEY 필요). 기본은 기존 bronze 소비만.
if "--collect" in sys.argv:
    import lm_api
    client = lm_api.LMClient()
    print(f"[수집] L1 intent_finder — {len(_frame_list)}노드 (limit={config.L1_LIMIT_PER_NODE})")
    tot = 0
    for i, n in enumerate(_frame_list, 1):
        try:
            _, led = client.intent_finder(n["keyword"], limit=config.L1_LIMIT_PER_NODE,
                                          phase="L1_candidate_expansion", purpose=f"L1 확장 seed={n['keyword']}")
            tot += led["total_cost"]
            if i % 50 == 0:
                print(f"  {i}/{len(_frame_list)} 노드… 누적 {tot} 크레딧")
        except lm_api.LMApiError as e:
            print(f"  ⚠ {n['keyword']}: {e}")
    print(f"✓ L1 수집 완료 — 총 {tot} 크레딧 → bronze/raw. 이어서 통합 진행.\n")

candidates = {}  # norm_key -> dict(keyword=원표기(최대볼륨), sources=set, volume_hint=max)
files = sorted(glob.glob(os.path.join(RAW, "call_*_intent_finder_*.json.gz"))
             + glob.glob(os.path.join(RAW, "call_*_intent_finder_*.json")))
n_rows = 0
for f in files:
    try:
        op = gzip.open if f.endswith(".gz") else open
        d = json.load(op(f, "rt", encoding="utf-8"))
    except Exception as e:
        print("SKIP unreadable", os.path.basename(f), e); continue
    seed = (d.get("request_detail") or {}).get("keywords", ["?"])
    seed = seed[0] if isinstance(seed, list) and seed else str(seed)
    data = d.get("data") or d.get("result_data") or []
    if isinstance(data, dict):  # 방어: {seed: [...]} 형태
        data = [x for v in data.values() if isinstance(v, list) for x in v]
    for row in data:
        if isinstance(row, str):
            kw, vol = row, 0
        else:
            kw = row.get("keyword") or row.get("kw") or ""
            vol = row.get("volume_avg") or row.get("volume") or 0
        if not kw:
            continue
        n_rows += 1
        nk = norm(kw)
        if nk in frame_norm:
            continue  # L0 노드는 이미 풀 수집
        c = candidates.setdefault(nk, {"keyword": kw, "sources": set(), "volume_hint": 0})
        c["sources"].add(seed)
        if vol and vol > c["volume_hint"]:
            c["volume_hint"] = vol
            c["keyword"] = kw  # 최대 볼륨 표기를 대표 원표기로

cands = sorted(candidates.values(), key=lambda c: -c["volume_hint"])
with open(os.path.join(RUN, "frame_design", "l1_candidates.csv"), "w", newline="", encoding="utf-8-sig") as fp:
    w = csv.writer(fp); w.writerow(["keyword", "volume_hint", "n_sources", "sources"])
    for c in cands:
        w.writerow([c["keyword"], c["volume_hint"], len(c["sources"]), "|".join(sorted(c["sources"]))])

B = 400
seq = 400  # L2 call_seq 는 0400 부터
batches = [cands[i:i+B] for i in range(0, len(cands), B)]
for i, b in enumerate(batches, start=1):
    json.dump({"call_seq": seq, "phase": "L2_gate", "api": "keyword_info",
               "params": {"gl": "kr", "data_type": "ads_metrics"},
               "keywords": [c["keyword"] for c in b]},
              open(os.path.join(OUT_BATCH_DIR, f"batch_{i:02d}.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    seq += 1

stats = {"intent_finder_files": len(files), "raw_rows": n_rows,
         "unique_candidates": len(cands), "l2_batches": len(batches),
         "top20": [(c["keyword"], c["volume_hint"]) for c in cands[:20]]}
json.dump(stats, open(os.path.join(RUN, "frame_design", "l1_consolidation_stats.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
print(json.dumps(stats, ensure_ascii=False, indent=1))
