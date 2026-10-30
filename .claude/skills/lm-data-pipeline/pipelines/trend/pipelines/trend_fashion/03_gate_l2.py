#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v2 L2 게이트 판정 — bronze/raw 의 L2 keyword_info(ads_metrics) 응답을 읽어
속도 게이팅 룰(ref/frame·ref/panels 계승)을 적용하고 L3 승격 배치를 생성한다.

게이트 (frame json gating_policy):
  STABLE      vol_avg >= 300
  EMERGING    vol_avg >= 50 AND |volume_trend| >= 0.3
  BREAKOUT    volume_trend >= 0.5 AND vol_avg >= 100
  BELOW_FLOOR 그 외 (패널 제외, 후보 지위 유지 — 데이터는 보존)

출력:
  frame_design/l2_gate_results.csv   키워드별 게이트 판정 전수
  frame_design/l3_batches/*.json     승격분 keyword_info(all) 배치 (48kw/콜, seq 500+)
  frame_design/l2_gate_stats.json
"""
import json, gzip, glob, os, csv, re

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
RUN = str(config.RUN)
RAW = os.path.join(RUN, "bronze", "raw")
OUT = os.path.join(RUN, "frame_design", "l3_batches")
os.makedirs(OUT, exist_ok=True)

def norm(k): return re.sub(r"\s+", "", k)

# ── L2 수집 (API Key) — 후보 keyword_info(ads_metrics) ───────
# python3 03_gate_l2.py --collect   (l1_candidates.csv → keyword_info ads_metrics)
if "--collect" in sys.argv:
    import lm_api
    client = lm_api.LMClient()
    cand_path = os.path.join(RUN, "frame_design", "l1_candidates.csv")
    cand_kws = [r["keyword"] for r in csv.DictReader(open(cand_path, encoding="utf-8-sig"))]
    print(f"[수집] L2 keyword_info(ads_metrics) — {len(cand_kws)}후보")
    tot = 0
    for i in range(0, len(cand_kws), config.KEYWORD_INFO_BATCH):
        chunk = cand_kws[i:i + config.KEYWORD_INFO_BATCH]
        bi = i // config.KEYWORD_INFO_BATCH + 1
        _, led = client.keyword_info(chunk, data_type="ads_metrics", tag=f"l2b{bi:03d}",
                                     phase="L2_gate", purpose="L2 게이팅 지표")
        tot += led["total_cost"]
        print(f"  배치 {bi}: seq={led['call_seq']} returned={led['returned']} cost={led['total_cost']}")
    print(f"✓ L2 수집 완료 — 총 {tot} 크레딧. 이어서 게이팅.\n")

# L2 응답 = keyword_info(ads_metrics): API 수집(tag l2*) 또는 레거시(call_04xx)
rows = {}
l2_files = sorted(set(glob.glob(os.path.join(RAW, "call_*_keyword_info_l2*.json.gz")) +
                      glob.glob(os.path.join(RAW, "call_04*_keyword_info_*.json.gz"))))
for f in l2_files:
    d = json.load(gzip.open(f, "rt", encoding="utf-8"))
    for e in d.get("data") or []:
        kw = e.get("keyword"); a = e.get("ads_metrics") or {}
        if not kw: continue
        nk = norm(kw)
        prev = rows.get(nk)
        if prev is None or (a.get("volume_avg") or 0) > (prev["volume_avg"] or 0):
            rows[nk] = {"keyword": kw, "volume_avg": a.get("volume_avg") or 0,
                        "volume_total": a.get("volume_total") or 0,
                        "volume_trend": a.get("volume_trend") or 0.0}

def gate(r):
    va, tr = r["volume_avg"], r["volume_trend"]
    if tr >= 0.5 and va >= 100: return "BREAKOUT"
    if va >= 300: return "STABLE"
    if va >= 50 and abs(tr) >= 0.3: return "EMERGING"
    return "BELOW_FLOOR"

for r in rows.values():
    r["gate"] = gate(r)

out = sorted(rows.values(), key=lambda r: -r["volume_total"])
with open(os.path.join(RUN, "frame_design", "l2_gate_results.csv"), "w", newline="", encoding="utf-8-sig") as fp:
    w = csv.DictWriter(fp, fieldnames=["keyword","volume_avg","volume_total","volume_trend","gate"])
    w.writeheader(); w.writerows(out)

promoted = [r["keyword"] for r in out if r["gate"] != "BELOW_FLOOR"]
B, seq = 48, 500
batches = [promoted[i:i+B] for i in range(0, len(promoted), B)]
for i, b in enumerate(batches, start=1):
    json.dump({"call_seq": seq, "phase": "L3_promotion", "api": "keyword_info",
               "params": {"gl": "kr", "data_type": "all"}, "keywords": b},
              open(os.path.join(OUT, f"batch_{i:02d}.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    seq += 1

stats = {"gated_total": len(out),
         "by_gate": {g: sum(1 for r in out if r["gate"] == g) for g in ["STABLE","EMERGING","BREAKOUT","BELOW_FLOOR"]},
         "promoted": len(promoted), "l3_batches": len(batches)}
json.dump(stats, open(os.path.join(RUN, "frame_design", "l2_gate_stats.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
print(json.dumps(stats, ensure_ascii=False, indent=1))
