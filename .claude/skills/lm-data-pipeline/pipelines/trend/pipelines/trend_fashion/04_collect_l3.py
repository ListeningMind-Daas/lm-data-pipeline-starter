#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STAGE 04 — L3 승격 수집 + 원장 백필.

--collect (API Key): l2_gate_results.csv 승격분(gate != BELOW_FLOOR)에
  keyword_info(all) 실호출 → bronze/raw + ledger (lm_api 클라이언트가 원장 자동 기록).
기본(플래그 없음): 백필 — raw는 있으나 ledger 없는 콜을 cost_detail에서 복원(레거시/멱등).
"""
import glob, gzip, json, os, re, subprocess, sys

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
RUN = str(config.RUN)

# ── L3 수집 (API Key) — 승격분 keyword_info(all) ─────────────
if "--collect" in sys.argv:
    import csv, lm_api
    client = lm_api.LMClient()
    gate_path = os.path.join(RUN, "frame_design", "l2_gate_results.csv")
    promoted = [r["keyword"] for r in csv.DictReader(open(gate_path, encoding="utf-8-sig"))
                if r.get("gate") != "BELOW_FLOOR"]
    print(f"[수집] L3 keyword_info(all) — 승격 {len(promoted)}개")
    tot = 0
    for i in range(0, len(promoted), config.KEYWORD_INFO_BATCH):
        chunk = promoted[i:i + config.KEYWORD_INFO_BATCH]
        bi = i // config.KEYWORD_INFO_BATCH + 1
        _, led = client.keyword_info(chunk, data_type="all", tag=f"l3b{bi:03d}",
                                     phase="L3_promotion", purpose="L3 승격 풀 수집")
        tot += led["total_cost"]
        miss = f" ⚠ missing={len(led['missing'])}" if led.get("missing") else ""
        print(f"  배치 {bi}: seq={led['call_seq']} returned={led['returned']} cost={led['total_cost']}{miss}")
    print(f"✓ L3 수집 완료 — 총 {tot} 크레딧 → bronze/raw + ledger\n")
PHASE_BY_RANGE = [(2, 7, "L0_frame_metrics"), (100, 370, "L1_candidate_expansion"),
                  (371, 380, "L1_hub_scan"), (400, 499, "L2_gate"), (500, 999, "L3_promotion")]

def phase_of(seq):
    for lo, hi, p in PHASE_BY_RANGE:
        if lo <= seq <= hi: return p
    return "unknown"

led = {int(re.search(r"call_(\d+)", f).group(1)) for f in glob.glob(f"{RUN}/bronze/ledger/call_0*.json")}
raws = {}
for f in glob.glob(f"{RUN}/bronze/raw/call_0*"):
    raws.setdefault(int(re.search(r"call_(\d+)", f).group(1)), []).append(f)

fixed, gzipped, bad = [], [], []
for seq, files in sorted(raws.items()):
    f = sorted(files, key=lambda x: (not x.endswith(".gz"), x))[0]  # prefer .gz
    if not f.endswith(".gz"):
        subprocess.run(["gzip", "-f", f], check=True)
        f += ".gz"; gzipped.append(seq)
    if seq in led:
        continue
    try:
        d = json.load(gzip.open(f, "rt", encoding="utf-8"))
    except Exception as e:
        bad.append((seq, str(e)[:80])); continue
    cd = d.get("cost_detail") or {}
    rd = d.get("request_detail") or {}
    kws = rd.get("keywords") or []
    api = "intent_finder" if "intent_finder" in f else ("keyword_info" if "keyword_info" in f else "unknown")
    ledger = {"call_seq": seq, "run_id": config.RUN_ID, "gl": rd.get("gl", "kr"),
              "phase": phase_of(seq), "api": api,
              "purpose": f"backfilled from raw · {(kws[0] if kws else '')}",
              "params": json.dumps({k: v for k, v in rd.items() if k != "keywords"}, ensure_ascii=False),
              "requested_keywords": len(kws),
              "input_cost": cd.get("input_cost"), "input_count": cd.get("input_count"),
              "output_cost": cd.get("output_cost"), "output_count": cd.get("output_count"),
              "total_cost": cd.get("total_cost"), "returned": len(d.get("data") or []),
              "request_at": d.get("request_at"), "used_credits_after": d.get("used_credits"),
              "raw_file": os.path.relpath(f, RUN), "note": "ledger backfilled from raw"}
    json.dump(ledger, open(f"{RUN}/bronze/ledger/call_{seq:04d}.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    fixed.append(seq)

print(f"backfilled: {len(fixed)} {fixed}")
print(f"gzipped: {len(gzipped)}")
if bad:
    print("UNREADABLE:", bad); sys.exit(1)
