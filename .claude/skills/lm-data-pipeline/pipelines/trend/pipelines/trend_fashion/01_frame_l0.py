#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STAGE 01 — L0 프레임 materialization + 수집 배치 계획.

ref/frame/<cat>_kr_frame.json(축·그룹·노드)을 평탄화해
  - frame_design/frame_nodes.json  (keyword/axis/group — 05·06·08이 세그먼트 소스로 사용)
  - frame_design/l0_batches/*.json (keyword_info 수집 배치 스펙)
을 생성한다. 실제 API 수집은 --collect 로 실행한다(수집 규약: 원본 무변형 저장 · 1콜 1원장).

이 스테이지는 결정적(deterministic) — 동일 frame.json → 동일 노드/배치.

수집 실행(선택):
    python3 01_frame_l0.py --collect     # L0 keyword_info(all) API 실호출 (LM_API_KEY 필요)
기본(플래그 없음)은 노드/배치 계획만 생성. 실호출은 lm_api 클라이언트(API Key) 사용.
"""
import json, os, sys, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

config.ensure_dirs()
FD = config.FRAME_DESIGN
(FD / "l0_batches").mkdir(parents=True, exist_ok=True)

print("=" * 70)
print(f"01 FRAME L0 — {config.RUN_ID}")
print("=" * 70)

if not config.FRAME_JSON.exists():
    print(f"❌ 프레임 없음: {config.FRAME_JSON}")
    sys.exit(1)

frame = json.load(open(config.FRAME_JSON, encoding="utf-8"))

# ── 노드 평탄화 ──────────────────────────────────────────────
nodes = []
for node in frame.get("nodes", []):
    axis, group = node["axis"], node.get("group", "")
    for kw in node["keywords"]:
        nodes.append({"keyword": kw, "axis": axis, "group": group})

# 중복 제거(정규화 기준 첫 등장 유지)
seen, uniq = set(), []
for n in nodes:
    nk = config.norm(n["keyword"])
    if nk not in seen:
        seen.add(nk); uniq.append(n)

json.dump(uniq, open(FD / "frame_nodes.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(f"✓ frame_nodes.json: {len(uniq)}개 노드")
from collections import Counter
print(f"  축 분포: {dict(Counter(n['axis'] for n in uniq))}")

# ── L0 배치 스펙 (keyword_info, 배치당 48개) ─────────────────
BATCH = 48
batches = [uniq[i:i + BATCH] for i in range(0, len(uniq), BATCH)]
for bi, b in enumerate(batches, 1):
    spec = {
        "batch": bi, "phase": "L0_frame_metrics", "api": "keyword_info",
        "params": {"gl": config.GL, "data_type": "all"},
        "keywords": [n["keyword"] for n in b],
    }
    json.dump(spec, open(FD / "l0_batches" / f"l0_batch_{bi:03d}.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
print(f"✓ l0_batches/: {len(batches)}개 배치 (배치당 최대 {BATCH})")

# ── L0 수집 (API Key 직접 호출) ──────────────────────────────
ap = argparse.ArgumentParser()
ap.add_argument("--collect", action="store_true", help="L0 keyword_info(all) API 실호출")
args, _ = ap.parse_known_args()

if args.collect:
    import lm_api
    client = lm_api.LMClient()
    print(f"\n[수집] L0 keyword_info(all) — {len(uniq)}노드, API={config.LM_API_BASE}")
    kws = [n["keyword"] for n in uniq]
    total_cost = 0
    for i in range(0, len(kws), config.KEYWORD_INFO_BATCH):
        chunk = kws[i:i + config.KEYWORD_INFO_BATCH]
        bi = i // config.KEYWORD_INFO_BATCH + 1
        resp, led = client.keyword_info(chunk, data_type="all", tag=f"l0b{bi:03d}",
                                        phase="L0_frame_metrics", purpose="L0 프레임 지표")
        total_cost += led["total_cost"]
        miss = f" ⚠ missing={len(led['missing'])}" if led.get("missing") else ""
        print(f"  배치 {bi}: seq={led['call_seq']} returned={led['returned']} cost={led['total_cost']}{miss}")
    print(f"✓ L0 수집 완료 — 총 {total_cost} 크레딧 → bronze/raw + ledger")
else:
    print("\n다음: `python3 01_frame_l0.py --collect` (API Key 실수집) 또는")
    print("      수집 규약대로 keyword_info 수집 → bronze/raw + ledger")
print("=" * 70)
