#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py — trend_fashion 파이프라인 오케스트레이터.

메달리온 번호순 실행. 수집(01~04)은 크레딧을 쓰므로 --collect 를 줄 때만 돌리고,
bronze/raw(L3)가 준비된 상태에서 재현 가능한 빌드 체인(05~08)을 실행한다.

    python3 run_all.py                # 05→08 빌드 체인 (기본)
    python3 run_all.py --from 05 --to 08
    python3 run_all.py --stage 08     # 단일 스테이지
    TREND_FASHION_RUN_DATE=2026-08-01 python3 run_all.py   # 다른 런

수집 단계(01~04)는 별도: 01은 배치 계획 생성(결정적), 02~04는 수집 규약(원본 무변형 저장 · 1콜 1원장).
대상 저장소 적재는 별도: loader/lmdp.py load (로컬·Databricks·Snowflake·BigQuery).
"""
import os, sys, subprocess, argparse, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

HERE = os.path.dirname(os.path.abspath(__file__))

STAGES = [
    ("01", "01_frame_l0.py",     "L0 프레임 materialization + 배치계획"),
    ("02", "02_expand_l1.py",    "L1 후보 확장 통합 (intent_finder)"),
    ("03", "03_gate_l2.py",      "L2 속도 게이팅 (ads_metrics)"),
    ("04", "04_collect_l3.py",   "L3 원장 백필/정규화"),
    ("05", "05_silver_master.py","SILVER: MASTER/MONTHLY/FEATURES (bronze L3)"),
    ("06", "06_silver_enrich.py","SILVER: TAGGED/EXPANDED/BRAND_MASTER"),
    ("07", "07_clean_noise.py",  "동음이의 비패션 노이즈 정제"),
    ("08", "08_gold_build.py",   "GOLD 15 + COST 2 + META 2 + BRONZE manifest"),
]

def run_stage(num, script, desc, extra=None):
    print(f"\n{'█'*70}\n█ STAGE {num} — {desc}\n{'█'*70}")
    t = time.time()
    r = subprocess.run([sys.executable, os.path.join(HERE, script)] + (extra or []))
    if r.returncode != 0:
        print(f"\n❌ STAGE {num} 실패 (exit {r.returncode}) — 중단")
        sys.exit(r.returncode)
    print(f"⏱ STAGE {num} {time.time()-t:.1f}s")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", default="05")
    ap.add_argument("--to", dest="to", default="08")
    ap.add_argument("--stage", dest="stage", default=None)
    ap.add_argument("--collect", action="store_true",
                    help="수집 모드: 01~04 API Key 실호출(LM_API_KEY 필요) 후 05~08 빌드")
    args = ap.parse_args()

    print(f"RUN_ID={config.RUN_ID}  ref_version={config.ref_version()}")
    if args.collect:
        # 수집(01~04, --collect 전달) → 빌드(05~08)
        for num, script, desc in STAGES:
            run_stage(num, script, desc, extra=["--collect"] if num in ("01", "02", "03", "04") else None)
        _summary(); return

    sel = [s for s in STAGES if (s[0] == args.stage) if args.stage] or \
          [s for s in STAGES if args.frm <= s[0] <= args.to]
    print(f"실행 스테이지: {', '.join(s[0] for s in sel)}")
    for num, script, desc in sel:
        run_stage(num, script, desc)
    _summary()

def _summary():
    print(f"\n{'='*70}\n✓ 파이프라인 완료 — {config.RUN_ID}")
    total = sum(len(v) for v in config.CATALOG.values())
    print(f"  카탈로그 {total}종 → projects/{config.RUN_ID}/{{bronze,silver,gold,cost,meta}}/")
    print(f"  적재: python3 .claude/skills/lm-data-pipeline/loader/lmdp.py load <프로젝트> --run {config.RUN_ID}")
    print("=" * 70)

if __name__ == "__main__":
    main()
