#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STAGE 07 — 동음이의 비패션 노이즈 정제 (SILVER in-place).

노이즈 규칙은 config.NOISE_* 단일 소스 (토큰/접미사 단위 — substring 오탐 방지).
정제 후 06(enrich)을 재실행해 EXPANDED/BRAND/TAGGED를 정제본 기준으로 재생성.
GOLD/COST/META는 08에서 정제된 SILVER를 읽어 생성 (여기서는 SILVER만 정제).

입력:  silver/KEYWORDS_MASTER·MONTHLY·FEATURES·TAGGED (05·06 산출)
출력:  동일 파일 in-place 정제 + EXPANDED/BRAND 재생성
"""
import csv, re, os, sys, subprocess
from collections import Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

SILVER = config.SILVER
HERE = os.path.dirname(os.path.abspath(__file__))

def noise_category(kw):
    if re.search(config.NOISE_EPISODE, kw):          # 에피소드 번호 (숫자+화)
        return "에피소드"
    n = kw.replace(" ", "")
    if n.endswith(config.NOISE_SUFFIX):              # 사전 질의 (뜻/유래)
        return "사전"
    for ph, cat in config.NOISE_PHRASES.items():     # 다단어 구
        if ph in n:
            return cat
    for tok in kw.split():                            # 토큰 매칭
        if tok.lower() in config.NOISE_TOKENS:
            return config.NOISE_TOKENS[tok.lower()]
    return None

print("=" * 70)
print(f"07 CLEAN NOISE — {config.RUN_ID}")
print("=" * 70)

# ── 1. 노이즈 키워드 집합 ────────────────────────────────────
print("\n[1/4] 노이즈 키워드 식별...")
with open(SILVER / "KEYWORDS_MASTER.csv", encoding="utf-8-sig") as f:
    master = list(csv.DictReader(f))
noise_keywords, detail = set(), Counter()
for r in master:
    cat = noise_category(r["keyword"])
    if cat:
        noise_keywords.add(r["keyword"]); detail[cat] += 1
print(f"✓ 노이즈 {len(noise_keywords)}개 / {len(master)}개 ({len(noise_keywords)/max(len(master),1)*100:.2f}%)")
print(f"  유형별: {dict(detail)}")

# ── 2. 행 단위 SILVER 정제 (keyword 컬럼 기준) ───────────────
print("\n[2/4] SILVER 행 정제...")
def clean_file(fname, key_col="keyword"):
    path = SILVER / fname
    if not path.exists():
        return
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f); fields = reader.fieldnames
        kc = next((c for c in fields if c.lower() == key_col.lower()), None)
        kept = [row for row in reader if kc and row[kc] not in noise_keywords]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(kept)
    print(f"  ✓ {fname}: {len(kept):,}행")

for fn in ["KEYWORDS_MASTER.csv", "KEYWORDS_FEATURES.csv", "KEYWORDS_MONTHLY.csv"]:
    clean_file(fn)
clean_file("KEYWORDS_TAGGED.csv", key_col="KEYWORD")

# ── 3. EXPANDED/BRAND/TAGGED 재생성 (정제 master 기준) ───────
print("\n[3/4] 06(enrich) 재실행 — EXPANDED/BRAND/TAGGED 정제본 재생성...")
r = subprocess.run([sys.executable, os.path.join(HERE, "06_silver_enrich.py")],
                   capture_output=True, text=True)
print("  ✓ 재생성 완료" if r.returncode == 0 else f"  ⚠ {r.stderr[-300:]}")

# ── 4. 검증 ──────────────────────────────────────────────────
print("\n[4/4] 검증...")
for fn in ["KEYWORDS_MASTER.csv", "KEYWORDS_MONTHLY.csv", "KEYWORDS_FEATURES.csv",
           "KEYWORDS_TAGGED.csv", "KEYWORDS_EXPANDED.csv", "BRAND_MASTER.csv"]:
    p = SILVER / fn
    if p.exists():
        with open(p, encoding="utf-8-sig") as f:
            print(f"  {fn:26} {sum(1 for _ in f)-1:>8,}행")
with open(SILVER / "KEYWORDS_MASTER.csv", encoding="utf-8-sig") as f:
    remaining = sum(1 for r in csv.DictReader(f) if noise_category(r["keyword"]))
print(f"\n노이즈 잔존: {remaining}개 (0이어야 정상)")
print("\n" + "=" * 70)
print("✓ 07 정제 완료")
print("=" * 70)
