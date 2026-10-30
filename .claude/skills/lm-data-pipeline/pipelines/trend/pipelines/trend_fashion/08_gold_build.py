#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 카탈로그 CSV 생성 (적재 스키마 컬럼 순서 일치).
SILVER 6종은 기생성 → 나머지 20종(BRONZE/GOLD/COST/META) 생성.

소스: silver/*.csv (정제본), bronze/ledger/*.json, frame_design/*, ref/*
출력: bronze/  gold/  cost/  meta/  하위 .csv
"""
import csv, json, glob, os, re, math, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

RUN = config.RUN
REPO = config.REPO
SILVER, GOLD = config.SILVER, config.GOLD
COST, META, BRONZE = config.COST, config.META, config.BRONZE
for d in (GOLD, COST, META):
    d.mkdir(parents=True, exist_ok=True)

RUN_ID = config.RUN_ID
RUN_DATE = config.RUN_DATE
GL = config.GL

def norm(k): return re.sub(r"\s+", "", k or "")
def fnum(v):
    try: return float(v)
    except: return 0.0
def inum(v):
    try: return int(float(v))
    except: return 0
def winsor(v):
    t = fnum(v); return 3.0 if t > 3 else (-1.0 if t < -1 else t)
def wcsv(path, cols, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in rows: w.writerow({c: r.get(c, "") for c in cols})
    print(f"  ✓ {path.parent.name}/{path.name}: {len(rows):,}행")

def dominant_intent(r):
    pairs = [("I", inum(r.get("intent_i"))), ("N", inum(r.get("intent_n"))),
             ("C", inum(r.get("intent_c"))), ("T", inum(r.get("intent_t")))]
    return max(pairs, key=lambda x: x[1])[0]

print("=" * 70); print("전체 카탈로그 CSV 생성 (20종)"); print("=" * 70)

# ── 소스 로드 ────────────────────────────────────────────────
master = list(csv.DictReader(open(SILVER / "KEYWORDS_MASTER.csv", encoding="utf-8-sig")))
tagged = list(csv.DictReader(open(SILVER / "KEYWORDS_TAGGED.csv", encoding="utf-8-sig")))
expanded = list(csv.DictReader(open(SILVER / "KEYWORDS_EXPANDED.csv", encoding="utf-8-sig")))
brands = list(csv.DictReader(open(SILVER / "BRAND_MASTER.csv", encoding="utf-8-sig")))
monthly = list(csv.DictReader(open(SILVER / "KEYWORDS_MONTHLY.csv", encoding="utf-8-sig")))

# gate_status 맵 (17-col 레거시)
gate_map = {}
_l2 = config.FRAME_DESIGN / "l2_gate_results.csv"
if _l2.exists():
    for r in csv.DictReader(open(_l2, encoding="utf-8-sig")):
        gate_map[r["keyword"]] = (r.get("gate", ""), "")

# 프레임 노드 → axis/group
frame = {}
for n in json.load(open(RUN / "frame_design" / "frame_nodes.json", encoding="utf-8")):
    frame[norm(n["keyword"])] = (n["axis"], n.get("group", ""), n["keyword"])
frame_sorted = sorted(frame.keys(), key=len, reverse=True)

def match_frame(kw):
    nk = norm(kw)
    if nk in frame: return frame[nk]
    for fn in frame_sorted:
        if len(fn) >= 2 and fn in nk: return frame[fn]
    return (None, None, None)

# ============================================================
# BRONZE.RAW_API_RESPONSES  (원본 파일 매니페스트)
# ============================================================
print("\n[BRONZE]")
raw_rows = []
for gz in sorted(glob.glob(str(BRONZE / "raw" / "call_*.json.gz"))):
    fn = os.path.basename(gz)
    api = "keyword_info" if "keyword_info" in fn else ("intent_finder" if "intent" in fn else "ads_metrics")
    raw_rows.append({"SOURCE_FILE": f"bronze/raw/{fn}", "API": api, "RUN_DATE": RUN_DATE,
                     "LOADED_AT": f"{RUN_DATE}T00:00:00Z", "PAYLOAD": ""})  # PAYLOAD는 gz 외부보존
wcsv(BRONZE / "RAW_API_RESPONSES.csv",
     ["SOURCE_FILE", "API", "RUN_DATE", "LOADED_AT", "PAYLOAD"], raw_rows)

# ============================================================
# GOLD
# ============================================================
print("\n[GOLD]")

# KEYWORD_SCORECARD — canonical 롤업 (WAVG 지표)
sc = defaultdict(lambda: {"seg": "", "variants": [], "vt": 0, "va": 0, "trw": 0.0,
                          "nvt": 0, "ciw": 0.0, "cpcw": 0.0,
                          "i": 0, "n": 0, "c": 0, "t": 0,
                          "gmw": 0.0, "gfw": 0.0, "age": defaultdict(float)})
AGE_COLS = ["age_0_ratio", "age_13_ratio", "age_20_ratio", "age_25_ratio", "age_30_ratio", "age_40_ratio", "age_50_ratio"]
for r in master:
    axis, group, disp = match_frame(r["keyword"])
    canon = disp if disp else r["keyword"]
    seg = group or r.get("segment") or ""
    vt = inum(r.get("volume_total"))
    g = sc[canon]
    g["seg"] = g["seg"] or seg
    g["variants"].append(r["keyword"])
    g["vt"] += vt; g["va"] += inum(r.get("volume_avg"))
    g["trw"] += winsor(r.get("volume_trend")) * vt
    g["nvt"] += inum(r.get("nv_volume_total"))
    g["ciw"] += inum(r.get("competition_index")) * vt
    g["cpcw"] += fnum(r.get("cpc")) * vt
    for k in "inct": g[k] += inum(r.get(f"intent_{k}"))
    g["gmw"] += fnum(r.get("gender_m_ratio")) * vt
    g["gfw"] += fnum(r.get("gender_f_ratio")) * vt
    for a in AGE_COLS: g["age"][a] += fnum(r.get(a)) * vt

ks_rows = []
for canon, g in sc.items():
    vt = g["vt"] or 1
    top_age = max(g["age"].items(), key=lambda x: x[1])[0].replace("age_", "").replace("_ratio", "") if g["age"] else ""
    di = max([("I", g["i"]), ("N", g["n"]), ("C", g["c"]), ("T", g["t"])], key=lambda x: x[1])[0]
    ks_rows.append({"CANONICAL_KEYWORD": canon, "SEGMENT": g["seg"], "VARIANT_COUNT": len(g["variants"]),
                    "VARIANTS": "|".join(g["variants"][:30]), "VOLUME_TOTAL": g["vt"], "VOLUME_AVG": g["va"],
                    "VOLUME_TREND_WAVG": round(g["trw"] / vt, 4), "NV_SHARE": round(g["nvt"] / vt, 4),
                    "COMPETITION_INDEX_WAVG": round(g["ciw"] / vt, 2), "CPC_WAVG": round(g["cpcw"] / vt, 4),
                    "DOMINANT_INTENT": di, "TOP_AGE_BUCKET": top_age,
                    "GENDER_M_RATIO_WAVG": round(g["gmw"] / vt, 4), "GENDER_F_RATIO_WAVG": round(g["gfw"] / vt, 4)})
ks_rows.sort(key=lambda x: -x["VOLUME_TOTAL"])
wcsv(GOLD / "KEYWORD_SCORECARD.csv",
     ["CANONICAL_KEYWORD", "SEGMENT", "VARIANT_COUNT", "VARIANTS", "VOLUME_TOTAL", "VOLUME_AVG",
      "VOLUME_TREND_WAVG", "NV_SHARE", "COMPETITION_INDEX_WAVG", "CPC_WAVG", "DOMINANT_INTENT",
      "TOP_AGE_BUCKET", "GENDER_M_RATIO_WAVG", "GENDER_F_RATIO_WAVG"], ks_rows)

# TREND_RANKING — canonical momentum 순위 (상위 300)
tr = []
for k in ks_rows:
    mom = round(k["VOLUME_TREND_WAVG"] * math.log10(max(k["VOLUME_TOTAL"], 10)), 3)
    direction = "UP" if k["VOLUME_TREND_WAVG"] > 0.1 else ("DOWN" if k["VOLUME_TREND_WAVG"] < -0.1 else "FLAT")
    tr.append({"CANONICAL_KEYWORD": k["CANONICAL_KEYWORD"], "SEGMENT": k["SEGMENT"],
               "VOLUME_TOTAL": k["VOLUME_TOTAL"], "VOLUME_TREND": k["VOLUME_TREND_WAVG"],
               "DOMINANT_INTENT": k["DOMINANT_INTENT"], "MOMENTUM_SCORE": mom, "DIRECTION": direction})
tr.sort(key=lambda x: -x["MOMENTUM_SCORE"])
tr = tr[:300]
for i, r in enumerate(tr, 1): r["RANK"] = i
wcsv(GOLD / "TREND_RANKING.csv",
     ["RANK", "CANONICAL_KEYWORD", "SEGMENT", "VOLUME_TOTAL", "VOLUME_TREND", "DOMINANT_INTENT",
      "MOMENTUM_SCORE", "DIRECTION"], tr)

# TREND_MONITOR — canonical + gate 분류
tm = []
for k in ks_rows:
    kw0 = k["VARIANTS"].split("|")[0]
    gate = gate_map.get(kw0, ("", ""))[0] or ""
    va = k["VOLUME_AVG"]; trd = k["VOLUME_TREND_WAVG"]
    breakout = 1 if (trd >= 0.5 and va >= 100) else 0
    mom = round(trd * math.log10(max(k["VOLUME_TOTAL"], 10)), 3)
    cc = "ACCELERATING" if trd >= 0.3 else ("FADING" if trd <= -0.05 else "STABLE")
    tm.append({"CANONICAL_KEYWORD": k["CANONICAL_KEYWORD"], "SEGMENT": k["SEGMENT"],
               "VOLUME_AVG": va, "VOLUME_TOTAL": k["VOLUME_TOTAL"], "VOLUME_TREND": trd,
               "MOMENTUM_SCORE": mom, "INCLUSION_STATUS": gate, "BREAKOUT_FLAG": breakout, "CHANGE_CLASS": cc})
tm.sort(key=lambda x: -x["MOMENTUM_SCORE"])
wcsv(GOLD / "TREND_MONITOR.csv",
     ["CANONICAL_KEYWORD", "SEGMENT", "VOLUME_AVG", "VOLUME_TOTAL", "VOLUME_TREND", "MOMENTUM_SCORE",
      "INCLUSION_STATUS", "BREAKOUT_FLAG", "CHANGE_CLASS"], tm)

# MONTHLY_TREND — 세그먼트×월 집계. Silver 의 빈 값(null — nv_code 만 있는 달 등, monthly.py)은
# 0 으로 세지 않고 건너뛴다: 합계는 "값이 있는 키워드의 합"이고, 값이 하나도 없는 칸은 빈 값으로 둔다.
mt = defaultdict(lambda: {"gg": None, "nv": None, "tot": None})
def addn(a, v):
    if v in (None, ""): return a
    return (a or 0) + float(v)
seg_of = {}
for r in master:
    _, group, disp = match_frame(r["keyword"])
    seg_of[r["keyword"]] = (disp or r["keyword"], group or "기타")
for r in monthly:
    canon, seg = seg_of.get(r["keyword"], (r["keyword"], "기타"))
    key = (seg, r.get("month", ""))
    g = mt[key]
    g["gg"] = addn(g["gg"], r.get("gg_volume")); g["nv"] = addn(g["nv"], r.get("nv_volume")); g["tot"] = addn(g["tot"], r.get("total_volume"))
rnd = lambda v, n=1: None if v is None else round(v, n)
mt_rows = [{"CANONICAL_KEYWORD": seg, "SEGMENT": seg, "MONTH": mo,
            "GG_VOLUME": None if g["gg"] is None else int(g["gg"]),
            "NV_VOLUME": rnd(g["nv"]), "TOTAL_VOLUME": rnd(g["tot"])}
           for (seg, mo), g in sorted(mt.items()) if mo]
wcsv(GOLD / "MONTHLY_TREND.csv",
     ["CANONICAL_KEYWORD", "SEGMENT", "MONTH", "GG_VOLUME", "NV_VOLUME", "TOTAL_VOLUME"], mt_rows)

# EXPANSION_SCORECARD = KEYWORDS_EXPANDED 동일 스키마
wcsv(GOLD / "EXPANSION_SCORECARD.csv",
     ["CANONICAL_KEYWORD", "TYPE", "DIMENSION", "VOLUME_TOTAL", "VOLUME_AVG", "VOLUME_TREND_WAVG", "NV_SHARE", "VARIANTS"],
     expanded)

# DIMENSION_CROSSTAB — canonical별 성별/시즌 카운트 (tagged)
dc = defaultdict(lambda: {"n": 0, "m": 0, "f": 0, "su": 0, "wi": 0})
for r in tagged:
    canon = r.get("CANONICAL") or r.get("KEYWORD")
    g = dc[canon]; g["n"] += 1
    if r.get("DIM_GENDER") == "M": g["m"] += 1
    if r.get("DIM_GENDER") == "F": g["f"] += 1
    if r.get("DIM_SEASON") == "여름": g["su"] += 1
    if r.get("DIM_SEASON") == "겨울": g["wi"] += 1
dc_rows = [{"CANONICAL": c, "N_KEYWORDS": g["n"], "MALE": g["m"], "FEMALE": g["f"],
            "SUMMER": g["su"], "WINTER": g["wi"]}
           for c, g in dc.items() if (g["m"] or g["f"] or g["su"] or g["wi"])]
dc_rows.sort(key=lambda x: -x["N_KEYWORDS"])
wcsv(GOLD / "DIMENSION_CROSSTAB.csv",
     ["CANONICAL", "N_KEYWORDS", "MALE", "FEMALE", "SUMMER", "WINTER"], dc_rows)

# GOLD_TAG_MOMENTUM — 차원값별 모멘텀
vol_by_kw = {r["keyword"]: (inum(r.get("volume_avg")), winsor(r.get("volume_trend"))) for r in master}
gtm = defaultdict(lambda: {"n": 0, "vsum": 0, "trw": 0.0})
for r in tagged:
    kw = r.get("KEYWORD")
    va, tr_ = vol_by_kw.get(kw, (0, 0.0))
    for axis, col in [("BODY_TYPE", "DIM_BODY"), ("OCCASION_TPO", "DIM_OCCASION"),
                      ("STYLE_MODIFIER", "DIM_MODIFIER"), ("GENDER", "DIM_GENDER"), ("SEASON", "DIM_SEASON")]:
        val = r.get(col)
        if val:
            g = gtm[(axis, val)]; g["n"] += 1; g["vsum"] += va; g["trw"] += tr_ * va
gtm_rows = []
for (axis, val), g in gtm.items():
    vs = g["vsum"] or 1
    tw = round(g["trw"] / vs, 4)
    gtm_rows.append({"CANONICAL": val, "AXIS": axis, "DIM_VALUE": val, "N_KEYWORDS": g["n"],
                     "VOL_AVG_SUM": g["vsum"], "TREND_WAVG": tw, "MOMENTUM": round(tw * math.log10(max(g["vsum"], 10)), 3)})
gtm_rows.sort(key=lambda x: -x["MOMENTUM"])
wcsv(GOLD / "GOLD_TAG_MOMENTUM.csv",
     ["CANONICAL", "AXIS", "DIM_VALUE", "N_KEYWORDS", "VOL_AVG_SUM", "TREND_WAVG", "MOMENTUM"], gtm_rows)

# BRAND_SOV — 플랫폼 제외 SOV
bsorted = sorted([b for b in brands if b["SEGMENT"] != "PLATFORM"], key=lambda x: -inum(x["VOL_AVG"]))
tot = sum(inum(b["VOL_AVG"]) for b in bsorted) or 1
cum = 0.0; bsov = []
for i, b in enumerate(bsorted, 1):
    sov = inum(b["VOL_AVG"]) / tot; prev = cum; cum += sov
    bsov.append({"RANK": i, "BRAND": b["BRAND"], "SEGMENT": b["SEGMENT"], "VOL_AVG": inum(b["VOL_AVG"]),
                 "SOV": round(sov, 4), "CUM_SOV": round(cum, 4),
                 "COVERED_80": (cum <= 0.80 or prev < 0.80), "RISING": fnum(b["TREND"]) >= 0.20, "TREND": fnum(b["TREND"])})
wcsv(GOLD / "BRAND_SOV.csv",
     ["RANK", "BRAND", "SEGMENT", "VOL_AVG", "SOV", "CUM_SOV", "COVERED_80", "RISING", "TREND"], bsov)

# BRAND_MONITOR — 델타 없음(최초 런) → NEW/STABLE
bm_rows = [{"BRAND": b["BRAND"], "SEGMENT": b["SEGMENT"], "RANK": b["RANK"], "SOV": b["SOV"],
            "CUM_SOV": b["CUM_SOV"], "TREND": b["TREND"], "RISING": b["RISING"], "COVERED_80": b["COVERED_80"],
            "RANK_DELTA": "", "SOV_DELTA": "", "CHANGE_CLASS": "NEW_BASELINE"} for b in bsov]
wcsv(GOLD / "BRAND_MONITOR.csv",
     ["BRAND", "SEGMENT", "RANK", "SOV", "CUM_SOV", "TREND", "RISING", "COVERED_80", "RANK_DELTA", "SOV_DELTA", "CHANGE_CLASS"], bm_rows)

# BRAND_TREND_LINK — 브랜드 × 스타일트렌드 교차
STYLE_TERMS = [n[2] for k, n in frame.items() if n[0] == "STYLE_TREND"]
brand_names = [b["BRAND"] for b in brands]
btl = []
for r in master:
    nk = norm(r["keyword"])
    br = next((b for b in brand_names if norm(b) in nk), None)
    st = next((s for s in STYLE_TERMS if norm(s) in nk), None)
    if br and st:
        btl.append({"BRAND": br, "TREND": st, "KEYWORD": r["keyword"],
                    "VOLUME_AVG": inum(r.get("volume_avg")), "VOLUME_TREND": winsor(r.get("volume_trend"))})
btl.sort(key=lambda x: -x["VOLUME_TREND"])
wcsv(GOLD / "BRAND_TREND_LINK.csv",
     ["BRAND", "TREND", "KEYWORD", "VOLUME_AVG", "VOLUME_TREND"], btl)

# COVERAGE_GAP_AUDIT — 프레임 노드 커버리지
master_norm = {norm(r["keyword"]): r for r in master}
cga = []
for fn, (axis, group, disp) in frame.items():
    r = master_norm.get(fn)
    if r:
        va, vt, trd = inum(r.get("volume_avg")), inum(r.get("volume_total")), winsor(r.get("volume_trend"))
        status = "COVERED"
    else:
        va = vt = 0; trd = 0.0; status = "GAP"
    tier = "HEAD" if va >= 100000 else ("MID" if va >= 10000 else "TAIL")
    cga.append({"KEYWORD": disp, "STATUS": status, "VOL_AVG": va, "VOL_TOTAL": vt, "TREND": trd,
                "TIER": tier, "SEASONAL_WINTER": 1 if any(s in disp for s in ["겨울", "패딩", "코트", "니트"]) else 0,
                "TREND_LINK": axis})
cga.sort(key=lambda x: -x["VOL_AVG"])
wcsv(GOLD / "COVERAGE_GAP_AUDIT.csv",
     ["KEYWORD", "STATUS", "VOL_AVG", "VOL_TOTAL", "TREND", "TIER", "SEASONAL_WINTER", "TREND_LINK"], cga)

# DISCOVERY_CANDIDATES_V2 — l1 후보 (seed → keyword)
dcv = []
l1p = RUN / "frame_design" / "l1_candidates.csv"
if l1p.exists():
    for r in csv.DictReader(open(l1p, encoding="utf-8-sig")):
        seed = r.get("seed") or r.get("source_keyword") or r.get("node") or ""
        kw = r.get("keyword") or r.get("candidate") or ""
        if kw: dcv.append({"SEED": seed, "KEYWORD": kw})
wcsv(GOLD / "DISCOVERY_CANDIDATES_V2.csv", ["SEED", "KEYWORD"], dcv)

# DISCOVERY_GATED — l2 게이트 결과
dg = []
l2p = RUN / "frame_design" / "l2_gate_results.csv"
if l2p.exists():
    for r in csv.DictReader(open(l2p, encoding="utf-8-sig")):
        va, trd = inum(r.get("volume_avg")), fnum(r.get("volume_trend"))
        gate = r.get("gate", "")
        dg.append({"KEYWORD": r.get("keyword", ""), "SEED": r.get("seed", ""), "VOLUME_AVG": va,
                   "VOLUME_TREND": trd, "INCLUSION_STATUS": gate,
                   "BREAKOUT": 1 if (trd >= 0.5 and va >= 100) else 0,
                   "PROMOTE": 1 if gate in ("STABLE", "EMERGING", "BREAKOUT") else 0})
wcsv(GOLD / "DISCOVERY_GATED.csv",
     ["KEYWORD", "SEED", "VOLUME_AVG", "VOLUME_TREND", "INCLUSION_STATUS", "BREAKOUT", "PROMOTE"], dg)

# NEW_TREND_CANDIDATES — 프레임 밖 breakout
ntc = []
for r in master:
    axis, _, _ = match_frame(r["keyword"])
    va, trd = inum(r.get("volume_avg")), winsor(r.get("volume_trend"))
    if axis is None and trd >= 0.5 and va >= 5000:
        ntc.append({"CONCEPT": r["keyword"], "IN_PANEL": False, "VOL_AVG": va, "TREND": trd,
                    "VERDICT": "PROMOTE" if va >= 20000 else "WATCH", "RECOVERS_GAP": False})
ntc.sort(key=lambda x: -x["VOL_AVG"]); ntc = ntc[:200]
wcsv(GOLD / "NEW_TREND_CANDIDATES.csv",
     ["CONCEPT", "IN_PANEL", "VOL_AVG", "TREND", "VERDICT", "RECOVERS_GAP"], ntc)

# UNKNOWN_UNKNOWNS — 카테고리 택소노미 밖에서 발굴된 차원 개념 (ref/dimensions)
DIMJSON = json.load(open(config.DIMENSIONS_JSON, encoding="utf-8"))["dimensions"]
tagged_concepts = Counter()
for r in tagged:
    for col in ("DIM_BODY", "DIM_OCCASION", "DIM_MODIFIER"):
        if r.get(col): tagged_concepts[r[col]] += 1
uu = []
for dimn, spec in DIMJSON.items():
    for member in spec["members"]:
        uu.append({"CONCEPT": member, "DIMENSION": dimn})
uu.sort(key=lambda x: -tagged_concepts.get(x["CONCEPT"], 0))
wcsv(GOLD / "UNKNOWN_UNKNOWNS.csv", ["CONCEPT", "DIMENSION"], uu)

# ============================================================
# COST
# ============================================================
print("\n[COST]")
ledger = []
for lf in sorted(glob.glob(str(BRONZE / "ledger" / "call_*.json"))):
    try: ledger.append(json.load(open(lf)))
    except: pass

acl = [{"CALL_SEQ": d.get("call_seq"), "RUN_ID": d.get("run_id"), "GL": d.get("gl"), "PHASE": d.get("phase"),
        "API": d.get("api"), "PURPOSE": d.get("purpose"), "PARAMS": d.get("params"),
        "INPUT_COST": d.get("input_cost"), "INPUT_COUNT": d.get("input_count"),
        "OUTPUT_COST": d.get("output_cost"), "OUTPUT_COUNT": d.get("output_count"),
        "TOTAL_COST": d.get("total_cost"), "RETURNED": d.get("returned"), "REQUEST_AT": d.get("request_at")}
       for d in sorted(ledger, key=lambda x: x.get("call_seq", 0))]
wcsv(COST / "API_CALL_LOG.csv",
     ["CALL_SEQ", "RUN_ID", "GL", "PHASE", "API", "PURPOSE", "PARAMS", "INPUT_COST", "INPUT_COUNT",
      "OUTPUT_COST", "OUTPUT_COUNT", "TOTAL_COST", "RETURNED", "REQUEST_AT"], acl)

# COST_SUMMARY — phase/api 차원 집계
total_credits = sum(inum(d.get("total_cost")) for d in ledger) or 1
cs = []
for dim, keyf in [("PHASE", "phase"), ("API", "api")]:
    agg = defaultdict(lambda: [0, 0])
    for d in ledger:
        k = d.get(keyf, "") or ""; agg[k][0] += 1; agg[k][1] += inum(d.get("total_cost"))
    for k, (calls, cr) in sorted(agg.items(), key=lambda x: -x[1][1]):
        cs.append({"DIMENSION": dim, "KEY": k, "CALLS": calls, "CREDITS": cr, "PCT": round(cr / total_credits * 100, 2)})
cs.append({"DIMENSION": "TOTAL", "KEY": "ALL", "CALLS": len(ledger), "CREDITS": total_credits, "PCT": 100.0})
wcsv(COST / "COST_SUMMARY.csv", ["DIMENSION", "KEY", "CALLS", "CREDITS", "PCT"], cs)

# ============================================================
# META
# ============================================================
print("\n[META]")
rm_payload = {"tables": 26, "silver_keywords": len(master), "l3_batches": 310,
              "noise_removed": 90, "credits_total": total_credits}
wcsv(META / "RUN_MANIFEST.csv",
     ["RUN_ID", "GL", "RUN_DATE", "REF_VERSION", "TOTAL_API_CREDITS", "LOADED_AT", "PAYLOAD"],
     [{"RUN_ID": RUN_ID, "GL": GL, "RUN_DATE": RUN_DATE, "REF_VERSION": "2.0.0",
       "TOTAL_API_CREDITS": total_credits, "LOADED_AT": f"{RUN_DATE}T00:00:00Z",
       "PAYLOAD": json.dumps(rm_payload, ensure_ascii=False)}])

sd_payload = {"frame_id": f"{config.CATEGORY_KEY}_kr_frame", "version": "2.0.0", "nodes": len(frame),
              "axes": list({v[0] for v in frame.values()})}
wcsv(META / "SEED_DESIGN.csv",
     ["RUN_ID", "SOURCE_FILE", "LOADED_AT", "PAYLOAD"],
     [{"RUN_ID": RUN_ID, "SOURCE_FILE": f"ref/frame/{config.CATEGORY_KEY}_kr_frame.json",
       "LOADED_AT": f"{RUN_DATE}T00:00:00Z", "PAYLOAD": json.dumps(sd_payload, ensure_ascii=False)}])

print("\n" + "=" * 70)
print("✓ 전체 카탈로그 20종 생성 완료!")
print("=" * 70)
