#!/usr/bin/env python3
# ============================================================================
# run_pipeline.py — MI 파이프라인 전체 오케스트레이터 (Python 드라이버)
#
# 시드 디자인 → 수집 → 로컬 적재(00~08+mart) 를 단일 실행.
# Databricks 플랫폼 적재는 배제 — 스키마 테이블은 프로젝트 폴더에 전량 적재한다:
#   projects/{gl}_{cat}_{run_date}/tables/{schema}/{table}.csv
# (변환 로직: scripts/local_engine.py — 정본 스키마 conform)
#
# 사용:
#   python scripts/run_pipeline.py --category vacuum-cleaner --run-date 2026-06 \
#          --seeds 로봇청소기 무선청소기 스틱청소기 \
#          --cluster-centers 로봇청소기 무선청소기 --path-centers 로봇청소기
#   (실측 시드 설계) --seed-design seed_design.json   # 실측 시드 설계(L1~L5 레이어) — --seeds 대신
#   (부분 실행) --phases seed,collect,load  (콤마 구분)
#   (재적재)    --phases load   # 이미 수집된 프로젝트 재계산
#
# 센터 선정 기준 (cluster_finder/path_finder — CLAUDE.md "클러스터·경로 센터 기준"):
#   --cluster-centers: 카테고리 총칭어 1 + L1 핵심 sub_group(L4/L5 제외)별 volume 1위 각 1개
#   --path-centers:    카테고리 총칭어 1 + (선택) 브랜드 대표 1~2개
#   미지정 시 seeds[0](--seed-design 이면 volume_total 최대 L1) 단일 센터 폴백 (경고 로그 — 명시 권장)
#
# 시드 레이어 (seed_design/seed_keywords.json):
#   --seed-design  → 파일의 실측 레이어·sub_group·볼륨을 그대로 기록 (정상 경로)
#   --seeds 만     → 추정: 입력=L1 · ref/brand_rules(run 의 gl + 품목|*) 매치=L2 · 나머지=L5, label=추정·note 표기
#
# 환경변수: LM_API_KEY (ListeningMind), LM_BASE_URL
# ============================================================================

import os, re, sys, json, time, argparse, hashlib, glob, urllib.request, urllib.error

# Windows 콘솔 cp949 인코딩 에러 방지 — UTF-8 강제 + 치환
for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

# ── 상수/설정 ────────────────────────────────────────────────────────────────
ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def projects_root() -> str:
    """run 폴더 루트 — LM_PROJECTS_ROOT(스타터는 90_Output/<프로젝트>) 또는 <ROOT>/projects. 부를 때마다 계산(테스트의 ROOT 교체를 따른다)."""
    return os.environ.get("LM_PROJECTS_ROOT") or os.path.join(ROOT, "projects")
LM_BASE  = os.environ.get("LM_BASE_URL", "https://listeningmind-data-api.ascentlab.io").rstrip("/")
LM_KEY   = os.environ.get("LM_API_KEY") or os.environ.get("LISTENINGMIND_API_KEY")

def lm(path, body, retries=3):
    for i in range(1, retries + 1):
        try:
            req = urllib.request.Request(LM_BASE + path, method="POST",
                headers={"LM-API-KEY": LM_KEY, "Authorization": "Bearer " + LM_KEY,   # 명세 인증은 LM-API-KEY · Bearer 는 하위 호환
                         "Content-Type": "application/json"},
                data=json.dumps(body).encode())
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            print(f"    [retry {i}] {str(e)[:120]}"); time.sleep(2 * i)
    raise RuntimeError(f"LM API 실패: {path}")

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def kid(k): return hashlib.sha1(k.encode("utf-8")).hexdigest()[:16]

def slug(k):
    """센터 키워드 → bronze 파일명 조각 (공백·특수문자 → _)."""
    return "".join(ch if ch.isalnum() else "_" for ch in k.strip().lower())

# ── 수집 효율 가드 (클러스터 과지출 재발 방지) ────────────────
PROBE_LIMIT = 200        # 프로브 상한 → 센터당 최대 150+50×200 = 10,150 크레딧
PROBE_TOKEN_PCT = 50.0   # 프로브 통과 기준: 카테고리 토큰 포함 키워드 비율(%)

def category_tokens(seeds, cat):
    """시드 다수(60%+)에 포함되는 최장 공통 부분문자열 → 카테고리 토큰 자동 추출.
    (us: 'tv', kr: '청소기' 등 — 언어 무관. 실패 시 category_key 폴백)"""
    if not seeds:
        return [cat.lower()]
    base = seeds[0].lower()
    best = ""
    for i in range(len(base)):
        for j in range(i + max(2, len(best) + 1), len(base) + 1):
            sub = base[i:j]
            hit = sum(1 for s in seeds if sub in s.lower())
            if hit / len(seeds) >= 0.6 and len(sub) > len(best):
                best = sub
    return [best.strip()] if best.strip() else [cat.lower()]

def load_noise_regexes(gl):
    """ref/noise_patterns/*.csv → gl 해당 패턴 목록 (universe 프리필터용 — 수집 전 차단)."""
    import csv as _csv, re as _re
    pats = []
    for fp in glob.glob(os.path.join(ROOT, "ref", "noise_patterns", "*.csv")):
        try:
            for r in _csv.DictReader(open(fp, encoding="utf-8-sig", newline="")):
                if r.get("gl") != gl or not r.get("pattern"):
                    continue
                mt = r.get("match_type") or "regex"
                if mt == "exact":
                    pats.append(("exact", r["pattern"].lower()))
                else:
                    try:
                        pats.append(("re", _re.compile(r["pattern"], _re.I)))
                    except _re.error:
                        pass
        except Exception:
            pass
    return pats

def is_noise_kw(kw, noise_pats):
    k = kw.lower()
    for mt, p in noise_pats:
        if (mt == "exact" and k == p) or (mt == "re" and p.search(k)):
            return True
    return False

def token_pct(keywords, tokens):
    if not keywords:
        return 0.0
    hit = sum(1 for k in keywords if any(t in k.lower() for t in tokens))
    return round(hit / len(keywords) * 100, 1)

# ── API 호출 로그 (metadata/api_call_log.json — cost.api_call_log 소스) ────────
def log_api_call(c, api, file_rel, resp, request_detail=None):
    """호출 1건을 metadata/api_call_log.json 에 누적 기록 (file 기준 멱등)."""
    path = os.path.join(c.pdir, "metadata", "api_call_log.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    entries = []
    if os.path.exists(path):
        try:
            entries = json.load(open(path, encoding="utf-8"))
        except Exception:
            entries = []
    entries = [e for e in entries if e.get("file") != file_rel]
    data = resp.get("data")
    entries.append({
        "api": api, "file": file_rel,
        "request_at": resp.get("request_at"), "result": resp.get("result"),
        "rows": (len(data) if isinstance(data, (list, dict)) else None),
        "cost_detail": resp.get("cost_detail"),
        "used_credits_snapshot": resp.get("used_credits"),
        "request_detail": request_detail or resp.get("request_detail")})
    json.dump(entries, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def save_usage_snapshot(c):
    """/usage/summary 스냅샷 → metadata/usage_summary.json (usage.* 4종 소스, 실패 시 스킵)."""
    j = None
    try:
        j = lm("/usage/summary", {}, retries=1)
    except Exception:
        try:  # 일부 배포는 GET 전용
            req = urllib.request.Request(LM_BASE + "/usage/summary",
                headers={"LM-API-KEY": LM_KEY, "Authorization": "Bearer " + LM_KEY})
            with urllib.request.urlopen(req, timeout=60) as r:
                j = json.loads(r.read().decode("utf-8"))
        except Exception as e:
            log(f"USAGE: /usage/summary 실패(스킵 — load 는 call log 집계 폴백): {str(e)[:100]}")
            return
    path = os.path.join(c.pdir, "metadata", "usage_summary.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(j, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    log("USAGE: metadata/usage_summary.json 저장")

# ── 시드 설계 입력 (--seed-design) · 레이어 추정 (--seeds) ─────────────────────
LAYERS = ("L1", "L2", "L3", "L4", "L5")   # ref/seed_layer_config 정의 — L3 속성·기능 · L4 사용맥락·니즈
# 시드 설계의 seed_layers 키 → 파이프라인 레이어. 두 체계는 L3·L4 정의가 엇갈린다
# (스킬 L3 Intent/Occasion = 파이프라인 L4 사용맥락·니즈 · 스킬 L4 Attribute/Modifier = 파이프라인 L3 속성·기능)
SKILL_LAYER_MAP = {
    "L1_product_category": "L1", "L2_brand": "L2", "L3_intent_occasion": "L4",
    "L4_attribute_modifier": "L3", "L5_problem_solution": "L5",
}
EST_NOTE = "레이어 추정(실측 아님)"

def _int_or_none(v):
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    return int(round(float(v)))

def _float_or_none(v):
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    return float(v)

def load_seed_design(path):
    """--seed-design 파일 → (정규화된 시드 행 목록, 메타).

    받는 형식 (모두 keyword·layer 필수, 나머지 선택):
      ① CSV / JSON 배열 — keyword, layer(L1~L5), sub_group, volume_avg, volume_total,
         volume_trend, competition, cpc, label, note
      ② JSON 객체 {"seed_keywords": [①의 행…], "gl", "denominator", "coverage_result"} — 시드 설계 결과
         (행에 layer 가 없고 skill_layer 만 있으면 SKILL_LAYER_MAP 으로 변환)
      ③ JSON 객체 {"seed_layers": {"L1_product_category": [키워드…], …}} — 시드 설계 구 형식.
         레이어만 있고 볼륨은 없다 (SKILL_LAYER_MAP 으로 변환 · note 에 원 레이어 기록)
    같은 키워드가 여러 번 나오면 첫 행만 쓴다."""
    import csv as _csv
    meta = {}
    if path.lower().endswith(".csv"):
        with open(path, encoding="utf-8-sig", newline="") as f:
            raw = list(_csv.DictReader(f))
    else:
        obj = json.load(open(path, encoding="utf-8"))
        if isinstance(obj, list):
            raw = obj
        elif isinstance(obj, dict) and isinstance(obj.get("seed_keywords"), list):
            raw, meta = obj["seed_keywords"], obj
        elif isinstance(obj, dict) and isinstance(obj.get("seed_layers"), dict):
            meta, raw = obj, []
            unknown = [k for k in obj["seed_layers"] if k not in SKILL_LAYER_MAP]
            if unknown:
                raise ValueError(f"seed_layers 알 수 없는 키 {unknown} — 허용: {list(SKILL_LAYER_MAP)}")
            for sk, kws in obj["seed_layers"].items():
                raw += [{"keyword": k, "skill_layer": sk} for k in kws or []]
        else:
            raise ValueError("시드 설계 형식을 알 수 없음 — JSON 배열, {seed_keywords: […]}, {seed_layers: {…}}, CSV 중 하나")
    rows, seen = [], {}
    for i, r in enumerate(raw):
        kw = str(r.get("keyword") or "").strip()
        if not kw:
            raise ValueError(f"{i + 1}번째 행: keyword 없음")
        layer = str(r.get("layer") or "").strip().upper()
        skill_layer = str(r.get("skill_layer") or "").strip()
        note = str(r.get("note") or "").strip()
        if not layer and skill_layer:
            if skill_layer not in SKILL_LAYER_MAP:
                raise ValueError(f"{kw!r}: 알 수 없는 skill_layer {skill_layer!r}")
            layer = SKILL_LAYER_MAP[skill_layer]
            note = "; ".join(x for x in (note, f"시드 설계 {skill_layer} → {layer}") if x)
        if layer not in LAYERS:
            raise ValueError(f"{kw!r}: layer {layer or '(없음)'} — L1~L5 중 하나여야 함")
        if kw in seen:
            if seen[kw] != layer:
                log(f"[WARN] 시드 설계: {kw!r} 중복 ({seen[kw]}·{layer}) — 첫 행 {seen[kw]} 사용")
            continue
        seen[kw] = layer
        rows.append({
            "layer": layer, "sub_group": str(r.get("sub_group") or "").strip(), "keyword": kw,
            "volume_avg": _int_or_none(r.get("volume_avg")),
            "volume_total": _int_or_none(r.get("volume_total")),
            "volume_trend": _float_or_none(r.get("volume_trend")),
            "competition": (str(r["competition"]).strip() or None) if r.get("competition") is not None else None,
            "cpc": _float_or_none(r.get("cpc")),
            "label": str(r.get("label") or "").strip() or "실제데이터",
            "note": note,
        })
    if not rows:
        raise ValueError("시드 설계에 키워드가 없음")
    return rows, meta

def load_brand_rules(gl, cat):
    """ref/brand_rules → 이 run 범위(gl 일치 · category_key ∈ {cat,'*'}) 규칙.
    local_engine._tag_dimension 과 같은 순서(priority 내림차순 → 긴 패턴 우선)로 정렬해 돌려준다."""
    import csv as _csv
    out = []
    for fp in sorted(glob.glob(os.path.join(ROOT, "ref", "brand_rules", "*.csv"))):
        with open(fp, encoding="utf-8-sig", newline="") as f:
            for r in _csv.DictReader(f):
                if (r.get("gl") or "").strip() != gl or (r.get("category_key") or "*").strip() not in (cat, "*"):
                    continue
                if not (r.get("pattern") or "") or not (r.get("brand") or "").strip():
                    continue
                try:
                    pri = float(r.get("priority") or "nan")
                except ValueError:
                    pri = float("nan")
                out.append((r["pattern"], r["brand"].strip(), pri))
    # NaN priority 는 맨 뒤 (local_engine: na_position="last")
    out.sort(key=lambda t: (t[2] == t[2], t[2] if t[2] == t[2] else 0, len(t[0])), reverse=True)
    return out

def match_brand(kw, rules):
    """첫 매치 브랜드 (Generic·미매치 → None). local_engine 과 같이 regex·대소문자 구분."""
    import re as _re
    for pat, brand, _ in rules:
        try:
            if _re.search(pat, kw):
                return None if brand.lower() == "generic" else brand
        except _re.error:
            continue
    return None

def build_seed_keywords(run_id, gl, cat, universe, seeds, seed_rows=None, brand_rules=None):
    """seed_design/seed_keywords.json 행.
    seed_rows(--seed-design) 가 있으면 그 실측 레이어를 그대로 쓴다 — 유니버스 확장어는 시드가 아니라 넣지 않는다.
    없으면(--seeds 만) 유니버스 전체에 레이어를 추정하고 label=추정 · note 로 표시한다."""
    base = {"run_id": run_id, "gl": gl, "market": cat}
    if seed_rows:
        return [{**base, **r} for r in seed_rows]
    seeds, rules, sk = set(seeds), brand_rules or [], []
    for kw in universe:
        if kw in seeds:
            layer, why = "L1", "입력 시드 → L1"
        else:
            b = match_brand(kw, rules)
            layer, why = ("L2", f"brand_rules {b} → L2") if b else \
                         ("L5", "미배정 → L5 기본값 (L3·L4 판정 없음 — --seed-design 권장)")
        sk.append({**base, "layer": layer, "sub_group": "", "keyword": kw, "volume_avg": None,
                   "volume_total": None, "volume_trend": None, "competition": None, "cpc": None,
                   "label": "추정", "note": f"{EST_NOTE} — {why}"})
    return sk

def _coverage_pct(meta):
    """시드 설계 coverage_result 의 마지막 누적값("82%") → 82.0 (없거나 숫자가 아니면 None)."""
    cr = (meta or {}).get("coverage_result") or {}
    for k in ("L5_cumulative", "L4_cumulative", "L3_cumulative", "L2_cumulative", "L1_cumulative"):
        m = re.match(r"\s*([\d.]+)\s*%?\s*$", str(cr.get(k) or ""))
        if m:
            return float(m.group(1))
    return None

# ── 경로 헬퍼 ────────────────────────────────────────────────────────────────
class Ctx:
    def __init__(self, gl, cat, run_date, seeds, cluster_centers=None, path_centers=None,
                 cluster_limit=2000, cat_tokens=None, seed_rows=None, seed_meta=None):
        self.seed_rows, self.seed_meta = seed_rows, seed_meta or {}
        if seed_rows:   # --seed-design: 시드 = 설계 키워드 전체 (L1~L5)
            seeds = [r["keyword"] for r in seed_rows]
            l1 = sorted((r for r in seed_rows if r["layer"] == "L1"),
                        key=lambda r: -(r["volume_total"] or 0))
            core = [r["keyword"] for r in l1] or seeds
        else:
            core = seeds
        self.gl, self.cat, self.run_date, self.seeds = gl, cat, run_date, seeds
        # 센터 미지정 → 코어 시드 1개 폴백 (커버리지 저하 — 명시 권장)
        self.cluster_centers = cluster_centers or (core[:1] if core else [])
        self.path_centers = path_centers or self.cluster_centers[:1]
        self.cluster_limit = cluster_limit   # 출력행 과금(50/행) 상한 → 센터당 ≤ 150+50×limit
        # 카테고리 토큰은 코어(L1) 시드에서 — 브랜드·롱테일 시드가 섞이면 공통 부분문자열이 흐려진다
        self.cat_tokens = cat_tokens or category_tokens(core, cat)
        if not cluster_centers and core:
            log(f"[WARN] --cluster-centers 미지정 → {core[0]!r} 단일 센터 폴백 "
                f"(볼륨 커버리지 저하 위험 — 총칭어+sub_group 대표 명시 권장)")
        self.proj = f"{gl}_{cat}_{run_date}"
        self.pdir = os.path.join(projects_root(), self.proj)
    def bdir(self, api): return os.path.join(self.pdir, "bronze", api)
    def sdir(self): return os.path.join(self.pdir, "seed_design")

# ── PHASE: seed ─────────────────────────────────────────────────────────────
def phase_seed(c):
    """시드 디자인: 유니버스 확장(intent+cluster) → seed_design 산출물 + universe."""
    log("SEED: 유니버스 확장 (intent_finder + cluster_finder rels)")
    os.makedirs(c.sdir(), exist_ok=True)
    for api in ("intent_finder", "cluster_finder", "path_finder"):
        os.makedirs(c.bdir(api), exist_ok=True)

    # intent_finder — 전 시드 연관어 (bronze 원본 겸 시드 L1~L5 원천)
    # API 0.0.50 부터 /intent_finder → /intent_finder/keyword_list 로 라우트 변경 (구경로 405)
    _rq = {"keywords": c.seeds, "gl": c.gl, "limit": 10000, "volume_threshold": 10}
    jif = lm("/intent_finder/keyword_list", _rq)
    json.dump(jif, open(os.path.join(c.bdir("intent_finder"), "intent_finder_raw.json"), "w", encoding="utf-8"), ensure_ascii=False)
    log_api_call(c, "intent_finder", "bronze/intent_finder/intent_finder_raw.json", jif, _rq)
    universe = set(c.seeds) | set(jif.get("data", []))

    # cluster_finder all — 센터별 호출: communities(군집→cluster_master/membership) + rels(관계 확장)
    # community_id 는 적재 시 "{center}:{cid}" 네임스페이스로 병합 (local_engine parse_to_silver)
    # 비용 가드: ① limit 상한(출력행 50/행 과금 → 센터당 ≤ 150+50×limit)
    #           ② 총칭어(첫 센터) 외에는 hop=1 프로브(≤10K) → 카테고리 토큰 비율 통과 시만 full
    #           ③ universe 확장은 카테고리 토큰 포함 + 노이즈 룰북 통과 키워드만 (오프카테고리 수집 차단)
    noise_pats = load_noise_regexes(c.gl)
    log(f"SEED: 카테고리 토큰={c.cat_tokens} | cluster limit={c.cluster_limit} | "
        f"노이즈 프리필터 {len(noise_pats)}패턴")
    sc_rows = []
    for ci, center in enumerate(c.cluster_centers):
        if ci > 0:  # 보완 센터는 프로브 게이트
            _rq = {"keyword": center, "gl": c.gl, "data_type": "all", "hop": 1, "limit": PROBE_LIMIT}
            jp = lm("/cluster_finder", _rq)
            pdir = os.path.join(c.pdir, "metadata", "cluster_probes")
            os.makedirs(pdir, exist_ok=True)
            pfn = f"probe_{slug(center)}.json"
            json.dump(jp, open(os.path.join(pdir, pfn), "w", encoding="utf-8"), ensure_ascii=False)
            log_api_call(c, "cluster_finder", f"metadata/cluster_probes/{pfn}", jp, _rq)
            pkw = sorted({k.strip() for e in ((jp.get("data") or {}).get("rels") or [])
                          for k in e if k})
            pct = token_pct(pkw, c.cat_tokens)
            if pct < PROBE_TOKEN_PCT:
                log(f"SEED: cluster_finder [{center}] 프로브 탈락 — 카테고리 토큰 {pct}% "
                    f"(< {PROBE_TOKEN_PCT}%) → full 수집 생략 (오프카테고리 네트워크)")
                continue
            log(f"SEED: cluster_finder [{center}] 프로브 통과 — 카테고리 토큰 {pct}%")
        _rq = {"keyword": center, "gl": c.gl, "data_type": "all", "hop": 2, "limit": c.cluster_limit}
        jcf = lm("/cluster_finder", _rq)
        fn = f"cluster_finder_{slug(center)}.json"
        json.dump(jcf, open(os.path.join(c.bdir("cluster_finder"), fn), "w", encoding="utf-8"), ensure_ascii=False)
        log_api_call(c, "cluster_finder", f"bronze/cluster_finder/{fn}", jcf, _rq)
        data = jcf.get("data") or {}
        rels = data.get("rels") or []
        skipped = 0
        for e in rels:
            for k in e:
                k = (k or "").strip()
                if not k:
                    continue
                if any(t in k.lower() for t in c.cat_tokens) and not is_noise_kw(k, noise_pats):
                    universe.add(k)
                else:
                    skipped += 1
        if skipped:
            log(f"SEED: [{center}] universe 프리필터 — {skipped}개 제외 (오프카테고리/노이즈)")
        # seed_communities 산출 — source_keyword=센터, hop=BFS 거리, weight=관계 차수
        adj, deg = {}, {}
        for e in rels:
            ks = [k.strip() for k in e if k]
            for k in ks:
                deg[k] = deg.get(k, 0) + 1
            for a in ks:
                for b in ks:
                    if a != b:
                        adj.setdefault(a, set()).add(b)
        hops, frontier = {center: 0}, [center]
        for h in (1, 2):
            nxt = []
            for u in frontier:
                for v in adj.get(u, ()):
                    if v not in hops:
                        hops[v] = h
                        nxt.append(v)
            frontier = nxt
        for cid, kws in (data.get("communities") or {}).items():
            for k in kws:
                if k:
                    sc_rows.append({"gl": c.gl, "market": c.cat, "source_keyword": center,
                                    "community_id": f"{center}:{cid}", "keyword": k,
                                    "weight": float(deg.get(k, 1)), "hop": hops.get(k)})
        log(f"SEED: cluster_finder [{center}] communities {len(data.get('communities') or {})}"
            f"·rels {len(rels)}")

    universe = sorted(x for x in universe if x)
    open(os.path.join(c.sdir(), "keyword_universe.txt"), "w", encoding="utf-8").write("\n".join(universe))
    log(f"SEED: 유니버스 {len(universe):,}개")

    # seed_design 산출물 (08 이 적재) — L1~L5 레이어링 + run 메타
    run_id = f"seed-{c.gl}-{c.run_date.replace('-','')}-01"
    if c.seed_rows:
        sk = build_seed_keywords(run_id, c.gl, c.cat, universe, c.seeds, seed_rows=c.seed_rows)
        log(f"SEED: seed_keywords = 시드 설계 실측 레이어 {len(sk)}개 "
            f"({', '.join('%s %d' % (l, sum(r['layer'] == l for r in sk)) for l in LAYERS)}) — "
            f"유니버스 확장어 {len(set(universe) - set(c.seeds)):,}개는 수집 대상일 뿐 시드로 기록하지 않음")
    else:
        rules = load_brand_rules(c.gl, c.cat)
        sk = build_seed_keywords(run_id, c.gl, c.cat, universe, c.seeds, brand_rules=rules)
        log(f"SEED: [WARN] --seed-design 없음 → 레이어 추정 (brand_rules {len(rules)}규칙 · gl={c.gl} · "
            f"품목={c.cat}|*) — L1 {sum(r['layer'] == 'L1' for r in sk)} · L2 {sum(r['layer'] == 'L2' for r in sk)} "
            f"· L5(미배정) {sum(r['layer'] == 'L5' for r in sk)}, label=추정")
    den = c.seed_meta.get("denominator") or {}
    cov = _coverage_pct(c.seed_meta)
    _write_json_lines(os.path.join(c.sdir(), "seed_keywords.json"), sk)
    _write_json_lines(os.path.join(c.sdir(), "seed_runs.json"), [{
        "run_id": run_id, "market": c.cat, "gl": c.gl, "created_at": c.run_date + "-01",
        "skill_version": "seed-design" if c.seed_rows else "orchestrator-v1", "total_seeds": len(sk),
        "coverage_pct": cov, "coverage_label": "파생" if cov is not None else "추정",
        "v_denominator_keyword": den.get("keyword"), "v_denominator_volume": _int_or_none(den.get("volume_total")),
        "used_credits": jif.get("used_credits"),
    }])
    _write_json_lines(os.path.join(c.sdir(), "seed_communities.json"),
                      [{"run_id": run_id, **r} for r in sc_rows])
    log(f"SEED: seed_design 산출물 생성 (seed_keywords {len(sk)}, seed_runs 1, "
        f"seed_communities {len(sc_rows)})")
    return universe

def _write_json_lines(path, rows):
    # 08_ingest_seed_design 의 read_seed 는 multiLine=True 로 읽으므로
    # JSON Lines(NDJSON)가 아니라 JSON 배열로 저장해야 전 레코드가 파싱된다.
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)

# ── PHASE: collect ──────────────────────────────────────────────────────────
def phase_collect(c, batch=1000):
    """keyword_info(all) 배치 + path_finder → bronze."""
    uni_file = os.path.join(c.sdir(), "keyword_universe.txt")
    kws = [l.strip() for l in open(uni_file, encoding="utf-8") if l.strip()]
    os.makedirs(c.bdir("keyword_info"), exist_ok=True)
    batches = [kws[i:i+batch] for i in range(0, len(kws), batch)]
    log(f"COLLECT: keyword_info {len(kws):,}개 → {len(batches)}배치")
    total = 0
    for bi, b in enumerate(batches, 1):
        j = lm("/keyword_info", {"keywords": b, "gl": c.gl, "data_type": "all"})
        fn = f"keyword_info_batch_{bi:03d}.json"
        json.dump(j, open(os.path.join(c.bdir("keyword_info"), fn), "w", encoding="utf-8"), ensure_ascii=False)
        log_api_call(c, "keyword_info", f"bronze/keyword_info/{fn}", j,
                     {"keywords": f"{len(b)}건", "gl": c.gl, "data_type": "all"})
        total += (j.get("cost_detail") or {}).get("total_cost", 0)
        log(f"  배치 {bi}/{len(batches)}: {len(j.get('data',[]))}행 (cost {(j.get('cost_detail') or {}).get('total_cost')})")
    # path_finder — 센터별 호출 (여정 시작점: 총칭어 + 브랜드 대표), journey_analysis 로 병합
    os.makedirs(c.bdir("path_finder"), exist_ok=True)
    n_paths = 0
    for center in c.path_centers:
        _rq = {"keyword": center, "gl": c.gl, "limit": 300}
        jp = lm("/path_finder", _rq)
        fn = f"path_finder_{slug(center)}.json"
        json.dump(jp, open(os.path.join(c.bdir("path_finder"), fn), "w", encoding="utf-8"), ensure_ascii=False)
        log_api_call(c, "path_finder", f"bronze/path_finder/{fn}", jp, _rq)
        n_paths += len(jp.get("data", []))
    log(f"COLLECT: path_finder {len(c.path_centers)}센터 {n_paths}경로 | 총 keyword_info cost {total:,}")
    # 계정 사용량 스냅샷 (usage.* 4종 소스)
    save_usage_snapshot(c)

# ── PHASE: load ─────────────────────────────────────────────────────────────
def phase_load(c):
    """로컬 적재 엔진 — 00~08+mart 등가 변환을 projects/{proj}/tables/ 에 적재."""
    log("LOAD: 로컬 적재 엔진 (00 ref → 01 bronze → 01b parse → 02 silver "
        "→ 03 gold → 08 seed → 04 qc → 05 reports → mart)")
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import local_engine
    local_engine.run(c.gl, c.cat, c.run_date)

# ── main ────────────────────────────────────────────────────────────────────
PHASES = ("seed", "collect", "load")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gl", default="kr")
    ap.add_argument("--category", required=True)
    ap.add_argument("--run-date", required=True, help="YYYY-MM")
    ap.add_argument("--seeds", nargs="+", default=[],
                    help="평평한 시드 목록 (하위 호환 — 레이어는 추정: 입력=L1 · brand_rules=L2 · 나머지=L5)")
    ap.add_argument("--seed-design", default=None,
                    help="실측 시드 설계 파일 (JSON/CSV: keyword, layer L1~L5, sub_group, volume_* …) — "
                         "시드 설계(실측 재구성) 산출. 시드 목록이 되고 seed_keywords 에 레이어 그대로 기록")
    ap.add_argument("--cluster-centers", nargs="+", default=None,
                    help="cluster_finder 센터 키워드 목록 — 총칭어 1 + L1 핵심 sub_group별 volume 1위 (L4/L5 제외)")
    ap.add_argument("--path-centers", nargs="+", default=None,
                    help="path_finder 센터 키워드 목록 — 총칭어 1 + 브랜드 대표 1~2 (미지정 시 cluster-centers[0])")
    ap.add_argument("--cluster-limit", type=int, default=2000,
                    help="cluster_finder limit — 비용 상한: 센터당 ≤ 150+50×limit (기본 2000 → ≤100K)")
    ap.add_argument("--category-tokens", nargs="+", default=None,
                    help="프로브/universe 프리필터용 카테고리 토큰 (미지정 시 시드 공통 부분문자열 자동 추출)")
    ap.add_argument("--phases", default="seed,collect,load")
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args()

    phases = [p.strip() for p in args.phases.split(",")]
    unknown = [p for p in phases if p not in PHASES]
    if unknown:
        sys.exit(f"알 수 없는 phase: {unknown} — 허용: {list(PHASES)} "
                 f"(Databricks 단계 upload/compute/dag/mart/cleanup 는 배제됨 → load 로 대체)")
    if any(p in phases for p in ("seed", "collect")) and not LM_KEY:
        sys.exit("환경변수 LM_API_KEY 필요 (seed/collect 단계)")
    if args.seeds and args.seed_design:
        sys.exit("--seeds 와 --seed-design 은 함께 쓸 수 없음 — 시드는 설계 파일에 넣을 것")
    if "seed" in phases and not (args.seeds or args.seed_design):
        sys.exit("--seed-design(권장) 또는 --seeds 필요 (seed 단계)")
    seed_rows, seed_meta = None, None
    if args.seed_design:
        try:
            seed_rows, seed_meta = load_seed_design(args.seed_design)
        except (OSError, ValueError) as e:
            sys.exit(f"--seed-design 읽기 실패: {e}")
        if seed_meta.get("gl") and seed_meta["gl"] != args.gl:
            sys.exit(f"--seed-design gl={seed_meta['gl']} ≠ --gl {args.gl}")

    c = Ctx(args.gl, args.category, args.run_date, args.seeds,
            args.cluster_centers, args.path_centers,
            args.cluster_limit, args.category_tokens, seed_rows, seed_meta)
    log(f"=== 파이프라인 시작: {c.proj} | phases={phases} ===")
    if "seed" in phases:    phase_seed(c)
    if "collect" in phases: phase_collect(c, args.batch)
    if "load" in phases:    phase_load(c)
    log(f"=== 완료: {c.proj} ===")

if __name__ == "__main__":
    main()
