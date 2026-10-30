# ============================================================================
# series_dedup.py — 같은 검색의 중복 행 판정 (MI 로컬 엔진 [02] build_silver 에서 호출)
#
# keyword_info 는 요청하지 않은 표기변형(복수형·어순·동의어·오타)을 함께 돌려준다(실측 예:
# 요청 1,000 → 응답 1,379행). 그중 월별 시계열(month × total·gg·nv)이 완전히 같은 행은 LM 이 같은
# 검색으로 보고 같은 볼륨을 복제해 준 것이다 — Gold 에서 행마다 더하면 한 검색을 N번 센다
# (실측 사례: 브랜드마다 복제 배수가 달라 점유율이 왜곡됐다).
#
# 규칙 (시계열 없는 행은 묶지 않는다):
#   1. 같은 run 안에서 시계열이 완전히 같은 행 → 한 묶음. 대표 1행만 Gold 가 센다.
#      - 시계열 합계가 MIN_SERIES_TOTAL 미만이면 버킷 반올림(월 10건 바닥)으로 무관 키워드가 우연히
#        같을 수 있다 → 대표와 토큰이 겹치거나 띄어쓰기 제거 표기가 같을 때만 묶는다.
#        (실측 예: 무관한 두 저볼륨 키워드가 같은 시계열)
#   2. 시계열이 없거나 합계 0 인 행은 묶지 않는다. 지표(volume_avg·total·trend) 일치만으로는 묶지 않는다 —
#      실측에서 지표가 같은 3,894묶음 중 816묶음은 시계열이 달랐다(버킷 반올림).
#   3. 묶음은 브랜드·세그먼트·노이즈를 가리지 않고 run 전체에서 만든다.
#      대표 우선순위: 노이즈 아님 > 브랜드 태깅됨(Generic 아님) > 기본 세그먼트 아님 > 짧은 표기 > 사전순.
#      → 'somy tv'(Generic)·'sony tv'(Sony) 는 Sony 로 한 번만 센다. 구성원 중 하나라도 노이즈가
#        아니면 대표가 노이즈 아님이 되어 묶음이 집계된다.
#   [추정] 규칙 기반 판정이다 — Silver 행은 지우지 않고 canonical_keyword·is_variant 로 표시만 한다.
# ============================================================================
import re
import unicodedata

import pandas as pd

MIN_SERIES_TOTAL = 1000   # [가정] 48개월 합계가 이보다 작으면 토큰 겹침을 추가로 요구 (oss 초안과 같은 값)


def canon(kw):
    """NFKC · 소문자 · 공백 제거 — 띄어쓰기 변형 판정용."""
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(kw)).lower())


def tokens(kw):
    """공백 토큰 집합 — 영어 복수형 's' 는 떼고 비교한다."""
    t = re.split(r"\s+", unicodedata.normalize("NFKC", str(kw)).lower().strip())
    return {w[:-1] if len(w) > 3 and w.endswith("s") else w for w in t if w}


def series_signatures(monthly):
    """keyword_id → (시계열 서명 문자열, 시계열 total 합계). 시계열이 없거나 합계 0 이면 빠진다."""
    if monthly is None or len(monthly) == 0:
        return {}
    mo = pd.DataFrame(monthly)[["keyword_id", "month", "total_volume", "gg_volume", "nv_volume"]]
    mo = mo.sort_values(["keyword_id", "month"])
    num = lambda c: mo[c].map(lambda v: "" if pd.isna(v) else repr(float(v)))
    mo = mo.assign(_c=mo["month"].astype(str) + ":" + num("total_volume") + "/" + num("gg_volume")
                   + "/" + num("nv_volume"))
    g = mo.groupby("keyword_id", sort=False)
    sig = g["_c"].agg("|".join)
    tot = g["total_volume"].sum(min_count=1).fillna(0)
    return {k: (sig[k], float(tot[k])) for k in sig.index if tot[k] > 0}


def dedup_groups(km, monthly, default_segments=(), min_series_total=MIN_SERIES_TOTAL):
    """keywords_master 프레임 → (canonical_keyword Series, is_variant Series, 요약 dict). km 인덱스 정렬.

    km: keyword_id·keyword·is_noise·brand·category_lvl2 컬럼. monthly: keyword_monthly 행(list/DataFrame).
    """
    return group_by_signature(km, series_signatures(monthly), default_segments, min_series_total)


def group_by_signature(km, sigs, default_segments=(), min_series_total=MIN_SERIES_TOTAL):
    """서명이 이미 계산된 경우(서명을 밖에서 만든 경우 — 예: 분산 처리에서 후보 행만 모은다) 묶음 판정만 한다.

    sigs: keyword_id → (서명, 시계열 total 합계). 서명 형식은 run 안에서 결정적이기만 하면 된다.
    """
    canonical = km["keyword"].copy()
    is_variant = pd.Series(False, index=km.index)
    if not sigs or not len(km):
        return canonical, is_variant, {"groups": 0, "variant_rows": 0}
    default_segments = set(default_segments or ())
    noise = km["is_noise"].fillna(False).astype(bool) if "is_noise" in km.columns \
        else pd.Series(False, index=km.index)
    brand = km["brand"] if "brand" in km.columns else pd.Series(None, index=km.index)
    seg = km["category_lvl2"] if "category_lvl2" in km.columns else pd.Series(None, index=km.index)

    buckets = {}
    for idx, kid in km["keyword_id"].items():
        s = sigs.get(kid)
        if s is not None:
            buckets.setdefault(s[0], []).append(idx)

    n_groups = 0
    for sig, members in buckets.items():
        if len(members) < 2:
            continue
        strong = sigs[km.at[members[0], "keyword_id"]][1] >= min_series_total
        members.sort(key=lambda i: (bool(noise[i]), pd.isna(brand[i]) or brand[i] == "Generic",
                                    seg[i] in default_segments, len(str(km.at[i, "keyword"])),
                                    str(km.at[i, "keyword"])))
        groups = []   # [(대표 idx, 대표 토큰, 대표 canon, [구성원 idx])]
        for i in members:
            kw = km.at[i, "keyword"]
            for rep, rtok, rcan, mem in groups:
                if strong or canon(kw) == rcan or (tokens(kw) & rtok):
                    mem.append(i)
                    break
            else:
                groups.append((i, tokens(kw), canon(kw), [i]))
        for rep, _, _, mem in groups:
            if len(mem) < 2:
                continue
            n_groups += 1
            for i in mem[1:]:
                canonical[i] = km.at[rep, "keyword"]
                is_variant[i] = True
    return canonical, is_variant, {"groups": n_groups, "variant_rows": int(is_variant.sum())}
