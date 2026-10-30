#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
정제된 KEYWORD_MASTER → Snowflake 스키마에 맞게 재생성

입력:
  - KEYWORD_MASTER.csv (정제된 13,878개)
  - Bronze L3 raw 파일들

출력:
  - KEYWORDS_MASTER.csv (Snowflake 스키마)
  - KEYWORDS_MONTHLY.csv (Snowflake 스키마)
  - KEYWORDS_FEATURES.csv (Snowflake 스키마)
"""
import csv, json, gzip, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
import monthly

RAW = config.BRONZE / "raw"
SILVER = config.SILVER
SILVER.mkdir(parents=True, exist_ok=True)
COLLECTED = config.RUN_DATE

# 프레임 세그먼트 맵 (norm_keyword → group). L3 승격 키워드는 프레임 노드에 매칭 시 세그먼트 부여.
seg_map = {}
_fn = config.FRAME_DESIGN / "frame_nodes.json"
if _fn.exists():
    for n in json.load(open(_fn, encoding="utf-8")):
        seg_map[config.norm(n["keyword"])] = n.get("group", "")
def segment_of(kw):
    return seg_map.get(config.norm(kw), "")

print("=" * 70)
print(f"05 SILVER MASTER — {config.RUN_ID}")
print("=" * 70)

# Step 1: 승격 키워드 = L3 raw에 존재하는 전량 (별도 화이트리스트 불필요 — 재현성)
print("\n[1/4] L3 raw 스캔 (승격 키워드 = L3 수집분 전량)...")

# Step 2: L3에서 데이터 추출
print("\n[2/4] L3 파일에서 데이터 추출...")

master_data = {}
features_data = {}

l3_files = sorted(RAW.glob("call_*_keyword_info_l3*.json.gz"))

for file_idx, f in enumerate(l3_files):
    try:
        d = json.load(gzip.open(f, "rt", encoding="utf-8"))
    except Exception as e:
        continue

    data_items = d if isinstance(d, list) else (d.get("data") or [])

    for item in data_items:
        if not isinstance(item, dict):
            continue

        kw = item.get("keyword", "").strip()
        if not kw:
            continue

        if kw not in master_data:
            ads = item.get("ads_metrics", {})
            demo = item.get("demography", {})

            master_data[kw] = {
                "canonical_keyword": kw,
                "segment": segment_of(kw),
                "keyword": kw,
                "is_variant": 0,
                "gl": "kr",
                # 데이터 인프라 통합(2026-09-22) — mi 파이프라인과의 통합 keywords_master 계약
                # (../../contracts/keywords_master.schema.json). segment(L0 프레임 그룹)는
                # mi의 category_key(품목 키)와 다른 축이라고 실측으로 확인했다 — segment 컬럼은
                # 06/08 단계가 계속 쓰므로 이름을 안 바꾸고, category_lvl2 에 같은 값을 나란히 둔다.
                # category_key 는 이 런이 속한 품목(config.CATEGORY_KEY, 예: trend_fashion)이다.
                "pipeline_source": "trend",
                "category_key": config.CATEGORY_KEY,
                "category_lvl2": segment_of(kw),
                "volume_avg": ads.get("volume_avg", 0),
                "volume_total": ads.get("volume_total", 0),
                "volume_trend": ads.get("volume_trend", 0.0),
                "gg_volume_avg": ads.get("gg_volume_avg", 0),
                "gg_volume_total": ads.get("gg_volume_total", 0),
                "gg_volume_trend": ads.get("gg_volume_trend", 0.0),
                "nv_volume_avg": ads.get("nv_volume_avg", 0),
                "nv_volume_total": ads.get("nv_volume_total", 0),
                "nv_volume_trend": ads.get("nv_volume_trend", 0.0),
                "nv_share": 0.0,
                "intent_primary": "",
                "intent_i": item.get("intents", {}).get("i", 0),
                "intent_n": item.get("intents", {}).get("n", 0),
                "intent_c": item.get("intents", {}).get("c", 0),
                "intent_t": item.get("intents", {}).get("t", 0),
                "competition": ads.get("competition", ""),
                "competition_index": ads.get("competition_index", 0),
                "cpc": ads.get("cpc", 0.0),
                "low_bid_micros": ads.get("low_bid_micros", 0),
                "high_bid_micros": ads.get("high_bid_micros", 0),
                "gender_m_ratio": demo.get("m_m_gender_ratio", 0.0),
                "gender_f_ratio": demo.get("m_f_gender_ratio", 0.0),
                "age_0_ratio": demo.get("a0_ratio", 0.0),
                "age_13_ratio": demo.get("a13_ratio", 0.0),
                "age_20_ratio": demo.get("a20_ratio", 0.0),
                "age_25_ratio": demo.get("a25_ratio", 0.0),
                "age_30_ratio": demo.get("a30_ratio", 0.0),
                "age_40_ratio": demo.get("a40_ratio", 0.0),
                "age_50_ratio": demo.get("a50_ratio", 0.0),
                "collected_date": COLLECTED
            }

        # Features 추출
        features = item.get("features", {})
        if features and kw not in features_data:
            features_data[kw] = {
                "canonical_keyword": kw,
                "segment": segment_of(kw),
                "keyword": kw,
                "gl": "kr",
                "f_ad": features.get("f_ad", 0),
                "f_ad_bottom": features.get("f_ad_bottom", 0),
                "f_ad_top": features.get("f_ad_top", 0),
                "f_ads": features.get("f_ads", 0),
                "f_ads_bottom": features.get("f_ads_bottom", 0),
                "f_ads_top": features.get("f_ads_top", 0),
                "f_adult_certification": features.get("f_adult_certification", 0),
                "f_ai_overview": features.get("f_ai_overview", 0),
                "f_app_results": features.get("f_app_results", 0),
                "f_articles": features.get("f_articles", 0),
                "f_broaden_searches": features.get("f_broaden_searches", 0),
                "f_discover_more_places": features.get("f_discover_more_places", 0),
                "f_featured_snippet": features.get("f_featured_snippet", 0),
                "f_featured_snippet_ordered_list": features.get("f_featured_snippet_ordered_list", 0),
                "f_featured_snippet_table": features.get("f_featured_snippet_table", 0),
                "f_featured_snippet_unordered_list": features.get("f_featured_snippet_unordered_list", 0),
                "f_featured_snippet_video_exist": features.get("f_featured_snippet_video_exist", 0),
                "f_google_play": features.get("f_google_play", 0),
                "f_images": features.get("f_images", 0),
                "f_job_search": features.get("f_job_search", 0),
                "f_knowledge_panel": features.get("f_knowledge_panel", 0),
                "f_knowledge_panel_appointment": features.get("f_knowledge_panel_appointment", 0),
                "f_knowledge_panel_local": features.get("f_knowledge_panel_local", 0),
                "f_local_results": features.get("f_local_results", 0),
                "f_organic_results": features.get("f_organic_results", 0),
                "f_organic_results_rating": features.get("f_organic_results_rating", 0),
                "f_organic_results_video": features.get("f_organic_results_video", 0),
                "f_others": features.get("f_others", 0),
                "f_people_also_ask_for": features.get("f_people_also_ask_for", 0),
                "f_people_also_search_for": features.get("f_people_also_search_for", 0),
                "f_people_search_next": features.get("f_people_search_next", 0),
                "f_product_comparison": features.get("f_product_comparison", 0),
                "f_refined_searches": features.get("f_refined_searches", 0),
                "f_related_searches": features.get("f_related_searches", 0),
                "f_safe_search": features.get("f_safe_search", 0),
                "f_scholar": features.get("f_scholar", 0),
                "f_sitelinks": features.get("f_sitelinks", 0),
                "f_sns": features.get("f_sns", 0),
                "f_spell_check": features.get("f_spell_check", 0),
                "f_sticky_header_tabs": features.get("f_sticky_header_tabs", 0),
                "f_twitter": features.get("f_twitter", 0),
                "f_unit_converter": features.get("f_unit_converter", 0),
                "f_unknown": features.get("f_unknown", 0),
                "f_video_carousels": features.get("f_video_carousels", 0),
                "f_video_results": features.get("f_video_results", 0),
                "collected_date": COLLECTED
            }

    if (file_idx + 1) % 50 == 0:
        print(f"  처리 중: {file_idx + 1}/{len(l3_files)} 파일...")

# Monthly 추출 — monthly.py (없는 값은 null · (keyword, month) 중복 제거 · 표기 규칙은 모듈 docstring)
monthly_rows, mstats = monthly.extract_monthly(RAW, segment_of)
print(f"✓ 추출 완료: {len(master_data)}개 KEYWORD_MASTER, {len(features_data)}개 FEATURES, {len(monthly_rows)}개 MONTHLY")
print(f"  MONTHLY: 중복 월 {mstats['duplicate_months']:,}행 제거 · 값 불일치 {mstats['conflicts']} · "
      f"nv_code 월 {mstats['nv_code_months']:,} (null) · 지표 없는 파일 {mstats['no_payload_files']}")

# Step 3: CSV 저장
print("\n[3/4] CSV 파일 저장...")

# KEYWORDS_MASTER
master_cols = ["canonical_keyword", "segment", "keyword", "is_variant", "gl",
               "pipeline_source", "category_key", "category_lvl2",
               "volume_avg", "volume_total", "volume_trend",
               "gg_volume_avg", "gg_volume_total", "gg_volume_trend",
               "nv_volume_avg", "nv_volume_total", "nv_volume_trend", "nv_share",
               "intent_primary", "intent_i", "intent_n", "intent_c", "intent_t",
               "competition", "competition_index", "cpc", "low_bid_micros", "high_bid_micros",
               "gender_m_ratio", "gender_f_ratio",
               "age_0_ratio", "age_13_ratio", "age_20_ratio", "age_25_ratio", "age_30_ratio", "age_40_ratio", "age_50_ratio",
               "collected_date"]

with open(SILVER / "KEYWORDS_MASTER.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=master_cols)
    w.writeheader()
    for kw in sorted(master_data.keys()):
        row = master_data[kw]
        w.writerow({col: row.get(col, "") for col in master_cols})

print(f"✓ KEYWORDS_MASTER.csv: {len(master_data)}행")

# KEYWORDS_FEATURES
features_cols = ["canonical_keyword", "segment", "keyword", "gl",
                "f_ad", "f_ad_bottom", "f_ad_top", "f_ads", "f_ads_bottom", "f_ads_top",
                "f_adult_certification", "f_ai_overview", "f_app_results", "f_articles",
                "f_broaden_searches", "f_discover_more_places", "f_featured_snippet",
                "f_featured_snippet_ordered_list", "f_featured_snippet_table",
                "f_featured_snippet_unordered_list", "f_featured_snippet_video_exist",
                "f_google_play", "f_images", "f_job_search", "f_knowledge_panel",
                "f_knowledge_panel_appointment", "f_knowledge_panel_local", "f_local_results",
                "f_organic_results", "f_organic_results_rating", "f_organic_results_video",
                "f_others", "f_people_also_ask_for", "f_people_also_search_for",
                "f_people_search_next", "f_product_comparison", "f_refined_searches",
                "f_related_searches", "f_safe_search", "f_scholar", "f_sitelinks",
                "f_sns", "f_spell_check", "f_sticky_header_tabs", "f_twitter",
                "f_unit_converter", "f_unknown", "f_video_carousels", "f_video_results",
                "collected_date"]

with open(SILVER / "KEYWORDS_FEATURES.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=features_cols)
    w.writeheader()
    for kw in sorted(features_data.keys()):
        row = features_data[kw]
        w.writerow({col: row.get(col, "") for col in features_cols})

print(f"✓ KEYWORDS_FEATURES.csv: {len(features_data)}행")

# KEYWORDS_MONTHLY
monthly.write_monthly(SILVER / "KEYWORDS_MONTHLY.csv", monthly_rows.values())
monthly_list = list(monthly_rows.values())

print(f"✓ KEYWORDS_MONTHLY.csv: {len(monthly_list)}행")

print("\n" + "=" * 70)
print("✓ Snowflake 스키마 재생성 완료!")
print("=" * 70)
print(f"\n최종 데이터:")
print(f"  - KEYWORDS_MASTER: {len(master_data)}행 (기존 13개 → {len(master_data)}개)")
print(f"  - KEYWORDS_FEATURES: {len(features_data)}행")
print(f"  - KEYWORDS_MONTHLY: {len(monthly_list)}행")
