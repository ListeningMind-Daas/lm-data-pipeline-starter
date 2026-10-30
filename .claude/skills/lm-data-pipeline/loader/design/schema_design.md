# 스키마 설계안

설계안 v1.0 · 이 문서는 `schema_design.json` 과 함께 생성된다(손으로 고치지 않는다).

- 적재는 **층 순서대로** 한다 — 스키마(데이터셋)를 만들고 그 층의 테이블을 만든 뒤 싣는다. 한 층이 실패하면 뒤 층은 싣지 않는다
- 모든 테이블 끝에 계보 컬럼 `_run_id`(run 폴더 이름) · `_loaded_at`(적재 시각 UTC)이 붙는다. 같은 run 을 다시 실으면 `_run_id` 행을 지우고 싣는다
- 공통 타입 → 대상별 타입: `loader/sinks/*.py` 의 TYPE_MAP

## 시장 데이터(MI) 메달리온 (`mi`)

run 폴더: `90_Output/<프로젝트>/{gl}_{category}_{run_date}/`

| 순서 | 층 | 이름 | 내용 | 테이블 |
|---|---|---|---|---|
| 1 | `ref` | 룰북 스냅샷 | 이 run 에 적용한 브랜드·품목·노이즈·시드 레이어·시즌 규칙 | 5 |
| 2 | `seed_design` | 시드 설계 | L1~L5 시드와 실행 메타 — 무엇을 수집했는지 | 4 |
| 3 | `bronze_raw` | 원본 응답 | API 응답 1개 = 1행 · 무변형 payload | 4 |
| 4 | `silver_master` | 정제 | 키워드 마스터·월별 검색량·SERP 피처·클러스터 | 5 |
| 5 | `gold_analytics` | 분석 | 시장 규모·점유율·추이·페르소나·여정·기회·대시보드 마트 | 7 |
| 6 | `gold_reports` | 리포트 | 리포트 메타·섹션 | 2 |
| 7 | `cost` | 비용 | 호출 1건 = 1행 크레딧 원장 | 1 |
| 8 | `usage` | 사용량 | 일별·엔드포인트별·시계열 사용량 요약 | 4 |
| 9 | `ops` | 운영 | 실행 기록·품질 점검·격리·신선도 | 4 |

- **ref.brand_rules** ← `tables/98_ref/brand_rules.csv`  
  `gl` string, `category_key` string, `pattern` string, `brand` string, `priority` int, `version` string, `note` string, `loaded_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ref.category_rules** ← `tables/98_ref/category_rules.csv`  
  `gl` string, `category_key` string, `pattern` string, `category_lvl1` string, `category_lvl2` string, `priority` int, `version` string, `note` string, `loaded_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ref.market_calendar** ← `tables/98_ref/market_calendar.csv`  
  `gl` string, `category_key` string, `month` int, `event_name` string, `event_type` string, `seasonality` string, `impact_level` string, `label` string, `version` string, `note` string, `loaded_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ref.noise_patterns** ← `tables/98_ref/noise_patterns.csv`  
  `gl` string, `category_key` string, `pattern` string, `match_type` string, `scope` string, `reason` string, `action` string, `version` string, `note` string, `loaded_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ref.seed_layer_config** ← `tables/98_ref/seed_layer_config.csv`  
  `gl` string, `category_key` string, `layer` string, `layer_name` string, `description` string, `expansion_method` string, `target_share_pct` int, `priority` int, `version` string, `note` string, `loaded_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **seed_design.seed_blind_spots** ← `tables/00_seed_design/seed_blind_spots.csv` · 파티션 gl  
  `run_id` string, `gl` string, `market` string, `record_type` string, `subject` string, `description` string, `position` string, `recommendation` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **seed_design.seed_communities** ← `tables/00_seed_design/seed_communities.csv` · 파티션 gl  
  `run_id` string, `gl` string, `market` string, `source_keyword` string, `community_id` string, `keyword` string, `weight` double, `hop` int, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **seed_design.seed_keywords** ← `tables/00_seed_design/seed_keywords.csv` · 파티션 gl, layer  
  `run_id` string, `gl` string, `market` string, `layer` string, `sub_group` string, `keyword` string, `volume_avg` bigint, `volume_total` bigint, `volume_trend` double, `competition` string, `cpc` double, `label` string, `note` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **seed_design.seed_runs** ← `tables/00_seed_design/seed_runs.csv` · 파티션 gl  
  `run_id` string, `market` string, `gl` string, `created_at` date, `skill_version` string, `total_seeds` int, `coverage_pct` double, `coverage_label` string, `v_denominator_keyword` string, `v_denominator_volume` bigint, `used_credits` double, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **bronze_raw.cluster_finder_raw** ← `tables/01_bronze_raw/cluster_finder_raw.csv` · 파티션 gl, run_date  
  `source_file` string, `source_file_hash` string, `gl` string, `run_date` date, `category_key` string, `api_version` string, `request_at` timestamp, `payload` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **bronze_raw.intent_finder_raw** ← `tables/01_bronze_raw/intent_finder_raw.csv` · 파티션 gl, run_date  
  `source_file` string, `source_file_hash` string, `gl` string, `run_date` date, `category_key` string, `api_version` string, `request_at` timestamp, `payload` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **bronze_raw.keyword_info_raw** ← `tables/01_bronze_raw/keyword_info_raw.csv` · 파티션 gl, run_date  
  `source_file` string, `source_file_hash` string, `gl` string, `run_date` date, `category_key` string, `api_version` string, `request_at` timestamp, `payload` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **bronze_raw.path_finder_raw** ← `tables/01_bronze_raw/path_finder_raw.csv` · 파티션 gl, run_date  
  `source_file` string, `source_file_hash` string, `gl` string, `run_date` date, `category_key` string, `api_version` string, `request_at` timestamp, `payload` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **silver_master.cluster_master** ← `tables/02_silver_master/cluster_master.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `community_id` string, `cluster_label` string, `top_keywords` json, `keyword_count` int, `is_empty` boolean, `data_quality_flag` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **silver_master.keyword_features** ← `tables/02_silver_master/keyword_features.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `keyword` string, `keyword_id` string, `f_ad_top` bigint, `f_ad_bottom` bigint, `f_ads_top` bigint, `f_ads_bottom` bigint, `f_featured_snippet` bigint, `f_featured_snippet_ordered_list` bigint, `f_featured_snippet_unordered_list` bigint, `f_featured_snippet_table` bigint, `f_featured_snippet_video_exist` bigint, `f_ai_overview` bigint, `f_images` bigint, `f_video_results` bigint, `f_video_carousels` bigint, `f_organic_results_video` bigint, `f_people_also_ask_for` bigint, `f_people_also_search_for` bigint, `f_related_searches` bigint, `f_sitelinks` bigint, `f_product_comparison` bigint, `f_organic_results_rating` bigint, `f_local_results` bigint, `f_knowledge_panel` bigint, `f_sns` bigint, `f_articles` bigint, `f_google_play` bigint, `f_app_results` bigint, `f_job_search` bigint, `f_scholar` bigint, `f_unit_converter` bigint, `f_spell_check` bigint, `f_safe_search` bigint, `f_adult_certification` bigint, `f_sticky_header_tabs` bigint, `f_organic_results` bigint, `f_others` bigint, `f_unknown` bigint, `collected_date` date, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **silver_master.keyword_monthly** ← `tables/02_silver_master/keyword_monthly.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `keyword` string, `keyword_id` string, `month` string, `gg_volume` bigint, `nv_volume` bigint, `total_volume` bigint, `label_volume` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **silver_master.keywords_master** ← `tables/02_silver_master/keywords_master.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `keyword` string, `canonical_keyword` string, `is_variant` boolean, `keyword_id` string, `pipeline_source` string, `volume_avg` bigint, `volume_total` bigint, `volume_trend` double, `gg_volume_avg` bigint, `gg_volume_total` bigint, `gg_volume_trend` double, `nv_volume_avg` bigint, `nv_volume_total` bigint, `nv_volume_trend` double, `intent_primary` string, `intent_i` double, `intent_n` double, `intent_c` double, `intent_t` double, `competition` string, `competition_index` bigint, `cpc` double, `low_bid_micros` bigint, `high_bid_micros` bigint, `gender_m_ratio` double, `gender_f_ratio` double, `age_0_ratio` double, `age_13_ratio` double, `age_20_ratio` double, `age_25_ratio` double, `age_30_ratio` double, `age_40_ratio` double, `age_50_ratio` double, `brand` string, `category_lvl1` string, `category_lvl2` string, `seed_layer_origin` string, `is_noise` boolean, `noise_reason` string, `growth_3m` double, `acceleration` double, `volatility` double, `seasonality_peak_index` double, `seasonality_peak_month` int, `label_volume` string, `label_intent` string, `label_demography` string, `label_derived` string, `data_quality_flag` string, `noise_filter_version` string, `brand_rules_version` string, `api_version` string, `collected_date` date, `source_run_id` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **silver_master.membership** ← `tables/02_silver_master/membership.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `keyword_id` string, `keyword` string, `community_id` string, `weight` double, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.dashboard_mart** ← `tables/03_gold_analytics/dashboard_mart.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `category_name` string, `tam_total_volume` bigint, `tam_index` double, `keyword_count` bigint, `growth_3m` double, `top_brand` string, `top_brand_sov_pct` double, `focus_brand` string, `focus_brand_sov_pct` double, `rival_brand` string, `rival_brand_sov_pct` double, `top_opportunity_keyword` string, `top_opportunity_score` double, `persona_primary` string, `primary_intent` string, `label_volume` string, `label_derived` string, `label_demography` string, `label_intent` string, `data_quality_flag` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.journey_analysis** ← `tables/03_gold_analytics/journey_analysis.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `journey_id` string, `step_order` int, `keyword` string, `intent_stage` string, `volume_avg` bigint, `label_intent` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.opportunity_analysis** ← `tables/03_gold_analytics/opportunity_analysis.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `keyword` string, `opportunity_score` double, `volume_avg` bigint, `competition` string, `competition_index` bigint, `intent_primary` string, `growth_3m` double, `sov_gap` double, `label_volume` string, `label_derived` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.persona_analysis** ← `tables/03_gold_analytics/persona_analysis.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `persona_segment` string, `gender` string, `age_group` string, `intent_primary` string, `keyword_count` bigint, `volume_share_pct` double, `top_keywords` json, `label_demography` string, `label_intent` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.sov_analysis** ← `tables/03_gold_analytics/sov_analysis.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `segment` string, `brand` string, `sov_keyword_cnt` bigint, `sov_volume` bigint, `sov_pct` double, `rank` int, `label_derived` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.tam_analysis** ← `tables/03_gold_analytics/tam_analysis.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `segment` string, `keyword_count` bigint, `total_volume` bigint, `volume_share_pct` double, `tam_index` double, `growth_3m` double, `label_derived` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_analytics.trend_analysis** ← `tables/03_gold_analytics/trend_analysis.csv` · 파티션 gl, run_date  
  `gl` string, `run_date` date, `category_key` string, `keyword` string, `month` string, `total_volume` bigint, `gg_volume` bigint, `nv_volume` bigint, `growth_3m` double, `acceleration` double, `label_volume` string, `label_derived` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_reports.report_meta** ← `tables/04_gold_reports/report_meta.csv` · 파티션 gl, run_date  
  `report_id` string, `gl` string, `run_date` date, `category_key` string, `report_type` string, `title` string, `s3_path` string, `file_format` string, `file_size_kb` bigint, `created_at` timestamp, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **gold_reports.report_sections** ← `tables/04_gold_reports/report_sections.csv` · 파티션 gl, run_date  
  `report_id` string, `gl` string, `run_date` date, `category_key` string, `section_order` int, `section_title` string, `content_md` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **cost.api_call_log** ← `tables/11_cost/api_call_log.csv` · 파티션 run_date  
  `log_id` string, `request_at` timestamp, `endpoint` string, `gl` string, `category_key` string, `run_date` date, `input_cost` bigint, `input_count` bigint, `input_total_cost` bigint, `output_cost` bigint, `output_count` bigint, `output_total_cost` bigint, `total_cost` bigint, `used_credits` bigint, `api_version` string, `user_type` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **usage.by_endpoint** ← `tables/12_usage/by_endpoint.csv` · 파티션 snapshot_date  
  `snapshot_date` date, `from_date` date, `to_date` date, `endpoint` string, `calls` bigint, `ratio` double, `err_4xx` bigint, `rate_4xx` double, `err_5xx` bigint, `rate_5xx` double, `error_rate` double, `credits` bigint, `credits_ratio` double, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **usage.by_platform** ← `tables/12_usage/by_platform.csv` · 파티션 snapshot_date  
  `snapshot_date` date, `from_date` date, `to_date` date, `platform` string, `calls` bigint, `ratio` double, `err_4xx` bigint, `rate_4xx` double, `err_5xx` bigint, `rate_5xx` double, `error_rate` double, `credits` bigint, `credits_ratio` double, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **usage.summary_daily** ← `tables/12_usage/summary_daily.csv` · 파티션 snapshot_date  
  `snapshot_date` date, `from_date` date, `to_date` date, `user_type` string, `total_calls` bigint, `error_count` bigint, `error_rate` double, `total_credits` bigint, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **usage.timeseries** ← `tables/12_usage/timeseries.csv` · 파티션 snapshot_date  
  `snapshot_date` date, `from_date` date, `to_date` date, `ref_date` string, `calls` bigint, `errors` bigint, `credits` bigint, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ops.data_freshness** ← `tables/99_ops/data_freshness.csv`  
  `schema_name` string, `table_name` string, `gl` string, `latest_run_date` date, `row_count` bigint, `checked_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ops.pipeline_runs** ← `tables/99_ops/pipeline_runs.csv` · 파티션 run_date  
  `run_id` string, `gl` string, `run_date` date, `category_key` string, `phase` string, `status` string, `started_at` timestamp, `finished_at` timestamp, `duration_sec` int, `error_message` string, `triggered_by` string, `ingested_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ops.qc_results** ← `tables/99_ops/qc_results.csv` · 파티션 run_date  
  `qc_id` string, `run_id` string, `gl` string, `run_date` date, `category_key` string, `check_name` string, `passed` boolean, `expected` string, `actual` string, `severity` string, `checked_at` timestamp, `_run_id` string, `_loaded_at` timestamp
- **ops.silver_quarantine** ← `tables/99_ops/silver_quarantine.csv`  
  `gl` string, `run_date` date, `category_key` string, `keyword_id` string, `keyword` string, `community_id` string, `month` string, `row_json` string, `quarantine_reason` string, `quarantined_at` timestamp, `source_run_id` string, `_run_id` string, `_loaded_at` timestamp

## 트렌드 유니버스 (`trend`)

run 폴더: `90_Output/<프로젝트>/{gl}_{category}_{run_date}/`

| 순서 | 층 | 이름 | 내용 | 테이블 |
|---|---|---|---|---|
| 1 | `DESIGN` | 프레임 설계 | L0 프레임 노드 → L1 후보 → L2 속도 판정 | 3 |
| 2 | `BRONZE` | 원본 응답 | API 응답 파일 매니페스트(파일 1개 = 1행) | 1 |
| 3 | `SILVER` | 정제 | 키워드 마스터·월별·피처·태깅·확장·브랜드 | 6 |
| 4 | `GOLD` | 분석 | 트렌드 스코어·랭킹·브랜드 점유율·태그 모멘텀·발견 후보 | 15 |
| 5 | `COST` | 비용 | 호출 로그·단계별 요약 | 2 |
| 6 | `META` | 메타 | run 매니페스트·시드(프레임) 설계 기록 | 2 |

- **DESIGN.FRAME_NODES** ← `frame_design/frame_nodes.json`  
  `keyword` string, `axis` string, `group` string, `_run_id` string, `_loaded_at` timestamp
- **DESIGN.L1_CANDIDATES** ← `frame_design/l1_candidates.csv`  
  `keyword` string, `volume_hint` bigint, `n_sources` int, `sources` string, `_run_id` string, `_loaded_at` timestamp
- **DESIGN.L2_GATE_RESULTS** ← `frame_design/l2_gate_results.csv`  
  `keyword` string, `volume_avg` bigint, `volume_total` bigint, `volume_trend` double, `gate` string, `_run_id` string, `_loaded_at` timestamp
- **BRONZE.RAW_API_RESPONSES** ← `bronze/RAW_API_RESPONSES.csv`  
  `SOURCE_FILE` string, `API` string, `RUN_DATE` string, `LOADED_AT` timestamp, `PAYLOAD` json, `_run_id` string, `_loaded_at` timestamp
- **SILVER.BRAND_MASTER** ← `silver/BRAND_MASTER.csv`  
  `BRAND` string, `SEGMENT` string, `VOL_AVG` bigint, `VOL_TOTAL` bigint, `TREND` double, `_run_id` string, `_loaded_at` timestamp
- **SILVER.KEYWORDS_EXPANDED** ← `silver/KEYWORDS_EXPANDED.csv`  
  `CANONICAL_KEYWORD` string, `TYPE` string, `DIMENSION` string, `VOLUME_TOTAL` bigint, `VOLUME_AVG` bigint, `VOLUME_TREND_WAVG` double, `NV_SHARE` double, `VARIANTS` string, `_run_id` string, `_loaded_at` timestamp
- **SILVER.KEYWORDS_FEATURES** ← `silver/KEYWORDS_FEATURES.csv`  
  `CANONICAL_KEYWORD` string, `SEGMENT` string, `KEYWORD` string, `GL` string, `F_AD` bigint, `F_AD_BOTTOM` bigint, `F_AD_TOP` bigint, `F_ADS` bigint, `F_ADS_BOTTOM` bigint, `F_ADS_TOP` bigint, `F_ADULT_CERTIFICATION` bigint, `F_AI_OVERVIEW` bigint, `F_APP_RESULTS` bigint, `F_ARTICLES` bigint, `F_BROADEN_SEARCHES` bigint, `F_DISCOVER_MORE_PLACES` bigint, `F_FEATURED_SNIPPET` bigint, `F_FEATURED_SNIPPET_ORDERED_LIST` bigint, `F_FEATURED_SNIPPET_TABLE` bigint, `F_FEATURED_SNIPPET_UNORDERED_LIST` bigint, `F_FEATURED_SNIPPET_VIDEO_EXIST` bigint, `F_GOOGLE_PLAY` bigint, `F_IMAGES` bigint, `F_JOB_SEARCH` bigint, `F_KNOWLEDGE_PANEL` bigint, `F_KNOWLEDGE_PANEL_APPOINTMENT` bigint, `F_KNOWLEDGE_PANEL_LOCAL` bigint, `F_LOCAL_RESULTS` bigint, `F_ORGANIC_RESULTS` bigint, `F_ORGANIC_RESULTS_RATING` bigint, `F_ORGANIC_RESULTS_VIDEO` bigint, `F_OTHERS` bigint, `F_PEOPLE_ALSO_ASK_FOR` bigint, `F_PEOPLE_ALSO_SEARCH_FOR` bigint, `F_PEOPLE_SEARCH_NEXT` bigint, `F_PRODUCT_COMPARISON` bigint, `F_REFINED_SEARCHES` bigint, `F_RELATED_SEARCHES` bigint, `F_SAFE_SEARCH` bigint, `F_SCHOLAR` bigint, `F_SITELINKS` bigint, `F_SNS` bigint, `F_SPELL_CHECK` bigint, `F_STICKY_HEADER_TABS` bigint, `F_TWITTER` bigint, `F_UNIT_CONVERTER` bigint, `F_UNKNOWN` bigint, `F_VIDEO_CAROUSELS` bigint, `F_VIDEO_RESULTS` bigint, `COLLECTED_DATE` string, `_run_id` string, `_loaded_at` timestamp
- **SILVER.KEYWORDS_MASTER** ← `silver/KEYWORDS_MASTER.csv`  
  `CANONICAL_KEYWORD` string, `SEGMENT` string, `KEYWORD` string, `IS_VARIANT` bigint, `GL` string, `VOLUME_AVG` bigint, `VOLUME_TOTAL` bigint, `VOLUME_TREND` double, `GG_VOLUME_AVG` bigint, `GG_VOLUME_TOTAL` bigint, `GG_VOLUME_TREND` double, `NV_VOLUME_AVG` bigint, `NV_VOLUME_TOTAL` bigint, `NV_VOLUME_TREND` double, `NV_SHARE` double, `INTENT_PRIMARY` string, `INTENT_I` bigint, `INTENT_N` bigint, `INTENT_C` bigint, `INTENT_T` bigint, `COMPETITION` string, `COMPETITION_INDEX` bigint, `CPC` double, `LOW_BID_MICROS` bigint, `HIGH_BID_MICROS` bigint, `GENDER_M_RATIO` double, `GENDER_F_RATIO` double, `AGE_0_RATIO` double, `AGE_13_RATIO` double, `AGE_20_RATIO` double, `AGE_25_RATIO` double, `AGE_30_RATIO` double, `AGE_40_RATIO` double, `AGE_50_RATIO` double, `COLLECTED_DATE` string, `_run_id` string, `_loaded_at` timestamp
- **SILVER.KEYWORDS_MONTHLY** ← `silver/KEYWORDS_MONTHLY.csv`  
  `CANONICAL_KEYWORD` string, `SEGMENT` string, `KEYWORD` string, `GL` string, `MONTH` string, `GG_VOLUME` bigint, `NV_VOLUME` double, `TOTAL_VOLUME` double, `_run_id` string, `_loaded_at` timestamp
- **SILVER.KEYWORDS_TAGGED** ← `silver/KEYWORDS_TAGGED.csv`  
  `SOURCE` string, `CANONICAL` string, `KEYWORD` string, `DIM_BODY` string, `DIM_OCCASION` string, `DIM_MODIFIER` string, `DIM_GENDER` string, `DIM_SEASON` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.BRAND_MONITOR** ← `gold/BRAND_MONITOR.csv`  
  `BRAND` string, `SEGMENT` string, `RANK` bigint, `SOV` double, `CUM_SOV` double, `TREND` double, `RISING` boolean, `COVERED_80` boolean, `RANK_DELTA` double, `SOV_DELTA` double, `CHANGE_CLASS` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.BRAND_SOV** ← `gold/BRAND_SOV.csv`  
  `RANK` bigint, `BRAND` string, `SEGMENT` string, `VOL_AVG` bigint, `SOV` double, `CUM_SOV` double, `COVERED_80` boolean, `RISING` boolean, `TREND` double, `_run_id` string, `_loaded_at` timestamp
- **GOLD.BRAND_TREND_LINK** ← `gold/BRAND_TREND_LINK.csv`  
  `BRAND` string, `TREND` string, `KEYWORD` string, `VOLUME_AVG` bigint, `VOLUME_TREND` double, `_run_id` string, `_loaded_at` timestamp
- **GOLD.COVERAGE_GAP_AUDIT** ← `gold/COVERAGE_GAP_AUDIT.csv`  
  `KEYWORD` string, `STATUS` string, `VOL_AVG` bigint, `VOL_TOTAL` bigint, `TREND` double, `TIER` string, `SEASONAL_WINTER` bigint, `TREND_LINK` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.DIMENSION_CROSSTAB** ← `gold/DIMENSION_CROSSTAB.csv`  
  `CANONICAL` string, `N_KEYWORDS` bigint, `MALE` bigint, `FEMALE` bigint, `SUMMER` bigint, `WINTER` bigint, `_run_id` string, `_loaded_at` timestamp
- **GOLD.DISCOVERY_CANDIDATES_V2** ← `gold/DISCOVERY_CANDIDATES_V2.csv`  
  `SEED` string, `KEYWORD` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.DISCOVERY_GATED** ← `gold/DISCOVERY_GATED.csv`  
  `KEYWORD` string, `SEED` string, `VOLUME_AVG` bigint, `VOLUME_TREND` double, `INCLUSION_STATUS` string, `BREAKOUT` bigint, `PROMOTE` bigint, `_run_id` string, `_loaded_at` timestamp
- **GOLD.EXPANSION_SCORECARD** ← `gold/EXPANSION_SCORECARD.csv`  
  `CANONICAL_KEYWORD` string, `TYPE` string, `DIMENSION` string, `VOLUME_TOTAL` bigint, `VOLUME_AVG` bigint, `VOLUME_TREND_WAVG` double, `NV_SHARE` double, `VARIANTS` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.GOLD_TAG_MOMENTUM** ← `gold/GOLD_TAG_MOMENTUM.csv`  
  `CANONICAL` string, `AXIS` string, `DIM_VALUE` string, `N_KEYWORDS` bigint, `VOL_AVG_SUM` bigint, `TREND_WAVG` double, `MOMENTUM` double, `_run_id` string, `_loaded_at` timestamp
- **GOLD.KEYWORD_SCORECARD** ← `gold/KEYWORD_SCORECARD.csv`  
  `CANONICAL_KEYWORD` string, `SEGMENT` string, `VARIANT_COUNT` bigint, `VARIANTS` string, `VOLUME_TOTAL` bigint, `VOLUME_AVG` bigint, `VOLUME_TREND_WAVG` double, `NV_SHARE` double, `COMPETITION_INDEX_WAVG` double, `CPC_WAVG` double, `DOMINANT_INTENT` string, `TOP_AGE_BUCKET` string, `GENDER_M_RATIO_WAVG` double, `GENDER_F_RATIO_WAVG` double, `_run_id` string, `_loaded_at` timestamp
- **GOLD.MONTHLY_TREND** ← `gold/MONTHLY_TREND.csv`  
  `CANONICAL_KEYWORD` string, `SEGMENT` string, `MONTH` string, `GG_VOLUME` bigint, `NV_VOLUME` double, `TOTAL_VOLUME` double, `_run_id` string, `_loaded_at` timestamp
- **GOLD.NEW_TREND_CANDIDATES** ← `gold/NEW_TREND_CANDIDATES.csv`  
  `CONCEPT` string, `IN_PANEL` boolean, `VOL_AVG` double, `TREND` double, `VERDICT` string, `RECOVERS_GAP` boolean, `_run_id` string, `_loaded_at` timestamp
- **GOLD.TREND_MONITOR** ← `gold/TREND_MONITOR.csv`  
  `CANONICAL_KEYWORD` string, `SEGMENT` string, `VOLUME_AVG` bigint, `VOLUME_TOTAL` bigint, `VOLUME_TREND` double, `MOMENTUM_SCORE` double, `INCLUSION_STATUS` string, `BREAKOUT_FLAG` bigint, `CHANGE_CLASS` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.TREND_RANKING** ← `gold/TREND_RANKING.csv`  
  `RANK` bigint, `CANONICAL_KEYWORD` string, `SEGMENT` string, `VOLUME_TOTAL` bigint, `VOLUME_TREND` double, `DOMINANT_INTENT` string, `MOMENTUM_SCORE` double, `DIRECTION` string, `_run_id` string, `_loaded_at` timestamp
- **GOLD.UNKNOWN_UNKNOWNS** ← `gold/UNKNOWN_UNKNOWNS.csv`  
  `CONCEPT` string, `DIMENSION` string, `_run_id` string, `_loaded_at` timestamp
- **COST.API_CALL_LOG** ← `cost/API_CALL_LOG.csv`  
  `CALL_SEQ` bigint, `RUN_ID` string, `GL` string, `PHASE` string, `API` string, `PURPOSE` string, `PARAMS` string, `INPUT_COST` bigint, `INPUT_COUNT` bigint, `OUTPUT_COST` bigint, `OUTPUT_COUNT` bigint, `TOTAL_COST` bigint, `RETURNED` bigint, `REQUEST_AT` string, `_run_id` string, `_loaded_at` timestamp
- **COST.COST_SUMMARY** ← `cost/COST_SUMMARY.csv`  
  `DIMENSION` string, `KEY` string, `CALLS` bigint, `CREDITS` bigint, `PCT` double, `_run_id` string, `_loaded_at` timestamp
- **META.RUN_MANIFEST** ← `meta/RUN_MANIFEST.csv`  
  `RUN_ID` string, `GL` string, `RUN_DATE` string, `REF_VERSION` string, `TOTAL_API_CREDITS` bigint, `LOADED_AT` timestamp, `PAYLOAD` json, `_run_id` string, `_loaded_at` timestamp
- **META.SEED_DESIGN** ← `meta/SEED_DESIGN.csv`  
  `RUN_ID` string, `SOURCE_FILE` string, `LOADED_AT` timestamp, `PAYLOAD` json, `_run_id` string, `_loaded_at` timestamp

