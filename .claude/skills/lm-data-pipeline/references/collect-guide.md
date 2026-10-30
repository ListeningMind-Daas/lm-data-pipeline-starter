# 수집 가이드 — 시드 · 센터 · 비용

수집(`lmdp run`)은 크레딧을 쓴다. 실행 전에 이 가이드로 입력을 정하고, 견적을 범위로 고지한 뒤 승인받는다.

## 시장 데이터(MI)

### 시드

- `--seeds <키워드 …>`: 품목 대표어·브랜드+품목 등 출발 시드. 레이어(L1~L5)는 룰북(`ref/brand_rules`)으로 **추정**해 기록한다
- `--seed-design <파일>`: 실측으로 재구성한 시드 설계(레이어·sub_group·볼륨 포함 JSON/CSV)를 그대로 쓴다 — 레이어가 **실측**으로 남는다
- 품목이 `ref/category_rules/category_rules.csv` 에 없으면 먼저 행을 추가한다(`lmdp check`가 알려 준다)

### 클러스터·경로 센터

단일 센터(시드 첫 번째) 암묵 규칙은 쓰지 않는다 — 미국 TV 실측에서 단일 센터의 클러스터 커버리지가 키워드 8.3%·검색량 21.9%에 그쳤다.

- **`--cluster-centers`**: 품목 총칭어 1개 + 핵심 하위 그룹별 검색량 1위 각 1개. 인접·액세서리·문제·수리 키워드는 센터로 쓰지 않는다
  (예: `smart tv` + `oled tv` · `65 inch tv`)
- **`--path-centers`**: 품목 총칭어 1개 + 브랜드 대표 1~2개 + (선택) 핵심 세그먼트 1개. 없으면 클러스터 첫 센터
- **`--cluster-limit`** (기본 2000): cluster_finder 과금은 `150 + 50 × 반환 고유 키워드` — `limit`은 관계 수 상한이라 센터당 상한 ≈ 150 + 50 × (limit+1)
- 보완 센터(두 번째부터)는 엔진이 먼저 hop 1 · limit 200 프로브(≈1만 크레딧)로 품목 관련성을 보고, 품목 토큰 비율 50% 이상일 때만 전량 수집한다
- 수집 키워드는 품목 토큰 + 노이즈 룰북을 통과한 것만 검색량 조회한다(`--category-tokens` 로 토큰 지정 가능)

### 비용 사례 [실측 — 품목·센터에 따라 다르다]

| run | 호출 | 크레딧 | 내역 |
|---|---:|---:|---|
| 한국 청소기 · 센터 2 · 경로 1 | 10 | 209,982 | cluster 148,500 · keyword_info 47,630 · intent 7,242 · path 6,610 |
| 미국 TV · 센터 다수 | 48 | 1,260,000 | cluster 805,850 · keyword_info 344,900 · intent 67,290 · path 41,960 — 센터 상한·프로브 규칙이 생기기 전 과지출 사례 |

- 월간 갱신은 intent_finder + keyword_info(검색량 갱신)만, 클러스터·경로는 분기 재수집을 권한다(구조는 느리게 변한다)
- 이번 run 의 실제 비용 = `tables/11_cost/api_call_log.csv` 의 `total_cost` 합

## 트렌드 유니버스

- 프레임(`ref/frame/<품목>_kr_frame.json`)의 노드마다 intent_finder 1회(L1) → keyword_info ads_metrics(L2 게이트 · 키워드당 1) →
  승격분 keyword_info all(L3 · 키워드당 10)
- 새 품목은 `lmdp rulebook <프로젝트>` → 룰북 4종의 꺾쇠 자리를 채운다(`ref/_templates/README.md`)
- 비용 사례 [실측]: 한국 패션 · 프레임 281노드 → 약 1.5만 후보 → 승격 1.48만 키워드 · 260,810 크레딧(대부분 L3 승격 수집)
- 이번 run 의 실제 비용 = `cost/API_CALL_LOG.csv` 의 `TOTAL_COST` 합

## 크레딧 용어

- 호출 1건 비용 = 응답 `cost_detail.total_cost`
- `used_credits` = 계정의 **월간 누적 사용량**(달마다 초기화 · 잔여량 아님)
