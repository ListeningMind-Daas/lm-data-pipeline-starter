---
name: lm-data-pipeline
description: |
  ListeningMind DaaS 검색 데이터 적재 파이프라인 — 프로젝트마다 지정한 저장소(로컬 파일 · Databricks · Snowflake · BigQuery)에
  스키마 설계안 순서대로 싣는다. 두 파이프라인: 시장 데이터(MI 메달리온 — 품목 시드 → 수집 → 정제 → 분석 36테이블) ·
  트렌드 유니버스(프레임 → L1~L3 수집 → 정제 → 분석 29테이블).
  "적재 프로젝트 만들어줘" · "○○ 품목 데이터 적재" · "Snowflake/Databricks/BigQuery 에 올려줘" · "스키마 DDL 뽑아줘" ·
  "적재 계획 보여줘" · "적재 검증" · "트렌드 새 품목 룰북" 요청에 쓴다.
metadata:
  build: v1.0.2
---

# lm-data-pipeline — 검색 데이터 적재 파이프라인

> 수집·정제는 이 워크스페이스 안에서 하고(엔진), 대상 저장소에는 **설계안 순서대로 싣기만** 한다(적재기).
> 모든 명령은 `python3 .claude/skills/lm-data-pipeline/loader/lmdp.py …` (아래에서는 `lmdp`로 줄인다).

## 구성

| 위치 | 역할 |
|---|---|
| `loader/lmdp.py` | 프로젝트 도구 — init · rulebook · check · ddl · run · plan · load · verify · selftest |
| `loader/design/schema_design.json` · `.md` | **스키마 설계안** — 층(적재 순서) · 테이블 · 컬럼 · 공통 타입 · 파티션 · 원본 파일 위치 |
| `loader/sinks/` | 적재 대상 4종(local · databricks · snowflake · bigquery) — 같은 순서로 불린다 |
| `pipelines/mi/` | 시장 데이터 엔진 — `scripts/run_pipeline.py`(시드 → 수집 → 로컬 적재) · `ref/` 룰북 |
| `pipelines/trend/` | 트렌드 엔진 — `pipelines/trend_fashion/run_all.py`(01~08) · `ref/` 룰북 · `ref/_templates/` 새 품목 템플릿 |

## 실행 순서 (항상 이 순서 — 크레딧을 쓰는 단계 전에는 사용자 승인)

1. **프로젝트 만들기** — 파이프라인(mi·trend) · 시장(kr·us·jp) · 품목 키 · 적재 대상을 AskUserQuestion 으로 확인한 뒤
   ```
   lmdp init <프로젝트> --pipeline mi --gl kr --category vacuum-cleaner --target databricks --catalog acme_mi [--schema-prefix vac_]
   lmdp init <프로젝트> --pipeline trend --category trend_shoes --target snowflake --database ACME_TREND
   lmdp init <프로젝트> --pipeline mi --category tv --gl us --target bigquery --project my-gcp-project [--location US]
   lmdp init <프로젝트> --pipeline mi --category tv --target local            # 기본: 90_Output/<프로젝트>/warehouse · csv
   ```
   관심·경쟁 브랜드(`--focus-brand` · `--rival-brand`)를 주면 대시보드 마트 헤드라인에 따로 뽑는다(브랜드 룰북 이름).
   설정은 `10_Projects/<프로젝트>/project.toml` — **비밀값을 쓰지 않는다**(아래 '저장소 인증').
2. **룰북 점검** — `lmdp check <프로젝트>`
   - MI: 품목이 `pipelines/mi/ref/category_rules/category_rules.csv` 에 없으면 행을 추가한다(없으면 품질 점검이 멈춘다)
   - 트렌드 새 품목: `lmdp rulebook <프로젝트>` → 룰북 4종(frame · dimensions · brand_rules · category_rules) 템플릿 생성 →
     꺾쇠(`<…>`) 자리를 사용자와 함께 채운다. 채우기 전에는 `run` 이 크레딧을 쓰기 전에 멈춘다(종료 코드 4)
3. **스키마 확인** — `lmdp ddl <프로젝트> --out 90_Output/<프로젝트>/ddl.sql` 로 대상 저장소용 DDL 을 보여 준다(연결 없음).
   조직 정책상 DDL 을 DBA 가 먼저 실행해야 하면 이 파일을 넘긴다 — 적재기는 `IF NOT EXISTS` 라 이미 있는 스키마·테이블을 그대로 쓴다
4. **수집(크레딧)** — 견적을 고지하고 승인받은 뒤 `lmdp run <프로젝트> --run-date YYYY-MM[-DD] [--phases …] -- <엔진 인자>`
   - MI 엔진 인자: `--seeds …` 또는 `--seed-design <파일>` · `--cluster-centers …` · `--path-centers …` · `--cluster-limit N`
     (시드·센터·limit 기준과 비용 사례: `references/collect-guide.md`)
   - 트렌드: `--phases collect`(기본 · 01~04 API 수집 → 05~08 빌드) · `build`(저장된 원본으로 05~08 · 0크레딧) · `05-08` · `06`
   - run 폴더: `90_Output/<프로젝트>/<gl>_<품목>_<run-date>/` — 원본 응답·로컬 테이블·비용 원장이 여기 남는다
5. **적재 계획** — `lmdp plan <프로젝트> --run <run 폴더>`: 층 순서 · 테이블 · 원본 행 수 · 원본 없는 테이블
6. **적재** — `lmdp load <프로젝트> --run <run 폴더>` (먼저 `--dry-run`)
   - 층 순서대로: 스키마 생성 → 테이블 생성 → 같은 `_run_id` 행 삭제 → 적재. **한 층이 실패하면 멈추고 뒤 층은 싣지 않는다**
   - 기록: `90_Output/<프로젝트>/<run>/_load/<대상>_<시각>.jsonl` — 층·테이블·행 수·변환 실패 칸
   - 변환 실패 칸(⚠)이 나오면 숨기지 말고 사용자에게 알린다(원본 값이 타입에 맞지 않았다는 뜻)
7. **검증** — `lmdp verify <프로젝트> --run <run 폴더>`: 대상 행 수 = 원본 행 수. 불일치가 있으면 완료라고 말하지 않는다
8. **보고** — 이번 수집 크레딧(= run 의 비용 원장 `total_cost` 합 · MI `tables/11_cost/api_call_log.csv` · 트렌드 `cost/API_CALL_LOG.csv`),
   적재 행 수, 대상 위치(catalog/database/dataset), 검증 결과, 미확인 사항

## 저장소 인증 (환경변수로만 — 설정·로그·커밋에 쓰지 않는다)

| 대상 | project.toml | 환경변수 | 드라이버 |
|---|---|---|---|
| local | path · format(csv·parquet) | — | (parquet 이면 pyarrow) |
| databricks | catalog(미리 생성) · schema_prefix · staging_volume(선택) | DATABRICKS_HOST · DATABRICKS_HTTP_PATH · DATABRICKS_TOKEN | databricks-sql-connector |
| snowflake | database · warehouse · role · schema_prefix | SNOWFLAKE_ACCOUNT · SNOWFLAKE_USER · SNOWFLAKE_PRIVATE_KEY_PATH(권장) 또는 SNOWFLAKE_PASSWORD | snowflake-connector-python[pandas] |
| bigquery | project · location · schema_prefix | GOOGLE_APPLICATION_CREDENTIALS 또는 `gcloud auth application-default login` | google-cloud-bigquery · pyarrow · db-dtypes |

접속만 먼저 확인: `lmdp check <프로젝트> --connect` (적재하지 않는다).
API 엔드포인트는 운영 주소가 기본값이다 — 바꿔야 하면 시장 데이터 엔진은 `LM_BASE_URL`, 트렌드 엔진은 `LM_API_BASE` 환경변수(두 엔진의 변수 이름이 다르다).
Databricks 큰 테이블(월별 검색량 수십만 행)은 `staging_volume`(Unity Catalog Volume)을 주면 파일 업로드 → COPY INTO 로 싣는다 — 없으면 INSERT 묶음이라 느리다.

## 이름 규칙

- 층 하나 = 스키마 하나(BigQuery 는 데이터셋 하나): `<schema_prefix><층>` — 예 `acme_mi.vac_silver_master.keywords_master`
- Snowflake 는 대문자(`ACME_TREND.SILVER.KEYWORDS_MASTER`), 나머지는 소문자
- 같은 대상에 여러 품목을 싣는다: 품목마다 `schema_prefix` 를 다르게 주거나, 같은 스키마에 싣고 `_run_id` 로 구분한다

## 규율

- 크레딧: 호출 1건 비용 = 응답 `cost_detail.total_cost` · `used_credits` 는 **월간 누적 사용량**(잔여량 아님)
- 수집 견적은 엔진 비용 가드(클러스터 센터당 상한 · 보완 센터 프로브)를 전제로 과거 실측 사례를 들어 범위로 말한다 —
  `references/collect-guide.md` '비용 사례'. 단정 금지 · 사용자가 상한을 말하면 그 안에서만 실행
- 검색량은 관심 신호(판매·방문 아님) · 점유율은 "검색량 기준 근사" · null 은 결측(0 아님)
- 적재기는 데이터를 바꾸지 않는다 — 타입 변환만 하고, 실패한 칸 수를 기록한다
- 자가 검증(0크레딧 · 네트워크 없음): `lmdp selftest`
