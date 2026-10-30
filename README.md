# lm-data-pipeline 스타터 v1.0.2

ListeningMind DaaS 검색 데이터를 **수집 → 정제 → 분석 테이블**로 만들어, 프로젝트마다 지정한 저장소에 싣는 **데이터 적재 파이프라인**입니다.

- **두 파이프라인**
  - 시장 데이터(MI): 품목 시드 → 연관 키워드 확장 → 검색량·의도·클러스터·검색 경로 수집 → 9층 36테이블
  - 트렌드 유니버스: 프레임 → L1~L3 단계 수집 → 노이즈 정제 → 6층 29테이블
- **네 가지 적재 대상**: 로컬 파일(CSV·Parquet) · Databricks(Unity Catalog) · Snowflake · BigQuery
- **한 가지 적재 순서**: 스키마 설계안(`schema_design.json`)의 층 순서대로 싣습니다. 한 층이 실패하면 거기서 멈춥니다.

Claude Code / Claude Desktop 워크스페이스로 열어 대화로 쓰도록 만들었습니다. 같은 명령을 터미널에서 직접 실행해도 됩니다.

## 준비물

| 항목 | 내용 |
|---|---|
| Python | 3.11 이상 · `pandas` |
| ListeningMind DaaS API 키 | 수집할 때만 필요합니다. 발급은 ListeningMind/Ascent 계약 담당을 통해 받습니다. 키 없이도 스키마 설계안 보기·DDL 만들기·자가 검증은 됩니다 |
| 저장소 드라이버 | 적재 대상에 따라 다릅니다(아래 '적재 대상과 인증') |

## 시작하기

### Claude Code / Claude Desktop 으로

1. 저장소를 받습니다. `git clone` 하거나 [Releases](../../releases)의 zip 을 내려받아 풉니다
2. 폴더를 Claude Code / Claude Desktop 에서 워크스페이스로 엽니다
3. 첫 세션에서 온보딩(5문항 · API 키 등록)이 시작됩니다
4. 이렇게 말해 보세요: `한국 로봇청소기 적재 프로젝트 만들어서 Snowflake ACME_MI 에 올려줘`

크레딧을 쓰는 수집 단계 전에는 언제나 견적을 보여 주고 승인을 받습니다.

### 터미널에서 직접

```bash
export LM_API_KEY=<발급받은 키>        # 수집할 때만 · 파일에 쓰지 않습니다
alias lmdp="python3 .claude/skills/lm-data-pipeline/loader/lmdp.py"

lmdp selftest                                                    # 크레딧 0 · 네트워크 없음
lmdp init vac --pipeline mi --gl kr --category vacuum-cleaner --target local
lmdp check vac                                                   # 룰북 점검 — 빠진 품목·브랜드 행을 알려 줍니다
lmdp ddl vac --out vac_ddl.sql                                   # 대상 저장소용 DDL (연결 없음)
lmdp run vac --run-date 2026-10 -- --seeds 로봇청소기 무선청소기    # 수집 (크레딧)
lmdp plan vac --run <run 폴더>                                   # 적재 계획
lmdp load vac --run <run 폴더> --dry-run                         # 먼저 시험
lmdp load vac --run <run 폴더>
lmdp verify vac --run <run 폴더>                                 # 대상 행 수 = 원본 행 수
```

명령별 인자와 절차는 [`SKILL.md`](.claude/skills/lm-data-pipeline/SKILL.md)가 정본입니다.

## 흐름

```
프로젝트 만들기(init) → 룰북 점검(check · rulebook) → DDL(ddl) → 수집(run · 크레딧)
 → 적재 계획(plan) → 적재(load · 층 순서 · 실패 층에서 멈춤) → 검증(verify)
```

- **층 하나 = 스키마 하나**입니다. BigQuery 에서는 데이터셋 하나입니다. 예: `acme_mi.vac_silver_master.keywords_master`
- 모든 테이블 끝에 `_run_id` · `_loaded_at` 열이 붙습니다. 같은 run 을 다시 실으면 그 run 의 행만 바뀝니다
- 적재기는 값을 바꾸지 않습니다. 타입 변환만 하고, 변환에 실패한 칸 수를 기록합니다
- run 폴더(`90_Output/<프로젝트>/<run>/`)에 원본 응답 · 로컬 테이블 · 비용 원장 · 적재 기록이 남습니다

## 적재 대상과 인증

비밀값은 **환경변수로만** 넘깁니다. 설정 파일(`10_Projects/<프로젝트>/project.toml`)에는 대상 이름만 씁니다.

| 대상 | 환경변수 | 드라이버 |
|---|---|---|
| 로컬 파일 | — | (Parquet 이면 `pyarrow`) |
| Databricks | `DATABRICKS_HOST` · `DATABRICKS_HTTP_PATH` · `DATABRICKS_TOKEN` | `databricks-sql-connector` |
| Snowflake | `SNOWFLAKE_ACCOUNT` · `SNOWFLAKE_USER` · `SNOWFLAKE_PRIVATE_KEY_PATH`(권장) 또는 `SNOWFLAKE_PASSWORD` | `snowflake-connector-python[pandas]` |
| BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` 또는 `gcloud auth application-default login` | `google-cloud-bigquery` · `pyarrow` · `db-dtypes` |

| 대상 | 확인 상태 |
|---|---|
| 로컬 파일 | 자가 검증 + 실데이터 적재·검증 완료 |
| Databricks · Snowflake | DDL·적재 계획·자가 검증 통과 · 실제 계정 적재는 도입 시 `lmdp check <프로젝트> --connect` 로 확인 |
| BigQuery | DDL·적재 계획·자가 검증 통과 · 실제 계정 적재는 확인하지 않았습니다 |

## API 와 크레딧

- 엔드포인트 기본값: `https://listeningmind-data-api.ascentlab.io` (`LM-API-KEY` 헤더 인증 · 명세 v1.0.15 기준)
- 두 엔진은 서로 다른 환경변수로 기본 주소를 바꿉니다: 시장 데이터 엔진은 `LM_BASE_URL`, 트렌드 엔진은 `LM_API_BASE`
- 쓰는 API: `keyword_info` · `intent_finder` · `cluster_finder` · `path_finder`
- 호출 1건의 비용은 응답의 `cost_detail.total_cost` 입니다. `used_credits` 는 **당월 누적 사용량**이고 잔여량이 아닙니다
- 이번 run 이 쓴 크레딧은 run 폴더의 비용 원장을 합한 값입니다
  - 시장 데이터: `tables/11_cost/api_call_log.csv`
  - 트렌드: `cost/API_CALL_LOG.csv`
- 비용 사례와 수집 범위를 정하는 기준은 [`references/collect-guide.md`](.claude/skills/lm-data-pipeline/references/collect-guide.md)에 있습니다

## 자가 검증 (크레딧 0 · 네트워크 없음)

```bash
python3 .claude/skills/lm-data-pipeline/loader/lmdp.py selftest
```

합성 run 으로 두 파이프라인을 로컬에 두 번 싣습니다(두 번째에도 행이 늘지 않아야 합니다). 행 수를 대조하고, Databricks·Snowflake·BigQuery DDL 을 만들어 봅니다.

## 폴더 구조

```
lm-data-pipeline-starter/
├── README.md · README-STARTER.md · CLAUDE.md
├── .claude/skills/lm-data-pipeline/
│   ├── SKILL.md · references/collect-guide.md
│   ├── loader/          lmdp.py · design/schema_design.json(.md) · sinks/(local·databricks·snowflake·bigquery)
│   └── pipelines/       mi/(시장 데이터 엔진 · ref 룰북) · trend/(트렌드 엔진 · ref 룰북 · _templates)
├── 00_Context/          about-me.md · .lm-api-key (온보딩 때 생성 · 커밋하지 않음)
├── 10_Projects/<프로젝트>/project.toml
├── 90_Output/<프로젝트>/<run>/
└── 99_Scratch/
```

스키마 설계안 전체(층·테이블·컬럼·타입)는 [`schema_design.md`](.claude/skills/lm-data-pipeline/loader/design/schema_design.md)에서 볼 수 있습니다.

## 담지 않은 것

- 수집된 데이터(run). 수집은 이 워크스페이스에서 직접 합니다
- 공유(Delta Sharing 등) 설정. 대상 저장소의 공유 기능을 쓰세요

## 데이터 해석 주의

- 검색량은 관심 신호입니다. 판매량이나 방문 수가 아닙니다
- 점유율은 "검색량 기준 근사"로 읽습니다
- 빈 값(null)은 결측입니다. 0 으로 읽지 않습니다
- 검색량은 월 단위로 제공되고, 최신 월이 반영되기까지 시차가 있습니다

---
v1.0.2 · Ascent AI · 문의: [ListeningMind DaaS](https://www.listeningmind.com/ko/daas?inquiry=open)
