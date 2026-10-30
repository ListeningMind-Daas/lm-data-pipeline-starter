# lm-data-pipeline 스타터 v1.0.2

ListeningMind DaaS 검색 데이터 **적재 파이프라인 워크스페이스**입니다.
품목·시장별 검색 데이터를 수집·정제·분석하고, 프로젝트마다 지정한 저장소에 **스키마 설계안 순서대로** 싣습니다.

| 적재 대상 | 상태 |
|---|---|
| 로컬 파일(CSV·Parquet) | 자가 검증 + 실데이터 적재·검증 완료 |
| Databricks (Unity Catalog) | DDL·적재 계획·자가 검증 통과 · 실제 계정 적재는 도입 시 `check --connect` 로 확인 |
| Snowflake | DDL·적재 계획·자가 검증 통과 · 실제 계정 적재는 도입 시 확인 |
| BigQuery | DDL·적재 계획·자가 검증 통과 · 실제 계정 적재는 확인하지 않았다 |

## 시작하기

1. 이 폴더를 원하는 위치에 두고 Claude Code / Claude Desktop 에서 워크스페이스로 엽니다
2. 첫 세션에서 온보딩(5문항 · API 키)이 시작됩니다
3. 예) `한국 로봇청소기 적재 프로젝트 만들어서 Snowflake ACME_MI 에 올려줘` 라고 말해 보세요

Python 3.11+ · `pandas` 필요. 대상별 드라이버는 `lm-data-pipeline` 스킬 '저장소 인증' 표를 따릅니다.

## 흐름

```
프로젝트 만들기(init) → 룰북 점검(check · rulebook) → DDL(ddl) → 수집(run · 크레딧)
 → 적재 계획(plan) → 적재(load · 층 순서 · 실패 층에서 멈춤) → 검증(verify)
```

- **스키마 설계안**: `.claude/skills/lm-data-pipeline/loader/design/schema_design.md` — 시장 데이터 9층 36테이블 · 트렌드 6층 29테이블
- 층 하나 = 스키마 하나(BigQuery 는 데이터셋 하나). 모든 테이블 끝에 `_run_id` · `_loaded_at` — 같은 run 을 다시 실으면 그 run 행만 바꾼다
- 비밀값은 환경변수로만. 설정 파일(`10_Projects/<프로젝트>/project.toml`)에는 대상 이름만 쓴다

## 자가 검증 (크레딧 0 · 네트워크 없음)

```bash
python3 .claude/skills/lm-data-pipeline/loader/lmdp.py selftest
```

합성 run 으로 두 파이프라인을 로컬에 두 번 싣고(두 번째도 행이 늘지 않아야 한다) 행 수를 대조하며, Databricks·Snowflake·BigQuery DDL 을 만들어 봅니다.

## 담지 않은 것

- 수집된 데이터(run) — 수집은 이 워크스페이스에서 직접 합니다
- 공유(Delta Sharing 등) 설정 — 대상 저장소의 공유 기능을 쓰세요

---
v1.0.2 · Ascent AI · 문의: ListeningMind DaaS 담당자
