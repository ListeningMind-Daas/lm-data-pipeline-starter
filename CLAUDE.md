# lm-data-pipeline

ListeningMind DaaS 검색 데이터 **적재 파이프라인 워크스페이스**.
품목·시장별로 검색 데이터를 수집·정제·분석하고, 프로젝트마다 지정한 저장소(로컬 파일 · Databricks · Snowflake · BigQuery)에
스키마 설계안 순서대로 싣습니다.

> **Ascent AI**가 제공하는 도구 · 배포 방식: 프로젝트 스킬(`.claude/skills/`) — 플러그인 설치 불필요

---

## 컨텍스트 파일 로딩

매 세션 시작 시:
1. `00_Context/about-me.md` 존재 확인 (없으면 아래 "초기 설정"으로 온보딩 — 최초 실행 시 정상 경로)
2. 있으면 → `about-me.md` 를 읽고 이후 작업에 적용
3. `10_Projects/<프로젝트>/project.toml` 이 있는 프로젝트를 다룰 때는 그 설정을 먼저 읽는다

## 초기 설정 (온보딩)

`00_Context/about-me.md` 가 없을 때만 자동 트리거. AskUserQuestion 으로 다음 5문항을 확인 후 `about-me.md` 를 만든다:

1. 이름/소속
2. **주로 쓸 적재 저장소** — 로컬 파일 / Databricks / Snowflake / BigQuery (→ `lmdp init` 의 `--target` 기본값)
3. 주로 다루는 시장·품목 (자유 기입 — 룰북 점검과 시드 제안에 참고)
4. 적재 주기 — 1회성 / 월간 갱신 / 분기 (→ 수집 범위 제안: 월간은 검색량 갱신 위주, 분기는 클러스터·경로 재수집)
5. **API 키 세팅** (아래 절차 — 최초 1회 필수)

### about-me.md 기록 형식 (Q&A 구조)

프로필은 **질문과 답의 기록**으로 저장한다. 문항별로 "질문 원문 → 답 → 적용" 3요소를 남긴다.

```markdown
# 사용자 프로필 — 온보딩 Q&A 기록

## Q1. 이름/소속은?
**A.** {이름} / {소속}

## Q2. 주로 쓸 적재 저장소는?
**A.** {로컬 파일 / Databricks / Snowflake / BigQuery}
→ 적용: 새 프로젝트의 적재 대상 기본값 (인증은 환경변수 — 키 값은 기록하지 않음)

## Q3. 주로 다루는 시장·품목은?
**A.** {답}
→ 적용: 룰북 점검 · 시드 제안

## Q4. 적재 주기는?
**A.** {답}
→ 적용: 수집 범위 제안

## Q5. API 키 세팅
**A.** {완료/보류} — 검증 호출 {성공 여부} (키 값은 절대 기록하지 않음)

## 메타
- 최초 설정: {YYYY-MM-DD} / 최근 갱신: {YYYY-MM-DD}
```

### API 키 세팅 절차 (최초 1회)

`00_Context/.lm-api-key` 가 없으면 **어떤 수집도 시작하기 전에** 다음을 실행한다:

1. AskUserQuestion 으로 발급받은 ListeningMind DaaS API 키를 요청
   (키가 없다고 하면 ListeningMind/Ascent 계약 담당을 통한 발급을 안내하고 수집 보류 — 스키마 설계·DDL·자가 검증은 키 없이 된다)
2. 받은 키를 `00_Context/.lm-api-key` 에 저장 — 값만 한 줄, `chmod 600`
3. **검증 호출**: `keyword_info` 1키워드(`data_type=ads_metrics`)로 확인(1크레딧) — 응답 `cost_detail.total_cost` 를 보고
   (`used_credits` 는 당월 누적 사용량으로 잔여량이 아님)
4. 401/403 이면 키 오탈자·발급 상태 재확인 (2회 실패 시 발급처 문의)

**보안 수칙**: API 키와 저장소 인증 값(토큰·비밀번호·키 파일 경로)은 파일·리포트·로그·커밋에 쓰지 않는다.
저장소 인증은 환경변수로만 넘긴다(`lm-data-pipeline` 스킬 '저장소 인증' 표). 노출이 의심되면 즉시 재발급을 안내한다.

---

## 응답 언어

사용자 입력 언어로 응답한다. `lm-data-pipeline` 스킬 실행 중에는 스킬 규칙(한국어)이 우선한다.

---

## 스킬

| 스킬명 | 역할 |
|---|---|
| `lm-data-pipeline` | 프로젝트 만들기 → 룰북 점검 → DDL → 수집(크레딧) → 적재 계획 → 적재 → 검증 → 보고 |

모든 명령은 `python3 .claude/skills/lm-data-pipeline/loader/lmdp.py …` — 절차와 규율은 스킬 `SKILL.md` 가 정본이다.

### 라우팅

```
① 적재 프로젝트 만들기·저장소 연결·DDL·적재·검증 발화 → lm-data-pipeline
② "○○ 품목 데이터 모아줘" (수집) → lm-data-pipeline — 크레딧 견적 고지·승인 후 실행
③ 모호할 때(파이프라인 mi·trend, 저장소, 시장이 불분명) → AskUserQuestion 으로 확인 (즉시 실행 금지)
```

---

## 폴더 구조

```
lm-data-pipeline-starter/
├── README.md · README-STARTER.md · CLAUDE.md
├── .claude/skills/lm-data-pipeline/
│   ├── SKILL.md · references/collect-guide.md
│   ├── loader/          lmdp.py · design/schema_design.json(.md) · sinks/(local·databricks·snowflake·bigquery)
│   └── pipelines/       mi/(시장 데이터 엔진 · ref 룰북) · trend/(트렌드 엔진 · ref 룰북 · _templates)
├── 00_Context/          about-me.md · .lm-api-key (온보딩 때 생성)
├── 10_Projects/<프로젝트>/project.toml
├── 90_Output/<프로젝트>/<run>/   원본 응답 · 로컬 테이블 · 비용 원장 · _load/ 적재 기록
└── 99_Scratch/
```

---

## 품질 기준

- 크레딧을 쓰는 단계(`lmdp run`) 전에는 견적을 범위로 고지하고 승인받는다. 사용자가 상한을 말하면 그 안에서만
- 적재 뒤에는 반드시 `lmdp verify` — 불일치가 있으면 완료라고 말하지 않는다. 변환 실패 칸(⚠)은 숨기지 않고 알린다
- 보고에는 이번 수집 크레딧(비용 원장 `total_cost` 합) · 적재 행 수 · 대상 위치 · 검증 결과 · 미확인 사항을 넣는다
- 검색량은 관심 신호(판매·방문 아님) · 점유율은 "검색량 기준 근사" · null 은 결측(0 아님) · 인과 단정 금지
- 인구통계(성·연령)는 gl=kr 에서만 제공된다 — 해외 시장은 "데이터 사양"으로 서술

---

## 트러블슈팅

| 증상 | 해결 |
|---|---|
| 스킬이 인식되지 않음 | `.claude/skills/lm-data-pipeline/SKILL.md` 존재 확인 · Claude Code/Desktop 재시작 |
| `run` 이 종료 코드 4 로 멈춤 | 룰북 미비 — `lmdp check` 가 알려 준 파일을 채운다(크레딧은 쓰지 않았다) |
| 적재가 한 층에서 멈춤 | `_load/*.jsonl` 의 FAILED 줄 오류 확인 → 원인 해결 후 같은 명령 재실행(같은 run 은 지우고 다시 싣는다) |
| 설정·연결 오류(종료 코드 3) | 환경변수 · catalog/database/project 이름 · 권한 확인 → `lmdp check <프로젝트> --connect` |
| 드라이버 없음 | 스킬 '저장소 인증' 표의 pip 패키지 설치 |
