# 시장 데이터(MI) 룰북 — 품목마다 채운다

엔진은 run 마다 이 룰북에서 **그 run 의 시장(gl) + (그 품목 또는 공통 `*`) 행만** 읽어 태깅·노이즈 제거·레이어 추정에 쓰고,
적재 시 `ref` 층에 스냅샷으로 남긴다. 기본으로 들어 있는 것은 공통(`*`) 규칙뿐이다 — 품목·브랜드 행은 프로젝트마다 채운다.

| 파일 | 기본 상태 | 채우지 않으면 |
|---|---|---|
| `category_rules/category_rules.csv` | 헤더만 | **품질 점검이 멈춘다** — `lmdp check` 가 먼저 알려 준다 |
| `brand_rules/brand_rules.csv` | 헤더만 | 모든 검색이 '브랜드 없는 검색'으로 분류돼 점유율이 비고, 품질 점검이 경고(WARN)한다 |
| `noise_patterns/noise_patterns.csv` | 공통 규칙 16행 — kr: 성인·금융·채용·불법·사전·사후서비스·중고·매장·사용법·에러·리퍼·렌탈해지 · us: 금융·채용·이메일/로그인. 품목 전용 노이즈(동음이의·혼입)는 run 결과를 보고 품목 행으로 더한다 | — |
| `seed_layer_config/seed_layer_config.csv` | kr·us 레이어 L1~L5 정의 | — |
| `market_calendar/market_calendar.csv` | kr 공통 시즌 4행 | 시즌 표기가 비어도 적재는 된다 |

## category_rules.csv — 품목 하위 분류

```
gl,category_key,pattern,category_lvl1,category_lvl2,priority,version,note
kr,vacuum-cleaner,.*,청소기,청소기 일반,1,v1,품목 전체(마지막에 걸리는 기본 행)
kr,vacuum-cleaner,로봇|robot,청소기,로봇청소기,24,v1,로봇형
kr,vacuum-cleaner,무선|스틱|핸디,청소기,무선청소기(스틱),22,v1,무선 스틱
```

- `category_key` 는 `lmdp init --category` 와 같은 값(영문 kebab-case)
- `pattern` 은 정규식(대소문자 무시) · `priority` 가 큰 행이 먼저 맞는다 · 품목 전체를 받는 `.*` 기본 행을 하나 둔다

## brand_rules.csv — 브랜드 사전

```
gl,category_key,pattern,brand,priority,version,note
kr,*,<브랜드 한글>|<brand 영문>,<브랜드 대표 표기>,12,v1,사명
kr,*,<시리즈명>|<series>,<브랜드 대표 표기>,20,v1,서브 브랜드·시리즈
kr,vacuum-cleaner,<이 품목에서만 쓰는 라인명>,<브랜드 대표 표기>,20,v1,품목 전용 라인
```

- 한 브랜드의 표기 변형(한글·영문·오타)을 한 행의 `pattern` 에 `|` 로 묶는다
- 한 글자·일반어와 겹치는 브랜드명은 오탐이 크다 — 시리즈명처럼 더 구체적인 표기를 쓴다
- 관심·경쟁 브랜드(`lmdp init --focus-brand · --rival-brand`)는 여기 `brand` 값과 같게 쓴다
