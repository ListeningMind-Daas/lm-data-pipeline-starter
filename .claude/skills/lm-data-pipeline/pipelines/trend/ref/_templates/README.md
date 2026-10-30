# 트렌드 새 품목 룰북 템플릿

트렌드 파이프라인은 품목 키(`category`)로 룰북 4종을 찾는다. 새 품목은 이 템플릿으로 만든다 —
`lmdp.py rulebook <프로젝트>` 가 품목 키로 복사한다. 꺾쇠(`<…>`) 자리를 모두 채우기 전에는 `lmdp.py check`가 수집을 막는다.

| 파일 | 복사 위치 | 쓰는 단계 |
|---|---|---|
| frame.json | `ref/frame/<품목>_kr_frame.json` | 01 L0 프레임 · 02 L1 확장 · 03 L2 게이트 |
| dimensions.json | `ref/dimensions/<품목>_dimensions.json` | 06 차원 태깅 |
| brand_rules.json | `ref/brand_rules/<품목>.json` | 06 브랜드 마스터 · 08 브랜드 점유율 |
| category_rules.json | `ref/category_rules/<품목>.json` | 05 대표 키워드·세그먼트 |

예시(완성본): `trend_fashion` 4종.
