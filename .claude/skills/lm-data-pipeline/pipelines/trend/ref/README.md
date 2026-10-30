# ref/ — 룰북 (Rulebook)

모든 런이 공유하는 **정제 규칙의 단일 소스**. 런 폴더(`projects/...`)와 분리해 버전 관리한다.

```
ref/
├─ brand_rules/      <category_key>.json           브랜드 별칭·그룹핑
├─ category_rules/   <category_key>.json           표기변형→대표키워드 통합 + 세그먼트(CATEGORY/STYLE_TREND/DIMENSION)
├─ noise_patterns/   common.json                   키워드 정규화·노이즈 제거 (공용)
├─ panels/           <category_key>_panel.json     트렌드 모니터링 고정 패널 + 속도 게이팅 룰
├─ anchors/          <category_key>_anchors.json   신규 트렌드 발굴 앵커 + 형태소/동음이의 룰 (cluster_finder)
└─ dimensions/       <category_key>_dimensions.json 차원 축(체형·상황·수식어) 태깅 룰
```

## panels/ — 모니터링 패널
트렌드 모니터링용 **고정 추적 유니버스 + 룰**. 런 간 차분(cross-run diff)의 기준.
- `inclusion_floor`: 속도 게이팅 — stable(vol_avg≥300) 또는 emerging(vol_avg≥50 & |trend|≥0.3)
- `discovery`: 매 사이클 신규 키워드 탐색(intent_finder, volume_threshold=30)
- `core_panel`: 매 사이클 동일 추적하는 키워드 셋 (비교 가능성 보장)
- `refresh_cadence`: monthly

## anchors/ — 신규 트렌드 발굴 (cluster_finder)
`intent_finder`(facet)가 못 잡는 **신규 미감**을 검색관계 그래프로 발굴. hop=1·분기 운영(≈3K crd/앵커).
형태소(`-코어`/`-룩`)·동음이의 필터·속도 게이팅 룰 포함. → `05_new_trend_discovery`.

## dimensions/ — 차원 축 태깅
유니버스를 "카테고리 × 스타일" 2축 → **+ 체형·상황·수식어** 로 확장. 차원은 seed가 아니라 **태그**.
`02_silver` 섹션 D에서 키워드에 태그 부여 → `gold_tag_momentum`(스타일×성별/시즌 교차) 활용.

## 버전 관리 (재현성 핵심)

룰북은 시간이 지나며 바뀐다. 파이프라인은 실행 시 `ref/` 전체의 **content hash**를 계산해
각 런의 `metadata/metadata.json` → `ref_version` 에 기록한다.
→ 어떤 룰 버전으로 만든 silver/gold인지 언제든 추적 가능.

`ref_version` = `sha256(ref/ 하위 모든 파일을 경로순 정렬해 연결한 바이트)` 앞 12자.
