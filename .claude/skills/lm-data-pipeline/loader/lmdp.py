#!/usr/bin/env python3
"""lmdp — 데이터 적재 파이프라인 프로젝트 도구. 프로젝트마다 지정한 저장소에 설계안(schema_design.json) 순서대로 싣는다.

  init    <프로젝트> --pipeline mi|trend --gl kr --category <품목> --target local|databricks|snowflake|bigquery [...]
  rulebook <프로젝트>                      트렌드 새 품목 룰북 4종을 템플릿으로 만든다
  check   <프로젝트> [--connect]          설정·룰북·환경변수 점검 (--connect: 대상에 접속만 해 본다)
  ddl     <프로젝트> [--out 파일]          대상 저장소용 DDL — 층 순서대로 (연결 없음)
  run     <프로젝트> --run-date YYYY-MM[-DD] [--phases …] [-- 엔진 인자]   수집·로컬 적재 엔진 실행 (크레딧 사용)
  plan    <프로젝트> --run <run 폴더>      적재 계획 — 층·테이블·원본 행 수·누락 (연결 없음)
  load    <프로젝트> --run <run 폴더> [--layers a,b] [--dry-run]   층 순서대로 스키마·테이블 생성 → 적재. 실패한 층에서 멈춘다
  verify  <프로젝트> --run <run 폴더>      대상의 행 수를 원본 파일과 대조
  selftest                                 합성 데이터로 두 파이프라인 × 로컬 적재·재적재·검증 + 4개 대상 DDL (0크레딧 · 네트워크 없음)

프로젝트 설정: 10_Projects/<프로젝트>/project.toml · run 산출: 90_Output/<프로젝트>/<run 폴더>/ · 적재 기록: 그 아래 _load/
비밀값(API 키·토큰·비밀번호)은 설정 파일에 쓰지 않는다 — API 키는 00_Context/.lm-api-key, 저장소 인증은 환경변수.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import sinks  # noqa: E402

DESIGN = json.loads((HERE / "design" / "schema_design.json").read_text(encoding="utf-8"))
TARGET_KEYS = {"local": ["path", "format"], "databricks": ["catalog", "schema_prefix", "staging_volume", "batch_rows"],
               "snowflake": ["database", "schema_prefix", "warehouse", "role", "create_database"],
               "bigquery": ["project", "location", "schema_prefix"]}


def engine_dir(name: str) -> Path:
    """엔진 위치 — <스킬>/pipelines/<이름>."""
    for p in (HERE.parent / "pipelines" / name, HERE.parent / name):
        if p.exists():
            return p
    raise SystemExit(f"{name} 엔진 폴더를 찾지 못했습니다")


def workspace(arg: str | None) -> Path:
    if arg:
        return Path(arg).resolve()
    p = Path.cwd().resolve()
    for q in (p, *p.parents):
        if (q / "10_Projects").is_dir():
            return q
    return p


# ── 설정 파일 (TOML · 표준 라이브러리로 읽고 단순 형식으로 쓴다) ────────────
def toml_dump(d: dict) -> str:
    def val(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, list):
            return "[" + ", ".join(val(x) for x in v) + "]"
        return json.dumps(str(v), ensure_ascii=False)
    out = []
    for sec, kv in d.items():
        out.append(f"[{sec}]")
        out += [f"{k} = {val(v)}" for k, v in kv.items() if v is not None and v != ""]
        out.append("")
    return "\n".join(out)


def toml_load(text: str) -> dict:
    """TOML 읽기 — 3.11+ 는 tomllib, 그 전은 tomli, 둘 다 없으면 이 도구가 쓰는 단순 형식([절] · 키 = 값)만 읽는다."""
    try:
        import tomllib
        return tomllib.loads(text)
    except ImportError:
        pass
    try:
        import tomli
        return tomli.loads(text)
    except ImportError:
        pass
    out, cur = {}, None
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip() if not raw.strip().startswith(("\"", "'")) else raw.strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            cur = out.setdefault(line[1:-1].strip(), {})
            continue
        k, v = (x.strip() for x in line.split("=", 1))
        v = {"true": "true", "false": "false"}.get(v, v)
        cur[k] = json.loads(v.replace("'", '"')) if v[:1] in '"[' or v in ("true", "false") or v.lstrip("-").replace(".", "", 1).isdigit() else v
    return out


def load_project(ws: Path, name: str) -> dict:
    p = ws / "10_Projects" / name / "project.toml"
    if not p.exists():
        raise SystemExit(f"프로젝트 설정이 없습니다: {p} — 먼저 `lmdp.py init {name} …`")
    cfg = toml_load(p.read_text(encoding="utf-8"))
    pl = cfg.get("project", {}).get("pipeline")
    if pl not in DESIGN["pipelines"]:
        raise SystemExit(f"project.pipeline 은 mi 또는 trend: {pl!r}")
    cfg["_name"], cfg["_ws"] = name, ws
    return cfg


def out_dir(cfg) -> Path:
    return cfg["_ws"] / "90_Output" / cfg["_name"]


def sink_of(cfg) -> sinks.Sink:
    t = dict(cfg.get("target", {}))
    if t.get("type") == "local" and t.get("path"):
        p = Path(t["path"])
        t["path"] = str(p if p.is_absolute() else cfg["_ws"] / p)
    return sinks.make(t)


# ── 원본 읽기·타입 맞추기 ─────────────────────────────────────────────────
def read_source(run_dir: Path, table: dict):
    import pandas as pd
    p = run_dir / table["source"]
    if not p.exists():
        return None
    if p.suffix == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        return pd.DataFrame(data if isinstance(data, list) else [data])
    return pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""], encoding="utf-8-sig")


def _dt(src, utc=False):
    """날짜·시각 — 한 열에 마이크로초 유무가 섞여도 읽는다(첫 값으로 형식을 추정하면 나머지가 버려진다)."""
    import pandas as pd
    try:
        return pd.to_datetime(src, errors="coerce", utc=utc, format="ISO8601")
    except (TypeError, ValueError):
        return pd.to_datetime(src, errors="coerce", utc=utc)


def conform(df, table: dict, run_id: str, loaded_at: str):
    """설계안 컬럼·타입으로 맞춘다. 원본에 값이 있는데 변환에 실패한 칸 수를 돌려준다(조용히 버리지 않는다)."""
    import pandas as pd
    lower = {c.lower(): c for c in df.columns}
    out, lost = pd.DataFrame(index=df.index), {}
    for c in table["columns"]:
        n, t = c["name"], c["type"]
        if n == "_run_id":
            out[n] = run_id
            continue
        if n == "_loaded_at":
            out[n] = pd.Timestamp(loaded_at)
            continue
        src = df[lower[n.lower()]] if n.lower() in lower else pd.Series([None] * len(df), index=df.index, dtype=object)
        had = src.notna() & (src.astype(str).str.strip() != "")
        if t in ("int", "bigint"):
            v = pd.to_numeric(src, errors="coerce").round().astype("Int64")
        elif t == "double":
            v = pd.to_numeric(src, errors="coerce").astype("Float64")
        elif t == "boolean":
            v = src.map(lambda x: None if x is None or (isinstance(x, float) and x != x) or str(x).strip() == "" else
                        str(x).strip().lower() in ("true", "1", "t", "y", "yes")).astype("boolean")
        elif t == "date":
            v = _dt(src).dt.date
        elif t == "timestamp":
            v = _dt(src, utc=True).dt.tz_localize(None)
        elif t == "json":
            v = src.map(lambda x: x if x is None or isinstance(x, str) else json.dumps(x, ensure_ascii=False))
        else:
            v = src.map(lambda x: None if x is None or (isinstance(x, float) and x != x) else str(x))
        bad = int((had & pd.Series(v, index=df.index).isna()).sum())
        if bad:
            lost[n] = bad
        out[n] = v
    return out, lost


def ordered(pipeline: str, layers_arg: str | None):
    d = DESIGN["pipelines"][pipeline]
    want = [x.strip() for x in layers_arg.split(",")] if layers_arg else None
    for L in sorted(d["layers"], key=lambda x: x["order"]):
        if want and L["key"] not in want:
            continue
        yield L, [t for t in d["tables"] if t["layer"] == L["key"]]


# ── 룰북 점검 (수집 전에 — 크레딧을 쓰기 전에 막는다) ────────────────────
TREND_RULEBOOKS = {"frame.json": "frame/{c}_kr_frame.json", "dimensions.json": "dimensions/{c}_dimensions.json",
                   "brand_rules.json": "brand_rules/{c}.json", "category_rules.json": "category_rules/{c}.json"}


def rulebook_problems(cfg) -> list[str]:
    pj, out = cfg["project"], []
    if pj["pipeline"] == "trend":
        ref = engine_dir("trend") / "ref"
        for rel in TREND_RULEBOOKS.values():
            p = ref / rel.format(c=pj["category"])
            if not p.exists():
                out.append(f"룰북 없음: {p.relative_to(ref)} — `lmdp.py rulebook {cfg['_name']}` 로 템플릿 복사")
            elif "<" in p.read_text(encoding="utf-8") and ">" in p.read_text(encoding="utf-8"):
                out.append(f"룰북에 채우지 않은 자리(<…>)가 있다: {p.relative_to(ref)}")
    else:
        import csv as _csv
        rules = engine_dir("mi") / "ref" / "category_rules" / "category_rules.csv"
        with open(rules, encoding="utf-8-sig") as f:
            keys = {(r.get("gl"), r.get("category_key")) for r in _csv.DictReader(f)}
        if (pj["gl"], pj["category"]) not in keys and ("*", pj["category"]) not in keys:
            out.append(f"품목 룰북에 {pj['gl']}·{pj['category']} 행이 없다 — ref/category_rules/category_rules.csv 에 추가(품질 점검이 막는다 · ref/README.md)")
    return out


# ── 명령 ────────────────────────────────────────────────────────────────
def cmd_rulebook(a):
    cfg = load_project(workspace(a.workspace), a.name)
    pj = cfg["project"]
    if pj["pipeline"] != "trend":
        raise SystemExit("rulebook 은 트렌드 파이프라인용 — MI 는 ref/*/ CSV 에 행을 추가한다")
    ref = engine_dir("trend") / "ref"
    for tpl, rel in TREND_RULEBOOKS.items():
        dst = ref / rel.format(c=pj["category"])
        if dst.exists():
            print(f"이미 있음: {dst.relative_to(ref)}")
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text((ref / "_templates" / tpl).read_text(encoding="utf-8").replace("{category}", pj["category"]), encoding="utf-8")
        print(f"만들었음: {dst.relative_to(ref)} — 꺾쇠 자리를 채운다")
    return 0


def cmd_init(a):
    ws = workspace(a.workspace)
    d = ws / "10_Projects" / a.name
    p = d / "project.toml"
    if p.exists() and not a.force:
        raise SystemExit(f"이미 있습니다: {p} (--force 로 덮어쓰기)")
    target = {"type": a.target}
    for k in TARGET_KEYS[a.target]:
        v = getattr(a, k, None)
        if v not in (None, ""):
            target[k] = v
    if a.target == "local":
        target.setdefault("path", f"90_Output/{a.name}/warehouse")
        target.setdefault("format", "csv")
    sinks.make({**target, "path": target.get("path", "x")})      # 필수값 점검
    cfg = {"project": {"name": a.name, "pipeline": a.pipeline, "gl": a.gl, "category": a.category,
                       "category_name": getattr(a, "category_name", None),
                       "focus_brand": a.focus_brand, "rival_brand": a.rival_brand},
           "target": target, "load": {"layers": []}}
    d.mkdir(parents=True, exist_ok=True)
    p.write_text("# lmdp 프로젝트 설정 — 비밀값은 여기에 쓰지 않는다(환경변수)\n" + toml_dump(cfg), encoding="utf-8")
    print(f"만들었습니다: {p}")
    return 0


def cmd_check(a):
    cfg = load_project(workspace(a.workspace), a.name)
    s = sink_of(cfg)
    pl = cfg["project"]["pipeline"]
    print(f"프로젝트 {a.name} · 파이프라인 {pl} · 대상 {s.describe()}")
    key = cfg["_ws"] / "00_Context" / ".lm-api-key"
    print(f"API 키 파일: {'있음' if key.exists() or os.environ.get('LM_API_KEY') else '없음 — 수집(run) 전에 필요'}")
    probs = rulebook_problems(cfg)
    print("룰북: 준비됨" if not probs else "룰북:\n  " + "\n  ".join(probs))
    if cfg["project"]["pipeline"] == "mi":
        import csv as _csv
        with open(engine_dir("mi") / "ref" / "brand_rules" / "brand_rules.csv", encoding="utf-8-sig") as f:
            n = sum(1 for r in _csv.DictReader(f) if r.get("gl") == cfg["project"]["gl"] and r.get("category_key") in ("*", cfg["project"]["category"]))
        if not n:
            print("  ⚠ 브랜드 사전에 이 시장·품목 행이 없다 — 점유율이 비고 품질 점검이 경고한다(ref/brand_rules/brand_rules.csv · ref/README.md)")
    if a.connect:
        s.connect()
        try:
            print(f"접속 확인: {s.ping()}")
        finally:
            s.close()
    return 0


def cmd_ddl(a):
    cfg = load_project(workspace(a.workspace), a.name)
    s = sink_of(cfg)
    lines = [f"-- {a.name} · {cfg['project']['pipeline']} · {s.describe()} · 설계안 v{DESIGN['version']}"]
    for L, tables in ordered(cfg["project"]["pipeline"], ",".join(cfg.get("load", {}).get("layers") or []) or None):
        lines.append(f"\n-- [{L['order']}] {L['key']} — {L['name']}: {L['what']}")
        lines.append(s.ddl_namespace(L["key"]) + ";")
        lines += [s.ddl_table(L["key"], t) + ";" for t in tables]
    text = "\n".join(lines) + "\n"
    if a.out:
        Path(a.out).write_text(text, encoding="utf-8")
        print(f"→ {a.out}")
    else:
        print(text)
    return 0


def cmd_run(a, extra):
    cfg = load_project(workspace(a.workspace), a.name)
    pj = cfg["project"]
    if probs := rulebook_problems(cfg):
        print("수집 전에 멈춤 — 룰북을 먼저 채운다(크레딧을 쓰지 않았다):\n  " + "\n  ".join(probs))
        return 4
    envv = dict(os.environ, LM_PROJECTS_ROOT=str(out_dir(cfg)))
    key = cfg["_ws"] / "00_Context" / ".lm-api-key"
    if not envv.get("LM_API_KEY") and key.exists():
        envv["LM_API_KEY"] = key.read_text(encoding="utf-8").strip()        # 값은 출력하지 않는다
    for k, e in (("focus_brand", "LM_FOCUS_BRAND"), ("rival_brand", "LM_RIVAL_BRAND"), ("category_name", "LM_CATEGORY_NAME")):
        if pj.get(k):
            envv[e] = pj[k]
    out_dir(cfg).mkdir(parents=True, exist_ok=True)
    if pj["pipeline"] == "mi":
        argv = [sys.executable, str(engine_dir("mi") / "scripts" / "run_pipeline.py"), "--gl", pj["gl"],
                "--category", pj["category"], "--run-date", a.run_date]
        if a.phases:
            argv += ["--phases", a.phases]
    else:
        envv.update(TREND_FASHION_GL=pj["gl"], TREND_FASHION_CATEGORY=pj["category"], TREND_FASHION_RUN_DATE=a.run_date)
        argv = [sys.executable, str(engine_dir("trend") / "pipelines" / "trend_fashion" / "run_all.py")]
        ph = (a.phases or "collect").strip()
        if ph == "collect":                     # 01~04 API 수집(크레딧) → 05~08 빌드
            argv += ["--collect"]
        elif ph == "build":                     # 저장된 원본으로 05~08 만 (0크레딧)
            argv += ["--from", "05", "--to", "08"]
        elif "-" in ph:                         # 예 05-08
            f, t = ph.split("-", 1)
            argv += ["--from", f.zfill(2), "--to", t.zfill(2)]
        else:                                   # 예 06
            argv += ["--stage", ph.zfill(2)]
    argv += extra
    print("실행:", " ".join(x if " " not in x else json.dumps(x, ensure_ascii=False) for x in argv[1:]))
    print(f"run 폴더: {out_dir(cfg)}/{pj['gl']}_{pj['category']}_{a.run_date}")
    return subprocess.call(argv, env=envv)


def _run_dir(cfg, run: str) -> Path:
    p = out_dir(cfg) / run
    if not p.is_dir():
        raise SystemExit(f"run 폴더가 없습니다: {p}")
    return p


def cmd_plan(a):
    cfg = load_project(workspace(a.workspace), a.name)
    rd, s = _run_dir(cfg, a.run), sink_of(cfg)
    print(f"적재 계획 · {a.name} · run {a.run} → {s.describe()}")
    total = miss = 0
    for L, tables in ordered(cfg["project"]["pipeline"], a.layers or ",".join(cfg.get("load", {}).get("layers") or []) or None):
        print(f"\n[{L['order']}] {L['key']} — {L['name']}  → {s.ns(L['key'])}")
        for t in tables:
            df = read_source(rd, t)
            n = None if df is None else len(df)
            total += n or 0
            miss += n is None
            print(f"    {t['name']:<28} {'원본 없음' if n is None else f'{n:,}행':>12}  ← {t['source']}")
    print(f"\n합계 {total:,}행 · 원본 없는 테이블 {miss}개")
    return 0


def cmd_load(a):
    cfg = load_project(workspace(a.workspace), a.name)
    rd, s = _run_dir(cfg, a.run), sink_of(cfg)
    run_id, now = a.run, datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "")
    log_dir = rd / "_load"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"{s.kind}_{now.replace(':', '').replace('-', '')}.jsonl"
    rec = []

    def note(**kw):
        kw["at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        rec.append(kw)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(kw, ensure_ascii=False, default=str) + "\n")

    print(f"적재 · {a.name} · run {run_id} → {s.describe()}{' · 연습(연결 없음)' if a.dry_run else ''}")
    if not a.dry_run:
        s.connect()
    status = 0
    try:
        for L, tables in ordered(cfg["project"]["pipeline"], a.layers or ",".join(cfg.get("load", {}).get("layers") or []) or None):
            print(f"[{L['order']}] {L['key']} — {L['name']}")
            try:
                if not a.dry_run:
                    s.ensure_namespace(L["key"])
                for t in tables:
                    df = read_source(rd, t)
                    if df is None:
                        note(layer=L["key"], table=t["name"], status="MISSING", source=t["source"])
                        print(f"    {t['name']:<28} 원본 없음 — 건너뜀")
                        continue
                    frame, lost = conform(df, t, run_id, now)
                    if a.dry_run:
                        n = len(frame)
                    else:
                        s.ensure_table(L["key"], t)
                        n = s.replace(L["key"], t, frame, run_id)
                    note(layer=L["key"], table=t["name"], status="DRY" if a.dry_run else "LOADED", rows=n, lost=lost)
                    print(f"    {t['name']:<28} {n:>10,}행" + (f"  ⚠ 변환 실패 칸 {lost}" if lost else ""))
            except Exception as e:                              # 이 층에서 멈춘다 — 뒤 층은 싣지 않는다
                note(layer=L["key"], status="FAILED", error=f"{type(e).__name__}: {e}")
                print(f"    ✗ {type(e).__name__}: {e}\n멈춤 — {L['key']} 층에서 실패. 뒤 층은 싣지 않았습니다")
                status = 2
                break
    finally:
        if not a.dry_run:
            s.close()
    loaded = sum(r.get("rows", 0) for r in rec if r.get("status") in ("LOADED", "DRY"))
    print(f"{'연습 ' if a.dry_run else ''}완료 {loaded:,}행 · 기록 {log_path}" if not status else f"기록 {log_path}")
    return status


def cmd_verify(a):
    cfg = load_project(workspace(a.workspace), a.name)
    rd, s = _run_dir(cfg, a.run), sink_of(cfg)
    s.connect()
    bad = 0
    try:
        for L, tables in ordered(cfg["project"]["pipeline"], a.layers or ",".join(cfg.get("load", {}).get("layers") or []) or None):
            for t in tables:
                df = read_source(rd, t)
                if df is None:
                    continue
                got = s.count(L["key"], t, a.run)
                ok = got == len(df)
                bad += not ok
                print(f"{'✓' if ok else '✗'} {L['key']}.{t['name']:<28} 원본 {len(df):>9,} · 대상 {got:>9,}")
    finally:
        s.close()
    print("검증 통과" if not bad else f"불일치 {bad}개")
    return 0 if not bad else 1


# ── 자가 검증 ────────────────────────────────────────────────────────────
def _synth(run_dir: Path, pipeline: str, n: int = 3):
    import csv as _csv
    sample = {"string": lambda i: f"값{i}", "int": lambda i: i, "bigint": lambda i: 1000 + i, "double": lambda i: i + 0.5,
              "boolean": lambda i: "true" if i % 2 else "false", "date": lambda i: f"2026-0{i + 1}-01",
              "timestamp": lambda i: f"2026-09-0{i + 1} 12:00:00", "json": lambda i: json.dumps({"i": i})}
    for t in DESIGN["pipelines"][pipeline]["tables"]:
        cols = [c for c in t["columns"] if not c["name"].startswith("_")]
        p = run_dir / t["source"]
        p.parent.mkdir(parents=True, exist_ok=True)
        rows = [{c["name"]: sample[c["type"]](i) for c in cols} for i in range(n)]
        if p.suffix == ".json":
            p.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        else:
            with open(p, "w", encoding="utf-8", newline="") as f:
                w = _csv.DictWriter(f, fieldnames=[c["name"] for c in cols])
                w.writeheader()
                w.writerows(rows)


def cmd_selftest(a):
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        ws = Path(tmp)
        (ws / "10_Projects").mkdir()
        for pl, cat in (("mi", "vacuum-cleaner"), ("trend", "trend_fashion")):
            name = f"selftest-{pl}"
            ns = argparse.Namespace(workspace=str(ws), name=name, pipeline=pl, gl="kr", category=cat, target="local",
                                    path=None, format="csv", focus_brand=None, rival_brand=None, force=True)
            cmd_init(ns)
            run = f"kr_{cat}_2026-07"
            _synth(ws / "90_Output" / name / run, pl)
            la = argparse.Namespace(workspace=str(ws), name=name, run=run, layers=None, dry_run=False)
            for i in (1, 2):                                   # 두 번 실어도 한 번만 남아야 한다
                if cmd_load(la):
                    fails.append(f"{pl} 적재 {i}회차 실패")
            if cmd_verify(argparse.Namespace(workspace=str(ws), name=name, run=run, layers=None)):
                fails.append(f"{pl} 검증 불일치(재적재 멱등 실패)")
            for kind, extra in (("databricks", {"catalog": "demo_cat"}), ("snowflake", {"database": "DEMO_DB"}),
                                ("bigquery", {"project": "demo-project"})):
                cfg = load_project(ws, name)
                cfg["target"] = {"type": kind, **extra}
                s = sink_of(cfg)
                n = sum(1 for _, ts in ordered(pl, None) for t in ts if s.ddl_table(t["layer"], t).startswith("CREATE TABLE"))
                if n != len(DESIGN["pipelines"][pl]["tables"]):
                    fails.append(f"{pl} {kind} DDL {n}개")
            print(f"[{pl}] 로컬 적재·재적재·검증 · DDL 3종 확인")
    print("자가 검증 통과" if not fails else "실패: " + " · ".join(fails))
    return 0 if not fails else 1


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    extra = []
    if "--" in argv:
        i = argv.index("--")
        argv, extra = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(prog="lmdp", description=__doc__.split("\n")[0])
    ap.add_argument("--workspace", help="워크스페이스 루트(기본: 10_Projects 가 있는 상위 폴더)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init")
    p.add_argument("name")
    p.add_argument("--pipeline", choices=list(DESIGN["pipelines"]), required=True)
    p.add_argument("--gl", default="kr", choices=["kr", "us", "jp"])
    p.add_argument("--category", required=True)
    p.add_argument("--target", choices=list(sinks.KINDS), default="local")
    for k in sorted({k for v in TARGET_KEYS.values() for k in v}):
        p.add_argument(f"--{k.replace('_', '-')}", dest=k)
    p.add_argument("--category-name", help="품목 표시 이름(예 로봇청소기) — 대시보드 마트 category_name")
    p.add_argument("--focus-brand")
    p.add_argument("--rival-brand")
    p.add_argument("--force", action="store_true")
    for c in ("check", "ddl", "plan", "load", "verify", "rulebook"):
        q = sub.add_parser(c)
        q.add_argument("name")
        if c == "check":
            q.add_argument("--connect", action="store_true")
        if c == "ddl":
            q.add_argument("--out")
        if c in ("plan", "load", "verify"):
            q.add_argument("--run", required=True, help="run 폴더 이름 (예 kr_vacuum-cleaner_2026-07)")
            q.add_argument("--layers", help="쉼표로 층 제한 (기본: 설정 또는 전 층)")
        if c == "load":
            q.add_argument("--dry-run", action="store_true")
    r = sub.add_parser("run")
    r.add_argument("name")
    r.add_argument("--run-date", required=True)
    r.add_argument("--phases", help="MI: seed,collect,load(쉼표) · 트렌드: collect(기본 · 01~04 수집 → 05~08) | build(05~08 · 0크레딧) | 05-08 | 06")
    sub.add_parser("selftest")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "run":
            return cmd_run(a, extra)
        return {"init": cmd_init, "rulebook": cmd_rulebook, "check": cmd_check, "ddl": cmd_ddl, "plan": cmd_plan, "load": cmd_load,
                "verify": cmd_verify, "selftest": cmd_selftest}[a.cmd](a)
    except sinks.SinkError as e:
        print(f"설정·연결 오류: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
