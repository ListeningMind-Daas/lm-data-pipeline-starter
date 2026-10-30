#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lm_api.py — ListeningMind DaaS Data API 클라이언트.

라이브 스펙: https://listeningmind-data-api.ascentlab.io/openapi.json (2026-10-30 확인 v1.0.15)
인증: LM-API-KEY 헤더 (환경변수 LM_API_KEY). 서버: config.LM_API_BASE.

API Key 직접 호출 — 수집 규약을 코드로 강제:
- 응답 전문 무변형 저장: bronze/raw/call_{seq:04d}_{api}_{tag}.json.gz
- 원장 1콜 1파일: bronze/ledger/call_{seq:04d}.json (cost_detail 실측)
- 자가검증: 요청 키워드 집합 == 응답 request_detail 키워드 집합
- 시퀀스: 기존 원장 최대 seq+1부터 append (중복 방지)

stdlib(urllib)만 사용 — 추가 의존성 없음.

CLI:
    python3 lm_api.py --check                      # 인증·연결 확인 (3kw 샘플)
    python3 lm_api.py keyword_info --file kws.txt --data-type all --tag l3b001
    python3 lm_api.py intent_finder --seed 패딩 --limit 150
"""
import os, sys, json, gzip, time, re, argparse
import urllib.request, urllib.error
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config


class LMApiError(RuntimeError):
    pass


class LMClient:
    def __init__(self, api_key=None, gl=None, base=None, run=None):
        self.api_key = api_key or config.LM_API_KEY
        self.gl = gl or config.GL
        self.base = (base or config.LM_API_BASE).rstrip("/")
        self.run = Path(run) if run else config.RUN
        self.raw = self.run / "bronze" / "raw"
        self.ledger = self.run / "bronze" / "ledger"
        self.raw.mkdir(parents=True, exist_ok=True)
        self.ledger.mkdir(parents=True, exist_ok=True)
        if not self.api_key:
            raise LMApiError("LM_API_KEY 미설정 — export LM_API_KEY=<키>")

    # ── 저수준 POST (실패/FAILED 자동 재시도 — 수집 규약 §재시도) ──
    def _post(self, path, body):
        url = f"{self.base}{path}"
        data = json.dumps(body).encode("utf-8")
        RETRYABLE = {408, 429, 500, 502, 503, 504}
        last = None
        for attempt in range(config.LM_API_RETRIES + 1):
            req = urllib.request.Request(url, data=data, method="POST", headers={
                "Content-Type": "application/json", "LM-API-KEY": self.api_key,
            })
            try:
                with urllib.request.urlopen(req, timeout=config.LM_TIMEOUT) as r:
                    resp = json.loads(r.read().decode("utf-8"))
                # 응답은 왔으나 result=FAILED → 재시도 여지 있으면 재시도, 아니면 그대로 반환(원장에 error 기록)
                if resp.get("result") == "FAILED" and attempt < config.LM_API_RETRIES:
                    last = LMApiError(f"result=FAILED: {resp.get('reason')}")
                    print(f"    ↻ 재시도 {attempt+1}/{config.LM_API_RETRIES} (FAILED: {resp.get('reason')})")
                    time.sleep(config.LM_API_BACKOFF); continue
                return resp
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")[:300]
                if e.code in RETRYABLE and attempt < config.LM_API_RETRIES:
                    last = LMApiError(f"HTTP {e.code}: {detail}")
                    print(f"    ↻ 재시도 {attempt+1}/{config.LM_API_RETRIES} (HTTP {e.code})")
                    time.sleep(config.LM_API_BACKOFF); continue
                raise LMApiError(f"HTTP {e.code} {path}: {detail}")
            except urllib.error.URLError as e:
                if attempt < config.LM_API_RETRIES:
                    last = LMApiError(f"연결 실패: {e.reason}")
                    print(f"    ↻ 재시도 {attempt+1}/{config.LM_API_RETRIES} ({e.reason})")
                    time.sleep(config.LM_API_BACKOFF); continue
                raise LMApiError(f"연결 실패 {path}: {e.reason}")
        if last:
            raise last

    # ── 시퀀스 관리 ──────────────────────────────────────────
    def _next_seq(self):
        mx = 0
        for f in self.ledger.glob("call_*.json"):
            m = re.search(r"call_0*(\d+)\.json", f.name)
            if m:
                mx = max(mx, int(m.group(1)))
        return mx + 1

    # ── 저장 + 원장 (수집 규약) ────────────────────
    def _persist(self, seq, api, tag, phase, purpose, params, resp, requested_kws=None):
        raw_name = f"call_{seq:04d}_{api}_{tag}.json.gz"
        with gzip.open(self.raw / raw_name, "wt", encoding="utf-8") as f:
            json.dump(resp, f, ensure_ascii=False)

        cd = resp.get("cost_detail") or {}
        data = resp.get("data")
        ledger = {
            "call_seq": seq, "run_id": config.RUN_ID, "gl": self.gl,
            "phase": phase, "api": api, "purpose": purpose, "params": params,
            "input_cost": cd.get("input_cost", 0), "input_count": cd.get("input_count", 0),
            "output_cost": cd.get("output_cost", 0), "output_count": cd.get("output_count", 0),
            "total_cost": cd.get("total_cost", 0),
            "returned": len(data) if isinstance(data, list) else 0,
            "request_at": resp.get("request_at", ""),
            "used_credits_after": resp.get("used_credits", 0),
            "raw_file": f"bronze/raw/{raw_name}",
        }
        # 자가검증 (keyword_info): 요청 == 응답 request_detail 키워드 집합
        if requested_kws is not None:
            got = set((resp.get("request_detail") or {}).get("keywords") or [])
            missing = sorted(set(requested_kws) - got)
            if missing:
                ledger["missing"] = missing
        if resp.get("result") not in ("OK", "SUCCESS", None):
            ledger["error"] = resp.get("reason", "")
        with open(self.ledger / f"call_{seq:04d}.json", "w", encoding="utf-8") as f:
            json.dump(ledger, f, ensure_ascii=False, indent=1)
        return ledger

    # ── keyword_info (누락 키워드 1회 자동 재수집 — 수집 규약 §재시도) ──
    def keyword_info(self, keywords, data_type="all", tag="kw", phase="collection",
                     purpose="", retry_missing=True):
        keywords = [k.strip() for k in keywords if k.strip()]
        if not (1 <= len(keywords) <= config.KEYWORD_INFO_BATCH):
            raise LMApiError(f"keyword_info 1~{config.KEYWORD_INFO_BATCH}개 (요청 {len(keywords)})")
        resp = self._post("/keyword_info", {"keywords": keywords, "gl": self.gl, "data_type": data_type})
        seq = self._next_seq()
        led = self._persist(seq, "keyword_info", tag, phase,
                            purpose or f"keyword_info({data_type}) {len(keywords)}kw",
                            f"data_type={data_type},{len(keywords)}kw", resp, requested_kws=keywords)

        # 누락분 1회 자동 재수집
        if retry_missing and led.get("missing") and 1 <= len(led["missing"]) <= config.KEYWORD_INFO_BATCH:
            miss = led["missing"]
            print(f"    ↻ 누락 {len(miss)}개 재수집…")
            resp2 = self._post("/keyword_info", {"keywords": miss, "gl": self.gl, "data_type": data_type})
            seq2 = self._next_seq()
            led2 = self._persist(seq2, "keyword_info", f"{tag}_retry", phase,
                                 f"누락 재수집({data_type}) {len(miss)}kw",
                                 f"data_type={data_type},retry,{len(miss)}kw", resp2, requested_kws=miss)
            still = led2.get("missing", [])
            led["retry_seq"] = seq2
            led["missing_after_retry"] = still
            led["missing"] = still           # 최종 미반환만 남김 (재수집 성공분 제거)
            with open(self.ledger / f"call_{seq:04d}.json", "w", encoding="utf-8") as f:
                json.dump(led, f, ensure_ascii=False, indent=1)
            if still:
                print(f"    ⚠ 재수집 후에도 미반환 {len(still)}개 (원장 기록)")
        return resp, led

    # ── intent_finder ────────────────────────────────────────
    def intent_finder(self, seeds, volume_threshold=None, limit=None, sort="volume_avg",
                      order="desc", tag=None, phase="L1_candidate_expansion", purpose=""):
        seeds = [s.strip() for s in ([seeds] if isinstance(seeds, str) else seeds) if s.strip()]
        if not (1 <= len(seeds) <= config.INTENT_MAX_SEEDS):
            raise LMApiError(f"intent_finder 1~{config.INTENT_MAX_SEEDS} seed (요청 {len(seeds)})")
        body = {"keywords": seeds, "gl": self.gl,
                "volume_threshold": volume_threshold if volume_threshold is not None else config.L1_VOLUME_THRESHOLD,
                "limit": limit if limit is not None else config.L1_LIMIT_PER_NODE,
                "sort": sort, "order": order}
        resp = self._post("/intent_finder/keyword_list", body)
        seq = self._next_seq()
        led = self._persist(seq, "intent_finder", tag or config.norm(seeds[0]), phase,
                            purpose or f"intent_finder {len(seeds)}seed", f"limit={body['limit']},{len(seeds)}seed", resp)
        return resp, led


# ── CLI ──────────────────────────────────────────────────────
def _main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    ap.add_argument("--check", action="store_true", help="인증·연결 확인 (3kw 샘플)")

    ki = sub.add_parser("keyword_info")
    ki.add_argument("--file"); ki.add_argument("--kw", nargs="*")
    ki.add_argument("--data-type", default="all"); ki.add_argument("--tag", default="kw")
    ki.add_argument("--phase", default="collection")

    itf = sub.add_parser("intent_finder")
    itf.add_argument("--seed", required=True); itf.add_argument("--limit", type=int, default=None)

    args = ap.parse_args()
    if not args.check and not args.cmd:
        ap.print_help()
        return
    try:
        client = LMClient()
    except LMApiError as e:
        print(f"❌ {e}")
        sys.exit(2)

    if args.check:
        resp, led = client.keyword_info(["하객룩", "패딩", "청바지"], data_type="ads_metrics",
                                        tag="apicheck", phase="setup_sanity", purpose="API Key 연결 검증(3kw)")
        print(f"✓ 연결 OK — result={resp.get('result')} version={resp.get('version')} "
              f"returned={led['returned']} cost={led['total_cost']} used_credits={led['used_credits_after']}")
        return

    if args.cmd == "keyword_info":
        kws = args.kw or [l.strip() for l in open(args.file, encoding="utf-8") if l.strip()]
        resp, led = client.keyword_info(kws, data_type=args.data_type, tag=args.tag, phase=args.phase)
        print(f"✓ keyword_info seq={led['call_seq']} returned={led['returned']} cost={led['total_cost']}"
              + (f" ⚠ missing={len(led['missing'])}" if led.get("missing") else ""))
    elif args.cmd == "intent_finder":
        resp, led = client.intent_finder(args.seed, limit=args.limit)
        print(f"✓ intent_finder seq={led['call_seq']} returned={led['returned']} cost={led['total_cost']}")
    else:
        ap.print_help()


if __name__ == "__main__":
    _main()
