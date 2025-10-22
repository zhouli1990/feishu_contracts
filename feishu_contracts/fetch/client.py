from __future__ import annotations
import os
import json
import time
import random
import threading
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from feishu_contracts.convert.jsonl_to_tabular import convert_jsonl
from feishu_contracts.common.logging_config import get_progress_interval

logger = logging.getLogger("fetch")


@dataclass
class FetchConfig:
    app_key: str
    app_secret: str
    page_size: int = 50
    permission: int = 0
    detail_workers: int = 1
    rate_limit_qps: int = 1
    limit_contracts: int = 0
    contract_codes_file: Optional[str] = None
    contract_codes: List[str] = None  # type: ignore[assignment]
    output_jsonl: str = "contracts.jsonl"
    status_csv: str = "contracts_status.csv"
    final_csv: str = "contracts_details.csv"
    output_xlsx: Optional[str] = None

    def __post_init__(self) -> None:
        if self.contract_codes is None:
            self.contract_codes = []


class RateLimiter:
    def __init__(self, max_calls: int, period: float = 1.0):
        self.max_calls = max_calls
        self.period = period
        self._times = deque()
        self._lock = threading.Lock()
        self._cooldown_until: float = 0.0

    def acquire(self) -> None:
        while True:
            sleep_time = 0.0
            with self._lock:
                now = time.time()
                if now < self._cooldown_until:
                    sleep_time = self._cooldown_until - now
                else:
                    while self._times and (now - self._times[0]) >= self.period:
                        self._times.popleft()
                    if len(self._times) < self.max_calls:
                        self._times.append(now)
                        return
                    sleep_time = self.period - (now - self._times[0])
            if sleep_time > 0:
                time.sleep(min(sleep_time, 0.2))
            else:
                time.sleep(0.001)

    def set_cooldown(self, seconds: float) -> None:
        with self._lock:
            now = time.time()
            self._cooldown_until = max(self._cooldown_until, now + max(0.0, seconds))


class FeishuTokenManager:
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = "https://open.feishu.cn/open-apis"

    def get_access_token(self) -> str:
        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        data = {"app_id": self.app_id, "app_secret": self.app_secret}
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get("code") != 0:
            raise RuntimeError(f"get_access_token failed: {result.get('msg')}")
        access_token = result.get("tenant_access_token")
        if not access_token:
            raise RuntimeError("missing tenant_access_token in response")
        return access_token


class FeishuContractClient:
    def __init__(self, access_token: str, rate_limiter: Optional[RateLimiter] = None):
        self.base_url = "https://open.feishu.cn/open-apis"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        self.rate_limiter = rate_limiter

    def search_contracts(self, page_size: int = 50, permission: Optional[int] = 0, max_items: Optional[int] = None) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/contract/v1/contracts/search"
        page_token: str = ""
        items: List[Dict[str, Any]] = []
        pages = 0
        while True:
            body: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                body["page_token"] = page_token
            if permission is not None:
                body["combine_condition"] = {"permission": permission}

            def _fmt_token(t: Optional[str]) -> str:
                if not t:
                    return ""
                s = str(t)
                return f"{s[:12]}...({len(s)})"

            _t0 = time.time()
            page_attempts = 0
            while True:
                try:
                    try:
                        logger.debug("search page start token=%s", _fmt_token(page_token))
                    except Exception:
                        pass
                    if self.rate_limiter is not None:
                        self.rate_limiter.acquire()
                    resp = requests.post(url, headers=self.headers, json=body, timeout=60)
                    resp.raise_for_status()
                    break
                except requests.HTTPError as e:
                    resp_obj = getattr(e, "response", None)
                    rstatus = resp_obj.status_code if resp_obj is not None else None
                    code_val = None
                    if resp_obj is not None:
                        try:
                            rj = resp_obj.json()
                            code_val = rj.get("code")
                        except Exception:
                            pass
                    if (code_val in (9499, 99991400) or rstatus == 429) and page_attempts < 3:
                        page_attempts += 1
                        base = min(2 ** page_attempts, 8.0)
                        wait = max(0.5, base + base * random.uniform(-0.2, 0.2))
                        try:
                            logger.warning(
                                "rate_limited during search code=%s status=%s wait=%.1fs attempt=%s",
                                str(code_val), str(rstatus), wait, page_attempts,
                            )
                        except Exception:
                            pass
                        if self.rate_limiter is not None:
                            self.rate_limiter.set_cooldown(wait)
                        time.sleep(wait)
                        continue
                    raise

            result = resp.json()
            if result.get("code") != 0:
                raise RuntimeError(f"search failed: {result.get('msg')}")
            data = result.get("data", {})
            page_items = data.get("items", []) or []

            has_more = bool(data.get("has_more", False) or data.get("hasMore", False))
            next_token_raw = data.get("page_token") or data.get("pageToken") or ""
            try:
                logger.debug(
                    "search page done items=%d has_more=%s next_token=%s",
                    len(page_items), str(has_more), _fmt_token(next_token_raw),
                )
            except Exception:
                pass

            items.extend(page_items)
            if max_items is not None and max_items > 0 and len(items) >= max_items:
                items = items[:max_items]
                break

            page_token = next_token_raw
            pages += 1
            if not has_more:
                break
            time.sleep(0.1)
        return items

    def search_contracts_by_number(self, contract_number: str, page_size: int = 50) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/contract/v1/contracts/search"
        page_token: str = ""
        items: List[Dict[str, Any]] = []
        pages = 0
        while True:
            body: Dict[str, Any] = {"page_size": page_size, "contract_number": contract_number}
            if page_token:
                body["page_token"] = page_token
            _t0 = time.time()
            attempts = 0
            while True:
                try:
                    if self.rate_limiter is not None:
                        self.rate_limiter.acquire()
                    resp = requests.post(url, headers=self.headers, json=body, timeout=60)
                    resp.raise_for_status()
                    break
                except requests.HTTPError as e:
                    resp_obj = getattr(e, "response", None)
                    rstatus = resp_obj.status_code if resp_obj is not None else None
                    code_val = None
                    if resp_obj is not None:
                        try:
                            rj = resp_obj.json()
                            code_val = rj.get("code")
                        except Exception:
                            pass
                    if (code_val in (9499, 99991400) or rstatus == 429) and attempts < 3:
                        attempts += 1
                        base = min(2 ** attempts, 8.0)
                        wait = max(0.5, base + base * random.uniform(-0.2, 0.2))
                        try:
                            logger.warning(
                                "rate_limited during search_by_number code=%s status=%s wait=%.1fs attempt=%s",
                                str(code_val), str(rstatus), wait, attempts,
                                extra={"contract_code": str(contract_number), "attempt": attempts},
                            )
                        except Exception:
                            pass
                        if self.rate_limiter is not None:
                            self.rate_limiter.set_cooldown(wait)
                        time.sleep(wait)
                        continue
                    # graceful degrade: return items collected so far
                    return items
            result = resp.json()
            if result.get("code") != 0:
                raise RuntimeError(f"search by number failed: {result.get('msg')}")
            data = result.get("data", {})
            page_items = data.get("items", []) or []
            has_more = bool(data.get("has_more", False) or data.get("hasMore", False))
            next_token_raw = data.get("page_token") or data.get("pageToken") or ""
            items.extend(page_items)
            page_token = next_token_raw
            pages += 1
            if not has_more:
                break
            time.sleep(0.1)
        return items

    def get_contract_detail(self, contract_id: str, user_id_type: Optional[str] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/contract/v1/contracts/{contract_id}"
        params: Dict[str, Any] = {}
        if user_id_type:
            params["user_id_type"] = user_id_type
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()
        resp = requests.get(url, headers={"Authorization": self.headers["Authorization"]}, params=params, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"detail failed for {contract_id}: {result.get('msg')}")
        data = result.get("data", {})
        if isinstance(data, dict) and "contract" in data:
            detail = data.get("contract")
        else:
            detail = data
        if isinstance(detail, dict):
            if "contract_id" not in detail:
                detail["contract_id"] = contract_id
        return detail

    def fetch_all_details(self, contract_ids: List[str], workers: int = 8) -> List[Dict[str, Any]]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results: List[Dict[str, Any]] = []

        def _task(cid: str) -> Dict[str, Any]:
            last_err: Optional[Exception] = None
            for i in range(5):
                try:
                    try:
                        logger.debug("detail start try=%d", i + 1, extra={"contract_id": cid})
                    except Exception:
                        pass
                    res = self.get_contract_detail(cid)
                    try:
                        logger.debug("detail done", extra={"contract_id": cid})
                    except Exception:
                        pass
                    return res
                except Exception as e:
                    last_err = e
                    try:
                        logger.error("detail error %s", str(e), extra={"contract_id": cid})
                    except Exception:
                        pass
                    time.sleep(min(1 * (i + 1), 5))
            return {"contract_id": cid, "_error": str(last_err) if last_err else "unknown error"}

        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            fut_map = {ex.submit(_task, str(cid)): str(cid) for cid in contract_ids}
            for fut in as_completed(fut_map):
                cid = fut_map[fut]
                try:
                    res = fut.result()
                except Exception as e:  # pragma: no cover
                    res = {"contract_id": cid, "_error": str(e)}
                results.append(res)
        return results


def _append_jsonl_line(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _write_status_csv(path: str, rows: List[Dict[str, Any]], append: bool = False) -> None:
    import csv
    fieldnames = ["contract_code", "contract_id", "status", "error", "attempt"]
    file_exists = os.path.exists(path)
    mode = "a" if append else "w"
    with open(path, mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        write_header = not append or not file_exists or os.path.getsize(path) == 0
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _load_contract_codes(contract_codes_file: Optional[str], inline_codes: List[str]) -> List[str]:
    codes: List[str] = []
    if contract_codes_file:
        try:
            with open(contract_codes_file, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s:
                        codes.append(s)
        except Exception:
            pass
    for c in inline_codes or []:
        s = str(c).strip()
        if s:
            codes.append(s)
    seen = set()
    deduped: List[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped


def _process_codes_once(
    codes: List[str],
    client: FeishuContractClient,
    page_size: int,
    jsonl_path: str,
    seen_ids: set,
    attempt: int,
    limit_contracts: int = 0,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    status_rows: List[Dict[str, Any]] = []
    failed_codes: set = set()
    count_written = 0
    success_count = 0
    fail_count = 0
    start_t = time.time()
    progress_interval = max(1, int(get_progress_interval(100)))
    for idx, code in enumerate(codes):
        try:
            logger.debug(
                "search_by_number start page_size=%d",
                page_size,
                extra={"contract_code": str(code)},
            )
        except Exception:
            pass
        try:
            items = client.search_contracts_by_number(contract_number=str(code), page_size=page_size)
        except Exception as e:
            status_rows.append({
                "contract_code": code,
                "contract_id": "",
                "status": "fail",
                "error": str(e),
                "attempt": attempt,
            })
            failed_codes.add(code)
            fail_count += 1
            processed = idx + 1
            if processed % progress_interval == 0 or processed == len(codes):
                elapsed = max(time.time() - start_t, 1e-6)
                rps = processed / elapsed
                remaining = max(len(codes) - processed, 0)
                eta_s = (remaining / rps) if rps > 0 else 0.0
                logger.info(
                    "progress %d/%d ok=%d fail=%d rps=%.1f eta=%.0fs",
                    processed, len(codes), success_count, fail_count, rps, eta_s,
                    extra={"is_progress": True, "contract_code": str(code)},
                )
            continue
        try:
            logger.debug(
                "search_by_number done items=%d",
                len(items),
                extra={"contract_code": str(code)},
            )
        except Exception:
            pass
        if not items:
            status_rows.append({
                "contract_code": code,
                "contract_id": "",
                "status": "fail",
                "error": "not_found",
                "attempt": attempt,
            })
            failed_codes.add(code)
            fail_count += 1
            processed = idx + 1
            if processed % progress_interval == 0 or processed == len(codes):
                elapsed = max(time.time() - start_t, 1e-6)
                rps = processed / elapsed
                remaining = max(len(codes) - processed, 0)
                eta_s = (remaining / rps) if rps > 0 else 0.0
                logger.info(
                    "progress %d/%d ok=%d fail=%d rps=%.1f eta=%.0fs",
                    processed, len(codes), success_count, fail_count, rps, eta_s,
                    extra={"is_progress": True, "contract_code": str(code)},
                )
            continue
        code_any_success = False
        for it in items:
            cid = str(it.get("contract_id") or "").strip()
            if not cid:
                status_rows.append({
                    "contract_code": code,
                    "contract_id": "",
                    "status": "fail",
                    "error": "missing_contract_id",
                    "attempt": attempt,
                })
                continue
            try:
                detail_list = client.fetch_all_details([cid], workers=1)
                detail = detail_list[0] if detail_list else {}
                if isinstance(detail, dict) and detail.get("_error"):
                    raise RuntimeError(str(detail.get("_error")))
                if isinstance(detail, dict) and cid not in seen_ids:
                    rec = dict(detail)
                    rec.setdefault("contract_id", cid)
                    rec.setdefault("contract_code", code)
                    _append_jsonl_line(jsonl_path, rec)
                    seen_ids.add(cid)
                    count_written += 1
                    code_any_success = True
                status_rows.append({
                    "contract_code": code,
                    "contract_id": cid,
                    "status": "success",
                    "error": "",
                    "attempt": attempt,
                })
            except Exception as e:
                status_rows.append({
                    "contract_code": code,
                    "contract_id": cid,
                    "status": "fail",
                    "error": str(e),
                    "attempt": attempt,
                })
        if not code_any_success:
            failed_codes.add(code)
            fail_count += 1
        else:
            success_count += 1

        processed = idx + 1
        if processed % progress_interval == 0 or processed == len(codes):
            elapsed = max(time.time() - start_t, 1e-6)
            rps = processed / elapsed
            remaining = max(len(codes) - processed, 0)
            eta_s = (remaining / rps) if rps > 0 else 0.0
            logger.info(
                "progress %d/%d ok=%d fail=%d rps=%.1f eta=%.0fs",
                processed, len(codes), success_count, fail_count, rps, eta_s,
                extra={"is_progress": True, "contract_code": str(code)},
            )

        if limit_contracts > 0 and count_written >= limit_contracts:
            break
    return status_rows, sorted(failed_codes)


def run_fetch(cfg: FetchConfig) -> Dict[str, Any]:
    # ensure parent dirs
    for p in [cfg.output_jsonl, cfg.status_csv, cfg.final_csv] + ([cfg.output_xlsx] if cfg.output_xlsx else []):
        if not p:
            continue
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)

    token_manager = FeishuTokenManager(cfg.app_key, cfg.app_secret)
    access_token = token_manager.get_access_token()
    rate_limiter = RateLimiter(cfg.rate_limit_qps, 1.0)
    client = FeishuContractClient(access_token, rate_limiter=rate_limiter)

    codes = _load_contract_codes(cfg.contract_codes_file, cfg.contract_codes)
    if not codes:
        return {
            "jsonl": cfg.output_jsonl,
            "status_csv": cfg.status_csv,
            "csv": cfg.final_csv,
            "excel": cfg.output_xlsx,
            "list_csvs": {},
        }

    # reset outputs
    for p in [cfg.output_jsonl, cfg.status_csv]:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    seen_ids: set = set()
    current_codes: List[str] = list(codes)
    max_retry_rounds = 3
    total_passes = max_retry_rounds + 1
    for i in range(total_passes):
        attempt = i + 1
        status_rows, failed_codes = _process_codes_once(
            current_codes,
            client,
            cfg.page_size,
            cfg.output_jsonl,
            seen_ids,
            attempt,
            limit_contracts=max(0, int(cfg.limit_contracts or 0)),
        )
        try:
            ok_cnt = sum(1 for r in status_rows if str(r.get("status")) == "success")
            fail_cnt = sum(1 for r in status_rows if str(r.get("status")) != "success")
            logger.info(
                "round_summary pass=%d ok=%d fail=%d remain=%d",
                attempt, ok_cnt, fail_cnt, len(failed_codes),
            )
        except Exception:
            pass
        _write_status_csv(cfg.status_csv, status_rows, append=(attempt > 1))
        if not failed_codes:
            break
        if attempt >= total_passes:
            break
        current_codes = failed_codes

    # convert jsonl → csv/excel
    convert_logger = logging.getLogger("convert")
    try:
        convert_logger.info(
            "start convert jsonl→csv/xlsx jsonl=%s csv=%s xlsx=%s",
            cfg.output_jsonl, cfg.final_csv, cfg.output_xlsx,
        )
    except Exception:
        pass
    convert_result = convert_jsonl(cfg.output_jsonl, cfg.final_csv, cfg.output_xlsx)
    try:
        convert_logger.info(
            "convert done csv=%s xlsx=%s",
            convert_result.get("csv"), convert_result.get("excel"),
        )
    except Exception:
        pass
    return {
        "jsonl": cfg.output_jsonl,
        "status_csv": cfg.status_csv,
        "csv": convert_result.get("csv"),
        "excel": convert_result.get("excel"),
        "list_csvs": convert_result.get("list_csvs") or {},
    }
