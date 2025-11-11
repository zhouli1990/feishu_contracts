from __future__ import annotations
import csv
import json
import os
import logging
from typing import Any, Dict, List, Optional, Tuple
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

logger = logging.getLogger("convert")


def _to_json_cell_value(v: Any) -> Any:
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, str):
        s = v.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, (list, dict)):
                    return json.dumps(parsed, ensure_ascii=False)
            except Exception:
                pass
    return v


def _sanitize_excel_text(s: str) -> str:
    try:
        return ILLEGAL_CHARACTERS_RE.sub(" ", s)
    except Exception:
        return s


def _to_excel_cell_value(v: Any) -> Any:
    v2 = _to_json_cell_value(v)
    if isinstance(v2, str):
        return _sanitize_excel_text(v2)
    return v2


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        records.append(obj)
                except Exception:
                    continue
    except FileNotFoundError:
        records = []
    return records


def _split_keys(records: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    all_keys = {k for r in records for k in r.keys()}
    list_keys: set = set()
    for k in all_keys:
        if k.lower().endswith("list"):
            list_keys.add(k)
            continue
        for r in records:
            v = r.get(k)
            if isinstance(v, list):
                list_keys.add(k)
                break
            if isinstance(v, str):
                s = v.strip()
                if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                    try:
                        parsed = json.loads(s)
                        if isinstance(parsed, list):
                            list_keys.add(k)
                            break
                    except Exception:
                        pass
    base_keys = sorted([k for k in all_keys if k not in list_keys])
    return base_keys, sorted(list_keys)


def _write_main_csv(records: List[Dict[str, Any]], keys: List[str], csv_path: str, encoding: str) -> None:
    with open(csv_path, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in records:
            out: Dict[str, Any] = {}
            for key in keys:
                if key in row:
                    out[key] = _to_json_cell_value(row.get(key))
                else:
                    out[key] = ""
            writer.writerow(out)


def _sanitize_sheet_name(name: str, used: Dict[str, int]) -> str:
    invalid = set("[]:*?/\\")
    cleaned = "".join(ch for ch in name if ch not in invalid)
    if not cleaned:
        cleaned = "sheet"
    cleaned = cleaned[:31]
    if cleaned not in used:
        used[cleaned] = 0
        return cleaned
    used[cleaned] += 1
    suffix = used[cleaned]
    base = cleaned[: max(0, 31 - len(str(suffix)) - 1)]
    final = f"{base}_{suffix}"
    while final in used:
        suffix += 1
        used[cleaned] = suffix
        base = cleaned[: max(0, 31 - len(str(suffix)) - 1)]
        final = f"{base}_{suffix}"
    used[final] = 0
    return final


def _build_list_tables(records: List[Dict[str, Any]], list_keys: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    tables: Dict[str, List[Dict[str, Any]]] = {k: [] for k in list_keys}
    for rec in records:
        contract_ref = (
            rec.get("contract_number")
            or rec.get("contract_code")
            or rec.get("contract_id")
            or ""
        )
        for key in list_keys:
            value = rec.get(key)
            if value is None:
                continue
            if isinstance(value, str):
                s = value.strip()
                if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                    try:
                        value = json.loads(s)
                    except Exception:
                        pass
            items = value if isinstance(value, list) else [value]
            for item in items:
                row: Dict[str, Any] = {"contract_number": contract_ref}
                if isinstance(item, dict):
                    for subk, subv in item.items():
                        row[subk] = _to_json_cell_value(subv)
                else:
                    row["value"] = _to_json_cell_value(item)
                tables[key].append(row)
    return tables


def _build_relation_contracts_contracts_table(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for rec in records:
        contract_ref = (
            rec.get("contract_number")
            or rec.get("contract_code")
            or rec.get("contract_id")
            or ""
        )
        relation_val = rec.get("relation")
        if relation_val is None:
            continue
        if isinstance(relation_val, str):
            s = relation_val.strip()
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    relation_val = json.loads(s)
                except Exception:
                    pass
        if not isinstance(relation_val, dict):
            continue
        rc_list = relation_val.get("relation_contracts")
        if rc_list is None:
            continue
        if isinstance(rc_list, str):
            s = rc_list.strip()
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                try:
                    rc_list = json.loads(s)
                except Exception:
                    pass
        rc_items = rc_list if isinstance(rc_list, list) else [rc_list]
        for rc in rc_items:
            if not isinstance(rc, dict):
                continue
            contracts = rc.get("contracts")
            if contracts is None:
                continue
            if isinstance(contracts, str):
                s = contracts.strip()
                if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                    try:
                        contracts = json.loads(s)
                    except Exception:
                        pass
            items = contracts if isinstance(contracts, list) else [contracts]
            for item in items:
                row: Dict[str, Any] = {"contract_number": contract_ref}
                row["relation_key"] = rc.get("relation_key", "")
                row["relation_name"] = rc.get("relation_name", "")
                row["contract_ids"] = _to_json_cell_value(rc.get("contract_ids"))
                if isinstance(item, dict):
                    for subk, subv in item.items():
                        row[f"related_{subk}"] = _to_json_cell_value(subv)
                else:
                    row["related_value"] = _to_json_cell_value(item)
                rows.append(row)
    return rows


def _write_list_csv(path: str, rows: List[Dict[str, Any]], encoding: str) -> None:
    fieldnames = (
        sorted({k for r in rows for k in r.keys()}) if rows else ["contract_number"]
    )
    with open(path, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out: Dict[str, Any] = {}
            for key in fieldnames:
                out[key] = row.get(key, "")
            writer.writerow(out)


def convert_jsonl(jsonl_path: str, csv_path: str, excel_path: Optional[str] = None, encoding: str = "utf-8-sig") -> Dict[str, Any]:
    records = _read_jsonl(jsonl_path)
    try:
        logger.info(
            "start jsonl→csv/xlsx jsonl=%s csv=%s xlsx=%s records=%d",
            jsonl_path, csv_path, excel_path, len(records),
            extra={"is_progress": True},
        )
    except Exception:
        pass
    if not records:
        with open(csv_path, "w", newline="", encoding=encoding) as f:
            writer = csv.writer(f)
            writer.writerow([])
        result: Dict[str, Any] = {"csv": csv_path, "excel": None, "list_csvs": {}}
        if excel_path and pd is not None:
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                (pd.DataFrame()).to_excel(writer, index=False, sheet_name="details")
            result["excel"] = excel_path
        elif excel_path:
            base = os.path.splitext(excel_path)[0]
            result["list_csvs"] = {}
            _write_list_csv(f"{base}_details.csv", [], encoding)
            result["list_csvs"]["details"] = f"{base}_details.csv"
        try:
            logger.info("done empty jsonl csv=%s xlsx=%s", result.get("csv"), result.get("excel"), extra={"is_progress": True})
        except Exception:
            pass
        return result

    base_keys, list_keys = _split_keys(records)
    try:
        logger.debug("split keys base=%d list=%d", len(base_keys), len(list_keys))
    except Exception:
        pass
    _write_main_csv(records, base_keys, csv_path, encoding)
    try:
        logger.info("write main csv rows=%d path=%s", len(records), csv_path)
    except Exception:
        pass
    list_tables = _build_list_tables(records, list_keys)

    special_rel_key = "relation_contracts_contracts"
    special_rel_rows = _build_relation_contracts_contracts_table(records)
    if special_rel_rows:
        list_tables[special_rel_key] = special_rel_rows
        if special_rel_key not in list_keys:
            list_keys = list(list_keys) + [special_rel_key]

    result: Dict[str, Any] = {"csv": csv_path, "excel": None, "list_csvs": {}}
    if excel_path:
        if pd is not None:
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                pd.DataFrame(
                    [{k: rec.get(k, "") for k in base_keys} for rec in records]
                ).applymap(_to_excel_cell_value).to_excel(
                    writer, index=False, sheet_name="details"
                )
                try:
                    logger.info("write sheet name=%s rows=%d", "details", len(records))
                except Exception:
                    pass
                used: Dict[str, int] = {}
                for key in list_keys:
                    rows = list_tables.get(key, [])
                    sheet_name = _sanitize_sheet_name(key, used)
                    if rows:
                        df = pd.DataFrame(rows)
                    else:
                        df = pd.DataFrame(columns=["contract_number"])
                    df = df.applymap(_to_excel_cell_value)
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
                    try:
                        logger.info("write sheet name=%s rows=%d", sheet_name, len(rows))
                    except Exception:
                        pass
            result["excel"] = excel_path
        else:
            base = os.path.splitext(excel_path)[0]
            for key in list_keys:
                rows = list_tables.get(key, [])
                path = f"{base}_{key}.csv"
                _write_list_csv(path, rows, encoding)
                result["list_csvs"][key] = path
                try:
                    logger.info("write list csv name=%s rows=%d path=%s", key, len(rows), path)
                except Exception:
                    pass
    try:
        logger.info("done csv=%s xlsx=%s list_sheets=%d", result.get("csv"), result.get("excel"), len(result.get("list_csvs") or list_keys), extra={"is_progress": True})
    except Exception:
        pass
    return result
