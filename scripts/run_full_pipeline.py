import argparse
import os
import sys
import logging

# ensure project root on sys.path
PROJ_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJ_ROOT not in sys.path:
    sys.path.insert(0, PROJ_ROOT)

from feishu_contracts.transform.transformer import run as transform_run
from feishu_contracts.common.logging_config import init_logging
from feishu_contracts.common.config import load_settings, ensure_parent_dir
from feishu_contracts.fetch.client import run_fetch, FetchConfig as FetchCfg


def main() -> None:
    parser = argparse.ArgumentParser(description="Full pipeline: fetch → convert → transform")
    default_config = os.path.join(PROJ_ROOT, "config", "settings.yaml")
    parser.add_argument("--config", default=default_config, help="Path to settings.yaml")
    args = parser.parse_args()

    cfg = load_settings(args.config)
    init_logging((cfg.get("logging") if isinstance(cfg, dict) else {}) or {}, PROJ_ROOT)
    logger = logging.getLogger("pipeline")

    paths = cfg.get("paths", {})
    conv = cfg.get("convert", {})
    feishu = cfg.get("feishu", {})
    fetch = cfg.get("fetch", {})
    options = cfg.get("options", {})

    # validate credentials
    app_key = (feishu.get("app_key") or "").strip()
    app_secret = (feishu.get("app_secret") or "").strip()
    if not app_key or not app_secret:
        raise SystemExit("请在 config/settings.yaml 的 feishu 段配置 app_key 与 app_secret")

    # io paths
    jsonl_input = conv.get("jsonl_input") or os.path.join(PROJ_ROOT, "data", "raw", "contracts.jsonl")
    csv_output = conv.get("csv_output") or os.path.join(PROJ_ROOT, "data", "processed", "contracts.csv")
    excel_output = conv.get("excel_output") or os.path.join(PROJ_ROOT, "data", "processed", "contracts.xlsx")
    status_csv = os.path.join(PROJ_ROOT, "data", "processed", "contracts_status.csv")

    codes_file = fetch.get("contract_codes_file") or "合同编码清单.txt"
    if not os.path.isabs(codes_file):
        codes_file = os.path.join(PROJ_ROOT, codes_file)

    # ensure output directories
    for p in [jsonl_input, csv_output, excel_output, status_csv]:
        ensure_parent_dir(p)

    logger.info("[1/3] 开始抓取合同数据 …")
    fetch_cfg = FetchCfg(
        app_key=app_key,
        app_secret=app_secret,
        page_size=int(fetch.get("page_size", 50)),
        permission=int(fetch.get("permission", 0)),
        detail_workers=int(fetch.get("detail_workers", 1)),
        limit_contracts=int(fetch.get("limit_contracts", 0)),
        contract_codes_file=codes_file,
        contract_codes=[],
        output_jsonl=jsonl_input,
        status_csv=status_csv,
        final_csv=csv_output,
        output_xlsx=excel_output,
        encoding=(options.get("encoding") or "utf-8-sig") if isinstance(options, dict) else "utf-8-sig",
    )
    _fetch_result = run_fetch(fetch_cfg)
    logger.info("[1/3] 抓取完成")

    # choose transform inputs
    source_excel = paths.get("source") or excel_output
    template = paths.get("template") or os.path.join(PROJ_ROOT, "templates", "合同导入模板.xlsx")
    mapping = paths.get("mapping") or os.path.join(PROJ_ROOT, "feishu_contracts", "transform", "mapping.yaml")
    out_path = paths.get("out")
    if not out_path and paths.get("output_dir"):
        out_path = os.path.join(paths.get("output_dir"), "合同导入模板_填充.xlsx")
    if not out_path:
        out_path = os.path.join(PROJ_ROOT, "output", "合同导入模板_填充.xlsx")
    if not os.path.isabs(out_path):
        out_path = os.path.join(PROJ_ROOT, out_path)
    ensure_parent_dir(out_path)

    logger.info("[2/3] 开始模板填充 …")
    transform_run(source_excel, template, mapping, out_path)
    logger.info("[2/3] 模板填充完成")

    print("完成：")
    print(f"  JSONL: {jsonl_input}")
    print(f"  详情CSV: {csv_output}")
    print(f"  详情Excel: {excel_output}")
    print(f"  状态清单: {status_csv}")
    print(f"  导入模板: {out_path}")


if __name__ == "__main__":
    main()
