import argparse
import os
import logging
import sys

def main():
    parser = argparse.ArgumentParser(description="Run JSONL → CSV/Excel convert using settings.yaml defaults")
    default_config = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "settings.yaml")
    parser.add_argument("--config", default=default_config, help="Path to settings.yaml")
    parser.add_argument("--input")
    parser.add_argument("--csv")
    parser.add_argument("--excel")
    args = parser.parse_args()

    # ensure project root on sys.path for imports when running from scripts/
    proj_root = os.path.dirname(os.path.dirname(__file__))
    if proj_root not in sys.path:
        sys.path.insert(0, proj_root)

    # delayed imports after sys.path ensured
    from feishu_contracts.common.logging import setup_logger
    from feishu_contracts.common.config import load_settings
    from feishu_contracts.convert.jsonl_to_tabular import convert_jsonl

    setup_logger(os.path.join(proj_root, "logs", "run_jsonl_convert.log"))

    cfg = load_settings(args.config)
    conv = cfg.get("convert", {}) if isinstance(cfg, dict) else {}
    options = cfg.get("options", {}) if isinstance(cfg, dict) else {}

    input_path = args.input or conv.get("jsonl_input")
    csv_path = args.csv or conv.get("csv_output")
    excel_path = args.excel or conv.get("excel_output")

    missing = []
    if not input_path:
        missing.append("--input")
    if not csv_path:
        missing.append("--csv")
    if missing:
        parser.error(f"Missing required arguments: {', '.join(missing)}")

    # Ensure output directories exist
    csv_dir = os.path.dirname(csv_path)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)
    if excel_path:
        excel_dir = os.path.dirname(excel_path)
        if excel_dir:
            os.makedirs(excel_dir, exist_ok=True)

    encoding = (options.get("encoding") or "utf-8-sig") if isinstance(options, dict) else "utf-8-sig"
    logging.info(f"convert_jsonl start: input={input_path}, csv={csv_path}, excel={excel_path}, encoding={encoding}")
    result = convert_jsonl(input_path, csv_path, excel_path, encoding=encoding)
    logging.info(f"convert_jsonl done: result={result}")

    parts = [f"CSV: {result['csv']}"]
    if result.get("excel"):
        parts.append(f"Excel: {result['excel']}")
    if result.get("list_csvs"):
        for key, path in result["list_csvs"].items():
            parts.append(f"{key}: {path}")
    print(" | ".join(parts))


if __name__ == "__main__":
    main()
