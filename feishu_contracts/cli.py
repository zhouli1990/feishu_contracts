from __future__ import annotations
import argparse
import os
from feishu_contracts.transform.transformer import run
from feishu_contracts.common.config import load_settings


def main():
    p = argparse.ArgumentParser(description="Feishu contracts → template transformer")
    default_mapping = os.path.join(os.path.dirname(__file__), "transform", "mapping.yaml")
    default_config = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "settings.yaml")
    p.add_argument("--config", default=default_config, help="Path to settings.yaml")
    p.add_argument("--source", help="Path to contracts.xlsx")
    p.add_argument("--template", help="Path to 模板.xlsx")
    p.add_argument("--mapping", default=default_mapping)
    p.add_argument("--out", help="Output xlsx path")
    args = p.parse_args()

    # load defaults from settings.yaml if present
    cfg = load_settings(args.config)
    paths = cfg.get("paths", {}) if isinstance(cfg, dict) else {}

    source = args.source or paths.get("source")
    template = args.template or paths.get("template")
    mapping = args.mapping or paths.get("mapping") or default_mapping
    out = args.out or paths.get("out")
    if not out and paths.get("output_dir"):
        out = os.path.join(paths.get("output_dir"), "合同导入模板_填充.xlsx")

    missing = []
    if not source:
        missing.append("--source")
    if not template:
        missing.append("--template")
    if not out:
        missing.append("--out")
    if missing:
        p.error(f"Missing required arguments: {', '.join(missing)}")

    out_dir = os.path.dirname(out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    run(source, template, mapping, out)


if __name__ == "__main__":
    main()
