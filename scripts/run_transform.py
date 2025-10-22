import argparse
import os
import logging
import sys


def main():
    parser = argparse.ArgumentParser(description="Run template transform using settings.yaml defaults")
    default_config = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "settings.yaml")
    parser.add_argument("--config", default=default_config, help="Path to settings.yaml")
    parser.add_argument("--source")
    parser.add_argument("--template")
    parser.add_argument("--mapping")
    parser.add_argument("--out")
    args = parser.parse_args()

    # ensure project root on sys.path for imports when running from scripts/
    proj_root = os.path.dirname(os.path.dirname(__file__))
    if proj_root not in sys.path:
        sys.path.insert(0, proj_root)
    # now import project modules
    from feishu_contracts.transform.transformer import run as transform_run
    from feishu_contracts.common.logging import setup_logger
    from feishu_contracts.common.config import load_settings

    # logging setup
    log_path = os.path.join(proj_root, "logs", "run_transform.log")
    setup_logger(log_path)

    cfg = load_settings(args.config)
    paths = cfg.get("paths", {}) if isinstance(cfg, dict) else {}

    source = args.source or paths.get("source")
    template = args.template or paths.get("template")
    mapping = args.mapping or paths.get("mapping") or os.path.join(proj_root, "feishu_contracts", "transform", "mapping.yaml")
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
        parser.error(f"Missing required arguments: {', '.join(missing)}")

    out_dir = os.path.dirname(out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    logging.info(f"transform start: source={source}, template={template}, mapping={mapping}, out={out}")
    transform_run(source, template, mapping, out)
    logging.info(f"transform done: out={out}")
    print(f"Transform done → {out}")


if __name__ == "__main__":
    main()
