# 飞书合同数据项目文档

## 快速开始
- 安装依赖：
```bash
python -m pip install -r requirements.txt
```

- 配置路径：复制并编辑 `config/settings.yaml`
  - 首次使用：复制示例 `config/settings.example.yaml` 为 `config/settings.yaml`，填入 `feishu.app_key/app_secret` 等。
  - 仓库已忽略 `config/settings.yaml`，避免凭据泄露。
  - `paths.source`：转换输入 Excel
  - `paths.template`：导入模板 Excel
  - `paths.mapping`：字段映射配置（默认 `feishu_contracts/transform/mapping.yaml`）
  - `paths.output_dir`：输出目录（默认 `output/`）
  - `convert.*`：JSONL → CSV/Excel 转换默认路径

## 运行方式
- JSONL → CSV/Excel：
```bash
python scripts/run_jsonl_convert.py --config config/settings.yaml
# 或显式指定
python scripts/run_jsonl_convert.py --input data/raw/contracts.jsonl --csv data/processed/contracts.csv --excel data/processed/contracts.xlsx
```

- Excel（多Sheet）→ 导入模板：
```bash
python scripts/run_transform.py --config config/settings.yaml
# 或显式指定
python -m feishu_contracts.cli --config config/settings.yaml
# 或显式指定模板路径
python -m feishu_contracts.cli --source data/processed/contracts.xlsx --template templates/合同导入模板.xlsx --out output/合同导入模板_填充.xlsx
```

## 日志
- 运行日志：`logs/run_jsonl_convert.log`、`logs/run_transform.log`
 - 日志格式统一：`%(asctime)s [%(levelname)s] %(message)s`，旋转大小 2MB × 5 份。

## 目录规范
- 数据：`data/raw`、`data/interim`、`data/processed`
- 产出：`output/`
- 脚本：`scripts/`
- 配置：`config/`
- 转换模块：`feishu_contracts/transform/`
- 抓取模块：`feishu_contracts/fetch/`（提供 `run_fetch()` 与 API 客户端）
- 模板：`templates/`

## 测试
- 依赖（开发机）：
```bash
python -m pip install pytest
```
- 运行：
```bash
pytest -q
```
- 测试位置：
  - `tests/test_jsonl_converter.py`
  - `tests/test_transformer.py`

## 注意
- 不使用 `.env`（统一 `config/settings.yaml`）。
- 不引入 `pyproject.toml`（依赖以 `requirements.txt` 为准）。
 - `config/settings.yaml` 已加入 `.gitignore`，请使用 `settings.example.yaml` 作为模板。
