# 飞书合同数据项目

本项目提供“抓取 → 转换 → 模板填充”的完整流水线（`scripts/` + 包内模块）。抓取阶段可输出 JSONL/CSV/Excel；转换阶段按 `mapping.yaml` 规则将多 Sheet 数据映射到导入模板。

## 功能概览
- 搜索接口：`POST /contract/v1/contracts/search`（分页，使用 `combine_condition.permission` 过滤可见范围）
- 详情接口：`GET /contract/v1/contracts/{contract_id}`（并发抓取，含退避重试）
- 导出：优先导出为 Excel（两个 Sheet：`search_items`、`details`），失败时回退为 CSV 两份文件

> 注：已移除根目录单脚本方式，请使用 `scripts/` 下脚本或包内 CLI。

## 先决条件
- 应用权限：至少需要 `获取合同信息(contract:contract:readonly)`
- Python 3.8+
- 网络可访问飞书开放平台

## 依赖安装（可选）
如果希望导出为 Excel，请安装：
```bash
python3 -m pip install requests pandas openpyxl
```
仅导出 CSV 则只需 `requests`：
```bash
python3 -m pip install requests
```

## 使用方式（概要）
请参考下文“一键全流程/分步运行”命令，或使用包内 CLI：`python -m feishu_contracts.cli`。

## 输出说明
- Excel：两个 Sheet
  - `search_items`：搜索接口原始结果
  - `details`：详情接口原始结果（展开后结构）
- CSV 回退：同名前缀 `_search.csv`、`_details.csv`

## 常见问题
- 权限不足：确保已在应用后台开通 `contract:contract:readonly`，若需要用户 ID 字段还需 `contact:user.employee_id:readonly`
- 频控限制：如遇 429/频控，可在 `config/settings.yaml` 中调小 `fetch.detail_workers`，或在企业网关/代理侧加速
- Excel 写入失败：未安装 `pandas/openpyxl` 或文件占用，脚本会自动回退到 CSV
- 网络超时：脚本内置重试，对于详情请求有指数回退；如仍失败可稍后重试或调小并发

## 参数建议
- `PAGE_SIZE`：50 合理；大量数据可适当增大，但注意接口频控
- `DETAIL_WORKERS`：建议 4~10 区间，根据网络与频控调优
- `PERMISSION`：若仅需本人可见或申请的合同，设置为 1 或 2 可减少数据量

## 安全
请勿将应用 `App Key`/`App Secret` 提交到公开仓库；推荐通过 CI/CD 或运行环境安全注入。

---

# 全项目运行与使用说明

## 配置 `config/settings.yaml`
- `feishu.app_key` / `feishu.app_secret`：飞书应用凭据（必填，用于“全流程”脚本）。
- `fetch.*`：抓取参数，含分页大小、可见范围、并发线程、限制数量、合同编码清单文件等。
- `convert.*`：JSONL → CSV/Excel 的默认路径。
- `paths.*`：模板转换的默认路径；`output_dir` 用于默认输出导入模板位置。

## 一键全流程（抓取 → 转换 → 模板填充）
```bash
python scripts/run_full_pipeline.py --config config/settings.yaml
```
输出包括：
- JSONL：`convert.jsonl_input`
- 详情 CSV：`convert.csv_output`
- 详情 Excel：`convert.excel_output`
- 状态清单：`data/processed/contracts_status.csv`
- 导入模板：`paths.output_dir/合同导入模板_填充.xlsx`

## 分步运行
- JSONL → CSV/Excel：
```bash
python scripts/run_jsonl_convert.py --config config/settings.yaml
# 或显式参数
python scripts/run_jsonl_convert.py --input data/raw/contracts.jsonl --csv data/processed/contracts.csv --excel data/processed/contracts.xlsx
```
- Excel（多Sheet）→ 导入模板：
```bash
python scripts/run_transform.py --config config/settings.yaml
# 或包内 CLI（等价）
python -m feishu_contracts.cli --config config/settings.yaml
```
说明：显式 CLI 参数优先于配置文件默认值。

## 目录结构与更多说明
- 详见《项目结构.md》与 `docs/README.md`。

## 测试
```bash
python -m pip install -r requirements.txt
python -m pip install pytest
pytest -q
```
包含：
- `tests/test_jsonl_converter.py`：空/基础 JSONL 转换用例。
- `tests/test_transformer.py`：动态构造模板与源数据，验证填充结果。

## 日志
- `logs/run_jsonl_convert.log`
- `logs/run_transform.log`
- `logs/run_full_pipeline.log`
