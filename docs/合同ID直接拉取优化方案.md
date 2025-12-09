# 合同ID直接拉取优化方案

## 1. 需求背景

### 1.1 现有流程
目前系统支持**合同编码→合同ID→结构化数据**的拉取流程：
1. 从 `data/raw/合同编码清单.txt` 读取合同编码列表
2. 调用搜索接口 `POST /contract/v1/contracts/search` 根据合同编码查询合同ID
3. 调用详情接口 `GET /contract/v1/contracts/{contract_id}` 获取结构化数据
4. 输出为 JSONL/CSV/Excel，并进行后续模板转换

### 1.2 新增需求
增加**合同ID直接拉取**场景：
1. 从新的配置文件读取合同ID列表
2. 直接调用详情接口获取结构化数据（跳过搜索步骤）
3. 将两种数据源的结果合并
4. 完成后续转换与模板填充

### 1.3 业务价值
- **提升效率**：对于已知合同ID的场景，跳过搜索接口调用，减少API请求次数
- **降低频控风险**：搜索接口与详情接口分别计频，减少搜索接口压力
- **灵活性**：支持多种数据来源混合拉取

---

## 2. 技术方案

### 2.1 配置层改动

#### 2.1.1 配置文件新增项
在 `config/settings.yaml` 的 `fetch` 段新增配置项：

```yaml
fetch:
  page_size: 50
  permission: 0
  detail_workers: 1
  limit_contracts: 0
  contract_codes_file: data/raw/合同编码清单.txt  # 原有：合同编码清单
  contract_ids_file: data/raw/合同ID清单.txt      # 新增：合同ID清单
```

#### 2.1.2 数据文件格式
- **合同ID清单文件**：纯文本文件，每行一个合同ID（与合同编码清单相同格式）
- **位置**：`data/raw/合同ID清单.txt`（可在配置中修改）
- **编码**：UTF-8
- **示例内容**：
  ```
  7403698906755244036
  7403698906755244037
  7403698906755244038
  ```

---

### 2.2 代码层改动

#### 2.2.1 FetchConfig 数据类扩展
在 `feishu_contracts/fetch/client.py` 的 `FetchConfig` 类中新增字段：

```python
@dataclass
class FetchConfig:
    # ... 原有字段 ...
    contract_codes_file: Optional[str] = None
    contract_codes: List[str] = None  # type: ignore[assignment]
    
    # 新增字段
    contract_ids_file: Optional[str] = None       # 合同ID清单文件路径
    contract_ids: List[str] = None  # type: ignore[assignment]  # 内联合同ID列表
```

#### 2.2.2 新增合同ID加载函数
新增 `_load_contract_ids()` 函数（与 `_load_contract_codes()` 类似）：

```python
def _load_contract_ids(contract_ids_file: Optional[str], inline_ids: List[str]) -> List[str]:
    """
    从文件和内联参数加载合同ID列表
    返回去重后的合同ID列表
    """
    ids: List[str] = []
    if contract_ids_file:
        try:
            with open(contract_ids_file, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s:
                        ids.append(s)
        except Exception:
            pass
    for cid in inline_ids or []:
        s = str(cid).strip()
        if s:
            ids.append(s)
    # 去重
    seen = set()
    deduped: List[str] = []
    for cid in ids:
        if cid not in seen:
            seen.add(cid)
            deduped.append(cid)
    return deduped
```

#### 2.2.3 新增合同ID直接拉取函数
新增 `_fetch_details_by_ids()` 函数：

```python
def _fetch_details_by_ids(
    contract_ids: List[str],
    client: FeishuContractClient,
    jsonl_path: str,
    seen_ids: set,
    attempt: int,
    limit_contracts: int = 0,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    直接根据合同ID列表拉取详情，不经过搜索接口
    返回：(状态行列表, 失败的合同ID列表)
    """
    status_rows: List[Dict[str, Any]] = []
    failed_ids: set = set()
    count_written = 0
    success_count = 0
    fail_count = 0
    start_t = time.time()
    progress_interval = max(1, int(get_progress_interval(100)))
    
    for idx, cid in enumerate(contract_ids):
        cid = str(cid).strip()
        if not cid:
            continue
            
        try:
            logger.debug("fetch_detail_by_id start", extra={"contract_id": cid})
        except Exception:
            pass
        
        try:
            detail_list = client.fetch_all_details([cid], workers=1)
            detail = detail_list[0] if detail_list else {}
            
            if isinstance(detail, dict) and detail.get("_error"):
                raise RuntimeError(str(detail.get("_error")))
            
            if isinstance(detail, dict) and cid not in seen_ids:
                rec = dict(detail)
                rec.setdefault("contract_id", cid)
                # 注意：从合同ID直接拉取时，没有合同编码信息
                # 可以从详情数据中提取 contract_number 字段作为合同编码
                contract_code = detail.get("contract_number") or ""
                rec.setdefault("contract_code", contract_code)
                
                _append_jsonl_line(jsonl_path, rec)
                seen_ids.add(cid)
                count_written += 1
                success_count += 1
            
            status_rows.append({
                "contract_code": detail.get("contract_number") or "",
                "contract_id": cid,
                "status": "success",
                "error": "",
                "attempt": attempt,
            })
            
        except Exception as e:
            status_rows.append({
                "contract_code": "",
                "contract_id": cid,
                "status": "fail",
                "error": str(e),
                "attempt": attempt,
            })
            failed_ids.add(cid)
            fail_count += 1
        
        # 进度日志
        processed = idx + 1
        if processed % progress_interval == 0 or processed == len(contract_ids):
            elapsed = max(time.time() - start_t, 1e-6)
            rps = processed / elapsed
            remaining = max(len(contract_ids) - processed, 0)
            eta_s = (remaining / rps) if rps > 0 else 0.0
            logger.info(
                "progress %d/%d ok=%d fail=%d rps=%.1f eta=%.0fs",
                processed, len(contract_ids), success_count, fail_count, rps, eta_s,
                extra={"is_progress": True, "contract_id": cid},
            )
        
        if limit_contracts > 0 and count_written >= limit_contracts:
            break
    
    return status_rows, sorted(failed_ids)
```

#### 2.2.4 改造 run_fetch() 主流程
在 `run_fetch()` 函数中，整合两种数据源的拉取逻辑：

```python
def run_fetch(cfg: FetchConfig) -> Dict[str, Any]:
    # ... 原有初始化代码 ...
    
    # 加载合同编码与合同ID
    codes = _load_contract_codes(cfg.contract_codes_file, cfg.contract_codes)
    contract_ids = _load_contract_ids(cfg.contract_ids_file, cfg.contract_ids)
    
    if not codes and not contract_ids:
        # 两种数据源都为空，直接返回
        return {
            "jsonl": cfg.output_jsonl,
            "status_csv": cfg.status_csv,
            "csv": cfg.final_csv,
            "excel": cfg.output_xlsx,
            "list_csvs": {},
        }
    
    # 重置输出文件
    for p in [cfg.output_jsonl, cfg.status_csv]:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
    
    seen_ids: set = set()
    
    # ===== 第一阶段：处理合同编码（原有逻辑） =====
    if codes:
        logger.info("开始处理合同编码，共 %d 条", len(codes))
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
            # ... 原有重试逻辑 ...
            _write_status_csv(cfg.status_csv, status_rows, append=(attempt > 1), 
                            encoding=cfg.encoding, text_columns=(cfg.text_columns or []))
            if not failed_codes:
                break
            if attempt >= total_passes:
                break
            current_codes = failed_codes
    
    # ===== 第二阶段：处理合同ID（新增逻辑） =====
    if contract_ids:
        logger.info("开始处理合同ID，共 %d 条", len(contract_ids))
        current_ids: List[str] = list(contract_ids)
        max_retry_rounds = 3
        total_passes = max_retry_rounds + 1
        
        for i in range(total_passes):
            attempt = i + 1
            status_rows, failed_ids = _fetch_details_by_ids(
                current_ids,
                client,
                cfg.output_jsonl,
                seen_ids,
                attempt,
                limit_contracts=max(0, int(cfg.limit_contracts or 0)) if not codes else 0,  # 如果已处理codes且达到限制，则跳过
            )
            try:
                ok_cnt = sum(1 for r in status_rows if str(r.get("status")) == "success")
                fail_cnt = sum(1 for r in status_rows if str(r.get("status")) != "success")
                logger.info(
                    "contract_ids_round_summary pass=%d ok=%d fail=%d remain=%d",
                    attempt, ok_cnt, fail_cnt, len(failed_ids),
                )
            except Exception:
                pass
            _write_status_csv(cfg.status_csv, status_rows, append=True,  # 追加到状态文件
                            encoding=cfg.encoding, text_columns=(cfg.text_columns or []))
            if not failed_ids:
                break
            if attempt >= total_passes:
                break
            current_ids = failed_ids
    
    # ===== 第三阶段：转换JSONL为CSV/Excel（原有逻辑） =====
    # ... 原有convert逻辑 ...
    
    return { ... }
```

---

### 2.3 调用层改动

#### 2.3.1 run_full_pipeline.py 改动
在 `scripts/run_full_pipeline.py` 中，从配置读取 `contract_ids_file` 并传递给 `FetchConfig`：

```python
def main() -> None:
    # ... 原有配置加载 ...
    
    codes_file = fetch.get("contract_codes_file") or "合同编码清单.txt"
    if not os.path.isabs(codes_file):
        codes_file = os.path.join(PROJ_ROOT, codes_file)
    
    # 新增：加载合同ID清单文件路径
    ids_file = fetch.get("contract_ids_file") or ""
    if ids_file and not os.path.isabs(ids_file):
        ids_file = os.path.join(PROJ_ROOT, ids_file)
    
    fetch_cfg = FetchCfg(
        # ... 原有参数 ...
        contract_codes_file=codes_file,
        contract_codes=[],
        contract_ids_file=ids_file,      # 新增
        contract_ids=[],                 # 新增
        # ... 其他参数 ...
    )
    _fetch_result = run_fetch(fetch_cfg)
    # ... 后续流程不变 ...
```

---

## 3. 数据流示意

### 3.1 原有流程（合同编码）
```
合同编码清单.txt (contract_codes_file)
    ↓
加载为 codes 列表
    ↓
搜索接口 search_contracts_by_number(contract_number)
    ↓
返回 contract_id
    ↓
详情接口 get_contract_detail(contract_id)
    ↓
写入 JSONL + 状态CSV
    ↓
转换为 CSV/Excel + 后续模板转换
```

### 3.2 新增流程（合同ID）
```
合同ID清单.txt (contract_ids_file)
    ↓
加载为 contract_ids 列表
    ↓
详情接口 get_contract_detail(contract_id) (跳过搜索)
    ↓
写入 JSONL + 状态CSV (追加模式)
    ↓
合并到同一 JSONL 文件
    ↓
转换为 CSV/Excel + 后续模板转换
```

### 3.3 合并流程（两种数据源混合）
```
合同编码清单.txt + 合同ID清单.txt
    ↓           ↓
第一阶段处理   第二阶段处理
    ↓           ↓
    seen_ids 去重合并
         ↓
    单一 JSONL 文件
         ↓
   统一转换为 CSV/Excel
         ↓
    后续模板转换
```

---

## 4. 影响范围与风险评估

### 4.1 影响范围
| 模块 | 文件 | 改动类型 | 影响评估 |
|------|------|----------|----------|
| 配置 | `config/settings.yaml` | 新增配置项 | 低风险，向后兼容 |
| 配置示例 | `config/settings.example.yaml` | 新增配置项 | 低风险 |
| 数据加载 | `feishu_contracts/fetch/client.py` | 新增函数 | 低风险，不影响原有逻辑 |
| 抓取流程 | `feishu_contracts/fetch/client.py` | 修改 `run_fetch()` | 中风险，需充分测试 |
| 全流程脚本 | `scripts/run_full_pipeline.py` | 新增参数传递 | 低风险 |
| 数据文件 | `data/raw/合同ID清单.txt` | 新增文件 | 无风险 |

### 4.2 风险点
1. **去重逻辑**：需确保 `seen_ids` 在两个阶段间正确共享，避免重复拉取
2. **状态CSV写入**：第二阶段需使用追加模式（`append=True`），避免覆盖第一阶段结果
3. **limit_contracts 限制**：需明确限制是针对总量还是各阶段独立计数
4. **进度日志**：需区分两个阶段的日志输出，避免进度统计混乱
5. **向后兼容性**：未配置 `contract_ids_file` 时，系统应保持原有行为


## 8. 文档
### 8.1 需要更新的文档
- `README.md`：在"使用方式"章节补充合同ID直接拉取说明
- `docs/项目结构.md`：更新数据文件结构说明
- `config/settings.example.yaml`：添加 `contract_ids_file` 配置项与中文注释

## 9. 交付清单

### 9.1 代码变更
- [ ] `feishu_contracts/fetch/client.py`
  - [ ] `FetchConfig` 类新增字段
  - [ ] 新增 `_load_contract_ids()` 函数
  - [ ] 新增 `_fetch_details_by_ids()` 函数
  - [ ] 改造 `run_fetch()` 函数
- [ ] `scripts/run_full_pipeline.py`
  - [ ] 新增 `contract_ids_file` 参数加载与传递

### 9.2 配置变更
- [ ] `config/settings.example.yaml`
  - [ ] 新增 `fetch.contract_ids_file` 配置项与说明

### 9.3 数据文件
- [ ] `data/raw/合同ID清单.txt`
  - [ ] 创建空文件或示例文件
  - [ ] 更新 `.gitignore`（如果需要排除实际数据）

### 9.4 文档变更
- [ ] `README.md`
  - [ ] 新增合同ID直接拉取场景说明
  - [ ] 更新使用示例
- [ ] `docs/项目结构.md`
  - [ ] 更新数据目录结构说明
- [ ] `docs/合同ID直接拉取优化方案.md`（本文档）
  - [ ] 归档到 `docs/` 目录

### 9.5 测试用例
- [ ] `tests/test_fetch_contract_ids.py`（新增）
  - [ ] 测试合同ID加载
  - [ ] 测试合同ID直接拉取
  - [ ] 测试混合场景

---

## 11. 附录

### 11.1 示例配置
```yaml
# config/settings.yaml 示例（仅展示 fetch 段）
fetch:
  page_size: 50
  permission: 0
  detail_workers: 4
  limit_contracts: 0
  contract_codes_file: data/raw/合同编码清单.txt   # 合同编码清单（原有）
  contract_ids_file: data/raw/合同ID清单.txt       # 合同ID清单（新增）
```

### 11.2 示例数据文件
**data/raw/合同ID清单.txt**
```
7403698906755244036
7403698906755244037
7403698906755244038
```

### 11.3 示例状态CSV输出
```csv
contract_code,contract_id,status,error,attempt
HT2024001,7403698906755244036,success,,1
,7403698906755244037,success,,1
HT2024002,7403698906755244038,fail,network timeout,1
```
说明：
- 第一行：通过合同编码拉取，有合同编码信息
- 第二行：通过合同ID直接拉取，合同编码为空或从详情中提取
- 第三行：拉取失败的记录

---

## 版本记录

| 版本 | 日期 | 作者 | 变更说明 |
|------|------|------|----------|
| v1.0 | 2025-01-XX | AI助手 | 初版方案 |

