# 飞书合同数据项目

本项目提供"抓取 → 转换 → 模板填充"的完整流水线（`scripts/` + 包内模块）。抓取阶段可输出 JSONL/CSV/Excel；转换阶段按 `mapping.yaml` 规则将多 Sheet 数据映射到导入模板。

## 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置凭据
复制配置模板并填入应用凭据：
```bash
cp config/settings.example.yaml config/settings.yaml
# 编辑 config/settings.yaml，填入 feishu.app_key 和 feishu.app_secret
```

### 3. 准备数据源文件
在 `data/raw/` 目录创建以下文件之一：
- `contract_codes.txt`：合同编码清单（每行一个编码）
- `contract_ids.txt`：合同ID清单（每行一个ID）

### 4. 运行全流程
```bash
python scripts/run_full_pipeline.py --config config/settings.yaml
```

输出文件：
- 详情 Excel：`data/processed/contracts.xlsx`
- 导入模板：`output/合同导入模板_填充.xlsx`

## 功能概览
- **两种拉取方式**：
  - **合同编码拉取**：通过搜索接口 `POST /contract/v1/contracts/search` 查询合同ID，再拉取详情
  - **合同ID直接拉取**：已知合同ID时，直接调用详情接口，跳过搜索步骤（提升效率，降低频控风险）
- 详情接口：`GET /contract/v1/contracts/{contract_id}`（并发抓取，含退避重试）
- 导出：优先导出为 Excel（两个 Sheet：`search_items`、`details`），失败时回退为 CSV 两份文件
- 数据合并：支持同时使用两种方式拉取，自动去重合并

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
- `fetch.*`：抓取参数，含分页大小、可见范围、并发线程、限制数量等。
  - `contract_codes_file`：合同编码清单文件路径（通过搜索接口拉取）
  - `contract_ids_file`：合同ID清单文件路径（直接拉取详情，跳过搜索）
  - 支持同时配置两个文件，系统会自动合并去重
- `convert.*`：JSONL → CSV/Excel 的默认路径。
- `paths.*`：模板转换的默认路径；`output_dir` 用于默认输出导入模板位置。

## 一键全流程（抓取 → 转换 → 模板填充）
```bash
python scripts/run_full_pipeline.py --config config/settings.yaml
```
**说明**：
- 会依次处理 `contract_codes_file` 和 `contract_ids_file` 两种数据源
- 已拉取的合同ID会自动去重，避免重复请求
- 两种方式的数据会合并到同一个输出文件

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

## Transformer 模块 - mapping.yaml 配置详解

### 概述
`mapping.yaml` 是数据转换的核心配置文件，定义了如何将源 Excel 的多 Sheet 数据映射到目标导入模板。

### 配置结构

```yaml
version: 1

# 定义跨表关联键（用于多 Sheet JOIN）
join:
  keys:
    contract_number:  # 关联键名称
      details: contract_number
      form: contract_number
      payments: contract_number

# 目标 Sheet 映射规则
target_sheets:
  - name: 历史合同导入           # 目标 Sheet 名称
    row_key: contract_number    # 行唯一键
    row_policy: one_to_one      # 行策略：one_to_one / one_to_many
    source: details             # 主数据源 Sheet
    mappings:                   # 字段映射列表
      - to: { column: 合同编码 }
        from: [{ sheet: details, column: contract_number }]
        transform: [trim]
        required: true

# 字典映射表
dict:
  合同状态:
    9: "已归档"
    10: "已归档"
```

### 字段映射配置项

#### 1. 基础映射
```yaml
- to: { column: 合同名称 }              # 目标列名
  from: [{ sheet: details, column: contract_name }]  # 源表.源列
  transform: [trim]                     # 转换链（可选）
  required: true                        # 是否必填（可选）
  default: "默认值"                      # 默认值（可选）
```

#### 2. 多源回退（fallback_mode）- v1.1 新增
当第一个字段为空时，自动回退到备用字段：
```yaml
- to: { column: 合同编码 }
  from:
    - { sheet: details, column: contract_number }  # 优先使用
    - { sheet: details, column: contract_id }      # 回退字段
  transform: [trim]
  fallback_mode: first_non_empty  # 启用回退：取第一个非空值
```

#### 3. 条件过滤（where）
从 form 长表中按属性名提取特定字段：
```yaml
- to: { column: 需求人 }
  from: [{ sheet: form, column: attribute_value }]
  where: { attribute_name: "需求人" }  # 过滤条件
  transform:
    - json_parse: {}
    - form_pick: { field: name }
```

#### 4. 多条件回退（where_list）
支持不同 where 条件的回退：
```yaml
- to: { column: 需求人 }
  from:
    - { sheet: form, column: attribute_value }
    - { sheet: form, column: attribute_value }
  where_list:
    - { attribute_name: "需求人" }      # 第一个源的条件
    - { attribute_name: "L需求人" }     # 回退源的条件（历史字段）
  transform:
    - json_parse: {}
    - form_pick: { field: name }
  fallback_mode: first_non_empty
```

### 转换因子（Transform）详解

转换因子按**顺序链式执行**，前一步的输出作为后一步的输入。

#### Transform 执行流程示例

假设有如下配置：
```yaml
- to: { column: 需求人 }
  from: [{ sheet: form, column: attribute_value }]
  where: { attribute_name: "需求人" }
  transform:
    - json_parse: {}
    - form_pick: { field: name }
    - join_agg: { sep: ", " }
```

执行过程：
1. **数据获取**：从 form 表中筛选 `attribute_name = "需求人"` 的行，获取 `attribute_value` 列的值列表
   ```
   输入: ['[{"name":"张三","user_id":"001"}]', '[{"name":"李四","user_id":"002"}]']
   ```

2. **json_parse**：解析每个 JSON 字符串
   ```
   输出: [[{"name":"张三","user_id":"001"}], [{"name":"李四","user_id":"002"}]]
   ```

3. **form_pick**：从对象数组中提取 `name` 字段
   ```
   输出: ["张三", "李四"]
   ```

4. **join_agg**：将列表聚合为字符串
   ```
   输出: "张三, 李四"
   ```

5. **写入目标**：将最终结果写入目标列"需求人"

#### 字符串处理
- **trim**：去除首尾空白
  ```yaml
  transform: [trim]
  ```

#### JSON 解析
- **json_parse**：将 JSON 字符串解析为对象/数组
  ```yaml
  transform:
    - json_parse: {}
  ```

#### 字段提取
- **form_pick**：从对象/数组中提取字段
  ```yaml
  # 提取单个字段
  - form_pick: { field: name }
  
  # 提取多个字段（返回对象列表）
  - form_pick: { field_pairs: [name, user_id], unique: true }
  ```

- **to_value_label**：构造 value-label 映射结构
  ```yaml
  - to_value_label:
      value_field: user_id
      label_field: name
      value_dict: 员工映射        # 引用字典进行值转换
      keep_original: true         # 字典未命中时保留原值
      default: ""
  ```

#### 格式化与聚合
- **format_each**：格式化列表中每个元素
  ```yaml
  - format_each: { template: "{name}({user_id})" }
  ```

- **join_agg**：将列表聚合为字符串
  ```yaml
  - join_agg: { sep: ", ", unique: true }
  ```

- **json_stringify**：将对象序列化为 JSON 字符串
  ```yaml
  - json_stringify: {}
  ```

  支持为空列表指定输出（避免空值输出为 `[]`）：
  ```yaml
  - json_stringify:
      empty: [{"value": "", "label": ""}]
  ```

#### 数值处理
- **number_parse**：解析数值字符串
  ```yaml
  - number_parse: { thousands: ",", decimal: "." }
  ```

- **round**：四舍五入
  ```yaml
  - round: 2  # 保留两位小数
  ```

#### 日期处理
- **date_parse**：解析日期字符串
  ```yaml
  - date_parse:
      input_formats: ["yyyy-MM-dd", "yyyy/M/d", "yyyy-MM-dd HH:mm:ss"]
      tz: "Asia/Shanghai"
  ```

- **date_format**：格式化日期
  ```yaml
  - date_format: "yyyy-MM-dd HH:mm:ss"
  ```

#### 字典映射
- **dict**：根据字典表转换值
  ```yaml
  - dict: { table: 合同状态, default: "" }
  ```

### 实用示例

#### 示例1：基础字段映射
```yaml
- to: { column: 合同名称 }
  from: [{ sheet: details, column: contract_name }]
  transform: [trim]
  required: true
```

#### 示例2：数值字段（带格式化）
```yaml
- to: { column: 合同总额（含税） }
  from: [{ sheet: details, column: amount }]
  transform:
    - number_parse: { thousands: ",", decimal: "." }
    - round: 4
```

#### 示例3：日期字段
```yaml
- to: { column: 合同签订时间 }
  from: [{ sheet: details, column: signed_time }]
  transform:
    - date_parse:
        input_formats: ["yyyy-MM-dd HH:mm:ss", "yyyy/M/d H:mm"]
        tz: "Asia/Shanghai"
    - date_format: "yyyy-MM-dd HH:mm:ss"
```

#### 示例4：form 长表字段（JSON 解析 + 字段提取）
```yaml
- to: { column: 需求人 }
  from:
    - { sheet: form, column: attribute_value }
    - { sheet: form, column: attribute_value }
  where_list:
    - { attribute_name: "需求人" }
    - { attribute_name: "L需求人" }
  transform:
    - json_parse: {}
    - form_pick: { field_pairs: [name, user_id], unique: true }
    - to_value_label:
        value_field: user_id
        label_field: name
        value_dict: 员工映射
        keep_original: true
        default: ""
    - json_stringify: { empty: [{"value": "", "label": ""}] }
  fallback_mode: first_non_empty
```

#### 示例5：字段回退
```yaml
# contract_number 为空时使用 contract_id
- to: { column: 合同编码 }
  from:
    - { sheet: details, column: contract_number }
    - { sheet: details, column: contract_id }
  transform: [trim]
  fallback_mode: first_non_empty
```

#### 示例6：字典映射
```yaml
- to: { column: 合同状态 }
  from: [{ sheet: details, column: contract_status_code }]
  transform:
    - dict: { table: 合同状态, default: "" }

# 在文件末尾定义字典
dict:
  合同状态:
    9: "已归档"
    10: "已归档"
    17: "已终止"
```

### 行策略（row_policy）

- **one_to_one**：一行源数据对应一行目标数据（主表）
- **one_to_many**：一行源数据展开为多行目标数据（明细表、付款计划等）

### 外部字典（dict_sources）

支持从 Excel 文件加载字典映射：
```yaml
dict_sources:
  - name: 员工映射
    path: ./dicts/hpfm_employee.xlsx
    sheet: hpfm_employee
    key_column: attribute3      # 源值列
    value_column: employee_num  # 目标值列

  - name: 部门映射
    path: ./dicts/LX-HR.xlsx
    sheet: Result 1
    key_column: feishu_id
    value_column: department_id
```

### 空值处理规则

1. **空值定义**：`None`、空字符串 `""`、`NaN`
2. **未配置 fallback_mode**：合并所有 from 源的值（原有行为）
3. **配置 fallback_mode: first_non_empty**：按顺序取第一个非空值
4. **所有源均为空**：使用 `default` 或留空
5. **required: true**：空值会触发校验错误

## 目录结构与更多说明
- 详见《项目结构.md》与各模块文档。

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
- `logs/run_jsonl_convert.log`：JSONL 转换日志
- `logs/run_transform.log`：模板填充日志
- `logs/run_full_pipeline.log`：全流程日志

所有日志采用按大小旋转策略（2MB × 5 份），格式统一。

## 常见配置问题

### 1. Excel 输出中文乱码
**问题**：导出的 CSV 在 Excel 中打开显示乱码

**解决**：项目默认使用 `utf-8-sig` 编码（带 BOM），可直接被 Windows Excel 识别。如需修改：
```yaml
# config/settings.yaml
options:
  encoding: utf-8-sig  # 或 gbk、gb18030
```

### 2. 长数字显示为科学计数法
**问题**：contract_id、contract_number 等长数字在 Excel 中显示为科学计数法

**解决**：配置文本列，强制按文本处理：
```yaml
# config/settings.yaml
options:
  text_columns:
    - contract_id
    - contract_code
    - contract_number
```

### 3. 输出文件被占用
**问题**：运行时提示 `PermissionError`，无法写入文件

**解决**：
- 关闭占用输出文件的程序（如 Excel）
- 或者等待程序自动重试（每 2 秒重试一次，直到文件释放）

### 4. mapping.yaml 配置不生效
**检查清单**：
- ✅ YAML 语法正确（注意缩进、冒号、列表格式）
- ✅ `from` 字段使用正确的列表格式
- ✅ 使用 `fallback_mode` 时列出了多个源
- ✅ `where` 或 `where_list` 条件与实际数据匹配
- ✅ transform 算子名称拼写正确

### 5. form 长表字段提取失败
**常见原因**：
- `attribute_name` 与实际数据不匹配（区分大小写）
- JSON 解析失败（使用 `json_parse` 前确认字段是 JSON 字符串）
- 字段提取路径错误（检查 `form_pick` 的 `field` 参数）

**调试方法**：
```yaml
# 简化 transform 链，逐步添加算子验证
transform:
  - trim  # 先只用 trim，查看原始值
```

## 目录结构

```
飞书合同数据项目/
├── config/
│   ├── settings.example.yaml   # 配置模板
│   └── settings.yaml            # 实际配置（需自行创建，已 gitignore）
├── data/
│   ├── raw/                     # 原始数据（合同编码/ID清单）
│   ├── interim/                 # 中间数据（JSONL）
│   └── processed/               # 处理后数据（CSV/Excel）
├── docs/                        # 文档目录
│   └── 项目结构.md
├── feishu_contracts/            # 核心包
│   ├── common/                  # 公共模块（配置、日志）
│   ├── fetch/                   # 抓取模块
│   ├── convert/                 # JSONL 转换模块
│   ├── transform/               # 数据转换模块
│   │   ├── transformer.py       # 转换引擎
│   │   ├── mapping.yaml         # 映射配置
│   │   └── dicts/               # 外部字典文件
│   └── cli.py                   # 命令行入口
├── logs/                        # 日志目录
├── output/                      # 输出目录（导入模板）
├── scripts/                     # 脚本目录
│   ├── run_full_pipeline.py     # 全流程脚本
│   ├── run_jsonl_convert.py     # JSONL 转换脚本
│   └── run_transform.py         # 模板填充脚本
├── templates/                   # 导入模板文件
├── tests/                       # 测试目录
├── requirements.txt             # 依赖清单
└── README.md                    # 本文件
```
