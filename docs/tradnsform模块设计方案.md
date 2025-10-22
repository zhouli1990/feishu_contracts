# 飞书历史合同数据 → 新系统导入模板 设计方案

## 1. 背景与目标
- **背景**：历史合同数据来自飞书导出，源Excel包含多个Sheet；目标是在“新系统导入模板”中落库，模板同样包含多个Sheet，字段命名、结构、约束与源不一致。
- **目标**：以“配置驱动”的方式，将多Sheet源数据映射、清洗、关联、转换为目标模板结构，并写入模板文件，最大化保留模板样式/下拉/校验。
- **非目标**：不改造飞书导出逻辑；不涉及导入模板的业务规则变更（仅适配）。

## 2. 范围与假设
- **范围**：主合同、明细、付款计划等多Sheet数据的读取、清洗、映射、写入；异常数据审计与报表；最小化侵入现有拉取脚本。
- **假设**：
  - 存在跨Sheet稳定关联键（如 `contract_id` 或 `contract_no`）。
  - 模板文件可作为底稿提供（含样式、下拉、数据验证）。
  - 数据量在单机可处理范围内（如 ≤ 10万行/Sheet）。

  - 源文件：`contracts.xlsx`，核心Sheet：`details`、`form`（`form` 为长表：一合同多属性行）。
  - 模板文件：`合同导入模板.xlsx`，目标Sheet：`历史合同导入`。

## 3. 总体架构
- **分层**：
  - Fetch 层（既有）：负责拉取/导出源Excel，不改动核心逻辑。
  - Transform 层（新增）：根据 `mapping.yaml` 完成多Sheet关联、字段转换、校验、行策略处理。
  - Writer 层（新增）：基于模板以“值写入”的方式填充目标Sheet并保留格式/校验。
- **目录建议**：
```
feishu_contracts/
  fetcher.py                  # 现有或既有拉取逻辑（保持原职能）
  transform/
    transformer.py            # 转换引擎（读取配置、执行转换、行策略）
    validators.py             # 校验与必填检查
    mapping.yaml              # 映射配置（可多份：不同模板/来源）
  cli.py                      # 命令行编排（fetch → transform → export）
  tests/                      # 单元/集成测试
```
- **技术栈**：Python 3.x；`pandas`（计算与中间数据）、`openpyxl`（模板保真写入）、`pyyaml`（读取YAML配置）。

## 4. 数据流与处理流程
1) 读取源Excel的多Sheet为表（DataFrame）。
2) 读取 `mapping.yaml`：解析目标Sheet、字段映射、跨表关联键、转换算子、校验规则、行策略。
3) 基于关联键完成多表合并/聚合或行展开；执行字段级转换（trim、日期解析、字典映射等）。
4) 先做数据校验与审计：必填/唯一/值域/外键存在性等，输出错误报告。
5) 打开“导入模板.xlsx”作为底稿，用 `openpyxl` 把转换后的数据逐行写入对应Sheet与列，保留模板格式/校验。
6) 输出最终“可导入”的新文件，并产出转换与校验日志。

## 5. 映射配置规范（YAML）
- **核心理念**：目标导入模板为中心；每个目标Sheet定义行粒度（row_key/row_policy），每个目标列定义来源字段与转换链。
- **字段路径表达**：`sheet.column`，如 `details.contract_name`。
- **跨Sheet关联**：通过 `join.keys` 指定主键/外键字段。
- **行策略（row_policy）**：
  - `one_to_one`：一行源 → 一行目标。
  - `one_to_many`：主合同行展开至明细行。
  - `many_to_one`：明细聚合至主合同（sum/first/concat 等）。

- **YAML Schema（简化示例）**：
```yaml
version: 1
join:
  keys:
    contract_number:
      details: contract_number
      form: contract_number
      payments: contract_number
      contract_budget_list: contract_number
      our_party_list: contract_number
      counter_party_list: contract_number

target_sheets:
  - name: 历史合同导入
    row_key: contract_number
    row_policy: one_to_one
    source: details
    mappings:
      # 通用字段：来自 details
      - to: { column: 合同名称 }
        from: [{ sheet: details, column: contract_name }]
        transform: [trim]
        required: true
      - to: { column: 收支类型 }
        from: [{ sheet: details, column: pay_type_name }]
        transform: [trim]
      - to: { column: 合同说明 }
        from: [{ sheet: details, column: remark }]
        transform: [trim]
      - to: { column: 计价方式 }
        from: [{ sheet: details, column: property_type_name }]
        transform:
          - dict_map: { mapping: pricing_type_dict, default: UNKNOWN }
      - to: { column: 合同总额 }
        from: [{ sheet: details, column: amount }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 预估金额 }
        from: [{ sheet: details, column: estimated_amount }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 币种 }
        from: [{ sheet: details, column: currency_name }]
        transform:
          - dict_map: { mapping: currency_dict, default: CNY }
      - to: { column: 合同期限类型 }
        from: [{ sheet: details, column: fixed_validity_name }]
        transform:
          - dict_map: { mapping: validity_type_dict, default: UNKNOWN }
      - to: { column: 合同期限说明 }
        from: [{ sheet: details, column: validity_date_desc }]
        transform: [trim]
      - to: { column: 合同开始日期 }
        from: [{ sheet: details, column: start_date }]
        transform:
          - date_parse: { input_formats: ["yyyy-MM-dd", "yyyy/M/d", "yyyy/M/dd"], tz: "Asia/Shanghai" }
          - date_format: "yyyy-MM-dd"
      - to: { column: 合同结束日期 }
        from: [{ sheet: details, column: end_date }]
        transform:
          - date_parse: { input_formats: ["yyyy-MM-dd", "yyyy/M/d", "yyyy/M/dd"], tz: "Asia/Shanghai" }
          - date_format: "yyyy-MM-dd"
      - to: { column: 合同申请时间 }
        from: [{ sheet: details, column: create_time }]
        transform:
          - date_parse: { input_formats: ["yyyy-MM-dd HH:mm:ss", "yyyy/M/d H:mm", "yyyy-MM-dd"], tz: "Asia/Shanghai" }
          - date_format: "yyyy-MM-dd HH:mm:ss"
      - to: { column: 合同签订时间 }
        from: [{ sheet: details, column: signed_time }]
        transform:
          - date_parse: { input_formats: ["yyyy-MM-dd HH:mm:ss", "yyyy/M/d H:mm", "yyyy-MM-dd"], tz: "Asia/Shanghai" }
          - date_format: "yyyy-MM-dd HH:mm:ss"
      - to: { column: 合同归档时间 }
        from: [{ sheet: details, column: archived_time }]
        transform:
          - date_parse: { input_formats: ["yyyy-MM-dd HH:mm:ss", "yyyy/M/d H:mm", "yyyy-MM-dd"], tz: "Asia/Shanghai" }
          - date_format: "yyyy-MM-dd HH:mm:ss"

      # form 长表字段：按 attribute_name 过滤，取 attribute_value
      - to: { column: 需求人 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "需求人" }
        transform:
          - json_parse: {}
          - form_picker_names: {}
          - join_agg: { func: concat, sep: "," }
      - to: { column: 需求人部门 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "需求人部门" }
        transform:
          - json_parse: {}
          - form_picker_names: {}
          - join_agg: { func: concat, sep: "," }
      - to: { column: 履约保证金 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "履约保证金" }
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 质保金 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "质保金" }
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 项目 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "项目" }
        transform:
          - json_parse: {}
          - form_picker_names: {}
          - join_agg: { func: concat, sep: "," }
      - to: { column: 知识产权归属 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "知识产权归属" }
        transform: [trim]
      - to: { column: 是否对外披露我方信息 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "是否对外披露我方信息" }
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }
      - to: { column: 是否有保函 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "是否有保函" }
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }
      - to: { column: 样件采购申请单号 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "样件采购申请单号" }
        transform: [trim]
      - to: { column: 来源系统业务类型 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "来源系统业务类型" }
        transform: [trim]
      - to: { column: 来源系统名称 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "来源系统名称" }
        transform: [trim]
      - to: { column: 来源系统单号 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "来源系统单号" }
        transform: [trim]
      - to: { column: 门店名称 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "门店名称" }
        transform: [trim]
      - to: { column: 承租地址 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "承租地址" }
        transform: [trim]
      - to: { column: 租赁面积 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "租赁面积" }
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: PEA审批记录 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "PEA审批记录" }
        transform: [trim]
      - to: { column: 是否为单方NDA }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "是否为单方NDA" }
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }

  - name: 付款计划
    row_key: payment_id
    row_policy: one_to_many
    source: payments
    mappings:
      - to: { column: 付款时间 }
        from: [{ sheet: payments, column: payment_date }]
        transform:
          - date_parse: { input_formats: ["yyyy-MM-dd", "yyyy/M/d", "yyyy-MM-dd HH:mm:ss"], tz: "Asia/Shanghai" }
          - date_format: "yyyy-MM-dd"
      - to: { column: 付款条件 }
        from: [{ sheet: payments, column: payment_condition }]
        transform: [trim]
      - to: { column: 付款金额 }
        from: [{ sheet: payments, column: payment_amount }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 是否预付 }
        from: [{ sheet: payments, column: prepaid }]
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }
      - to: { column: 付款对象 }
        from: [{ sheet: payments, column: trading_party_id }]
        transform:
          - dict_map: { mapping: party_id_to_name, default: "" }
      - to: { column: 付款说明 }
        from: [{ sheet: payments, column: payment_desc }]
        transform: [trim]

  - name: 预算明细
    row_key: contract_budget_id
    row_policy: one_to_many
    source: contract_budget_list
    mappings:
      - to: { column: 预算年度 }
        from: [{ sheet: contract_budget_list, column: budget_year }]
        transform: [number_parse]
      - to: { column: 预算部门 }
        from: [{ sheet: contract_budget_list, column: budget_department_name }]
        transform: [trim]
      - to: { column: 会计科目 }
        from: [{ sheet: contract_budget_list, column: budget_chart_of_account_code }]
        transform: [trim]
      - to: { column: 业务场景 }
        from: [{ sheet: contract_budget_list, column: budget_business_scene_code }]
        transform: [trim]
      - to: { column: 项目 }
        from: [{ sheet: contract_budget_list, column: budget_project_code }]
        transform: [trim]
      - to: { column: 产品 }
        from: [{ sheet: contract_budget_list, column: budget_product_code }]
        transform: [trim]
      - to: { column: 金额（含税） }
        from: [{ sheet: contract_budget_list, column: budget_taxed_amount }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 税率（%） }
        from: [{ sheet: contract_budget_list, column: tax_rate }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 税额 }
        from: [{ sheet: contract_budget_list, column: tax_amount }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2
      - to: { column: 金额（不含税） }
        from: [{ sheet: contract_budget_list, column: budget_occupied_amount }]
        transform:
          - number_parse: { thousands: ",", decimal: "." }
          - round: 2

  - name: 签约信息拟制表单
    row_key: contract_number
    row_policy: one_to_one
    source: form
    mappings:
      - to: { column: 是否我方先盖章 }
        from: [{ sheet: form, column: attribute_value }]
        where: { attribute_name: "是否我方先盖章" }
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }

  # 相对方：我方与对方两份源表，分别追加写入同一目标Sheet
  - name: 合同关联相对方（通用）
    append: true
    row_key: our_party_id
    row_policy: one_to_many
    source: our_party_list
    mappings:
      - to: { column: 交易方名称 }
        from: [{ sheet: our_party_list, column: our_party_name }]
        transform: [trim]
      - to: { column: 是否我方 }
        from: [{ sheet: our_party_list, column: third_party_platform_valid }]
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }
      - to: { column: 交易方编码 }
        from: [{ sheet: our_party_list, column: our_party_code }]
        transform: [trim]
      - to: { column: 纳税人识别号 }
        from: [{ sheet: our_party_list, column: certification_id }]
        transform: [trim]

  - name: 合同关联相对方（通用）
    append: true
    row_key: counter_party_id
    row_policy: one_to_many
    source: counter_party_list
    mappings:
      - to: { column: 交易方名称 }
        from: [{ sheet: counter_party_list, column: counter_party_name }]
        transform: [trim]
      - to: { column: 是否我方 }
        from: [{ sheet: counter_party_list, column: third_party_platform_valid }]
        transform:
          - boolean_map: { true_values: ["是","Y","true","1"], false_values: ["否","N","false","0"], output_true: "是", output_false: "否" }
      - to: { column: 交易方编码 }
        from: [{ sheet: counter_party_list, column: counter_party_code }]
        transform: [trim]
      - to: { column: 纳税人识别号 }
        from: [{ sheet: counter_party_list, column: certification_id }]
        transform: [trim]

lookups:
  pay_type_dict:
    收入: INCOME
    支出: EXPENSE
  currency_dict: {}
  pricing_type_dict: {}
  validity_type_dict: {}
  # 由实现层动态构建：根据 our_party_list/counter_party_list 将 ID → 名称
  party_id_to_name: { dynamic: true }
```

### 5.1 form表（长表）到模板列映射规则
- **数据结构**：`form` Sheet 至少包含 `contract_number`、`attribute_name`、`attribute_value` 列；同一 `contract_number` 可能存在多条同名 `attribute_name` 行。
- **关联键**：与 `details.contract_number` 进行一对多关联。
- **筛选方式**：在 `mappings` 中使用 `where: { attribute_name: "字段名" }` 精确取值。
- **取值与解析**：
  - 若 `attribute_value` 为纯文本，直接 `trim`。
  - 若为 JSON/数组：先 `json_parse`，再用 `form_picker_names`（提取对象数组中的 `name` 字段）；结果可用 `join_agg` 合并为逗号分隔字符串。
  - 若为日期/数值：使用 `date_parse/format`、`number_parse/round`。
- **多值策略**：同一合同下多行同名属性时，默认 `join_agg` 合并；如需只取首个，可换用 `first` 策略（实现层在 `transformer.py` 支持）。
- **缺失处理**：未命中属性时按 `default` 赋值并记录校验告警；如为必填则进入错误报告。

#### 5.1.1 Transform 执行顺序与匹配规则
- **[顺序]** where 过滤 → 取值集聚合（同一 `contract_number`）→ 逐元素执行 transform 链 → 行级聚合（如 `join_agg`）→ 默认值/必填校验。
- **[where 过滤]** 当前支持等值匹配：`where: { attribute_name: "需求人" }`。可并列多条件（与关系），如 `where: { attribute_name: "需求人", module_name: "基础信息" }`。
- **[聚合范围]** 对 form 属于同一合同号且命中 where 的多行，先汇集为列表，再执行后续算子（如解析 JSON、抽取 name、拼接）。
- **[字段选择]** `form_picker_names` 为便捷写法，等价于 `form_pick: { field: name }`；如需 `user_id`，可写 `form_pick: { field: user_id }`。

#### 5.1.2 常用算子与参数（约定）
- **trim**：去除首尾空白。参数：无。
- **json_parse**：将字符串尝试解析为 JSON（对象/数组/标量）。参数：`on_error: fallback`（默认：保留原文本）。
- **form_pick / form_picker_names（别名）**：
  - 作用：从对象或对象数组中抽取字段。
  - 参数：
    - `field`: 目标键名，如 `name`/`user_id`；或
    - `field_pairs`: 列表，如 `[name, user_id]`，配合 `format_each` 进行组装。
    - `unique: true|false`（默认 true，按出现顺序去重）。
  - 例：`form_pick: { field: name, unique: true }`；`form_pick: { field_pairs: [name, user_id] }`。
- **format_each**：对列表中每个元素格式化为字符串。参数：`template`，如 `"{name}({user_id})"`。
- **join_agg**：将列表聚合为单个字符串。参数：`func: concat`、`sep: ", "`、`unique: true|false`（默认 true 保序去重）。
- **number_parse**：解析数值字符串。参数：`thousands`、`decimal`、`on_error: null|fallback`（默认 null）。
- **round**：四舍五入。参数：`2` 表示保留两位小数。
- **date_parse**：解析日期/时间。参数：`input_formats`（数组）、`tz`（默认 `Asia/Shanghai`）、`on_error`（默认 null）。
- **date_format**：格式化为目标样式。参数：`pattern`，如 `"yyyy-MM-dd"`。
- **first**：取第一个非空值（列表/标量均可）。参数：无。
- **coalesce**：从多个来源中取第一个非空。参数：来源字段路径列表。
- **default**：若为空则写入默认值。参数：`value`。

> 说明：本项目当前阶段不强制使用 `boolean_map/dict_map` 归一化；示例中若出现，仅代表能力，落地时按“原样文本或解析后文本”输出为先。

#### 5.1.3 示例（需求人：name / user_id / 组合）
- **取 name 列表**（写入：`秦臻, 马可欣, …`）
```yaml
- to: { column: 需求人 }
  from: [{ sheet: form, column: attribute_value }]
  where: { attribute_name: "需求人" }
  transform:
    - json_parse: {}
    - form_pick: { field: name, unique: true }
    - join_agg: { func: concat, sep: ", " }
```

- **取 user_id 列表**（写入：`fa38..., 7fba..., …`）
```yaml
- to: { column: 需求人 }
  from: [{ sheet: form, column: attribute_value }]
  where: { attribute_name: "需求人" }
  transform:
    - json_parse: {}
    - form_pick: { field: user_id, unique: true }
    - join_agg: { func: concat, sep: ", " }
```

- **组合输出：姓名(user_id)**（写入：`秦臻(fa38...), 马可欣(7fba...)`）
```yaml
- to: { column: 需求人 }
  from: [{ sheet: form, column: attribute_value }]
  where: { attribute_name: "需求人" }
  transform:
    - json_parse: {}
    - form_pick: { field_pairs: [name, user_id], unique: true }
    - format_each: { template: "{name}({user_id})" }
    - join_agg: { func: concat, sep: ", " }
```

#### 5.1.4 错误处理与审计
- **解析失败**：`json_parse` 失败回退原文本；后续 `form_pick` 在非对象/数组时自动跳过。
- **空值策略**：链路最终为空时，如配置了 `default` 则写默认值；若列为必填（`required: true`）则记录错误并根据运行参数决定是否中断。
- **冲突处理**：同一合同同名属性多条且配置使用 `first` 时，按源出现顺序取首条；使用 `join_agg` 时合并输出，默认保序去重。
- **审计**：输出 `transform_audit.json` 记录每列处理的命中行数、解析/跳过/默认值次数等指标。

#### 5.1.5 性能与大数据量建议
- 仅选择需要的列读取（`usecols`）。
- 对 `form` 先基于 `attribute_name` 预过滤，再与 `details` 关联。
- 对长列表字段的 JSON 解析采用向量化/批量处理，避免逐行 Python 循环；必要时分块处理。


- **字段映射确认（与你的样例一致）**：
  - `details.contract_name` → `历史合同导入.合同名称`
  - `details.pay_type_name` → `历史合同导入.收支类型`
  - `details.remark` → `历史合同导入.合同说明`
  - `form.attribute_value | where attribute_name=需求人` → `历史合同导入.需求人`
  - 其余 `form` 字段按上述模式扩展。

- **内置转换算子（示例）**：
  - 字符串：`trim`、`lower/upper`、`replace`、`substring`、`regex_replace/regex_extract`、`concat`。
  - 映射：`dict_map(mapping, default)`、`boolean_map(true_values, false_values)`、`coalesce`。
  - 日期：`date_parse(input_formats, tz)`、`date_format(pattern)`。
  - 数值：`number_parse(thousands, decimal)`、`round(scale)`、`currency_normalize`。
  - 其他：`unique_id(prefix)`、`join_agg(func, sep)`。

## 6. 多Sheet关联与行策略
- 通过 `join.keys` 定义源/子表的关联字段（如 `contract_id`）。
- `many_to_one`：对明细按主键聚合（sum、first、max/min、concat）。
- `one_to_many`：将主表行与子表行匹配后展开为多行写入目标Sheet。

## 7. 模板写入策略（保留样式/下拉/校验）
- 以模板为底稿用 `openpyxl` 打开，只写单元格 `value`，不重建Sheet与样式。
- 写入坐标通过“目标列名 → 列索引”映射（首行表头定位）。
- 支持同一目标Sheet多次写入（多来源合并），通过 `append: true` 追加行，不清空已写入数据。
- 空值与默认值：尊重 `default`，否则留空；严格遵循模板必填列校验。

## 8. 校验与异常处理
- 字段级：`required`、`allowed_values`、`pattern`、`length`。
- 行级：`unique(row_key)`、`foreign_key_exists`（跨Sheet引用）。
- 结果：输出 `errors.xlsx`/`errors.csv`，包含Sheet/行/列/错误类型/原值/建议处理。
- 容错：提供 `--fail-on-error`（遇错终止）与 `--continue-on-error`（跳过错误写入合格行）。

## 9. 日志与审计
- 关键日志：输入行数、过滤/聚合/展开后行数、缺失映射、字典未命中、写入成功行数、耗时。
- 审计工件：`transform_audit.json`（配置摘要 + 指标 + 版本信息）。

## 10. 性能与可扩展性
- 读：`pandas.read_excel` 加速解析；字段裁剪仅读必要列。
- 算：优先向量化转换；聚合使用 `groupby`；必要时分批处理子表。
- 写：`openpyxl` 顺序写入；大表按块写入避免内存峰值。

## 11. 配置管理与版本
- `mapping.yaml` 可多版本（按模板版本/来源划分），文件名/目录区分；在审计中记录哈希与版本。
- 通过 `cli.py` 的 `--mapping` 参数切换配置。

## 12. CLI 使用示例
```
python -m feishu_contracts.cli \
  --source "./data/contracts.xlsx" \
  --template "./templates/合同导入模板.xlsx" \
  --mapping "./feishu_contracts/transform/mapping.yaml" \
  --out "./out/合同导入模板_填充.xlsx" \
  --continue-on-error
```

## 13. 测试策略
- 单元：转换算子、字典映射、日期/数值解析、校验器。
- 集成：端到端转换，覆盖多Sheet关联/展开/聚合。
- 回归：关键模板版本变更的基线样例对比。

## 14. 风险与边界
- 源数据缺少稳定主键/格式异常 → 需前置清洗或补充规则。
- 模板频繁变更 → 依赖配置版本化与冒烟测试。
- 超大数据量 → 可能需要分文件/分批处理或切换至流式写入策略。

## 15. 里程碑
- M1：确认样例与规则，提交 `mapping.yaml` 初版（1-2天）。
- M2：完成 `transformer.py` 骨架与核心算子（2-3天）。
- M3：集成现有拉取脚本并产出可导入文件（1天）。
- M4：完善校验、日志与测试（1-2天）。

## 16. 输入/输出清单
- 输入：源多Sheet Excel、导入模板Excel、`mapping.yaml`。
- 输出：可导入Excel（保留模板格式）、错误报告、审计日志。

## 17. 架构图
```mermaid
flowchart LR
  subgraph Src[Sources]
    A[contracts.xlsx\n(details, form, payments,\ncontract_budget_list, our_party_list, counter_party_list)]
    Tmpl[合同导入模板.xlsx]
  end

  A --> R[Reader\n(pandas.read_excel)]
  M[mapping.yaml\n- join.keys(contract_number)\n- row_policy(one_to_one/one_to_many)\n- mappings/where/transform] --> X[Transformer]
  R --> X
  X --> V[Validators\n(required/unique/fk_exists)]
  V --> W[Writer\n(openpyxl, append保留样式/下拉)]
  Tmpl --> W
  W --> O[合同导入模板_填充.xlsx]
  V --> E[errors.xlsx/csv]
  X --> J[transform_audit.json]
```
