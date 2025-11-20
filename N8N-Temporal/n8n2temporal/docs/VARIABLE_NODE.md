# 变量节点

## 定义
- 类型：`*.variable`
- 作用：解析参数中的表达式，将解析结果写入工作流变量上下文 `__variables__`。

## 节点定义示例
```json
{
  "id": "variable-node",
  "name": "变量节点",
  "type": "*.variable",
  "version": 1,
  "is_remote": false,
  "parameters_source": "",
  "parameters": {
    "operation": "set",
    "overwriteMode": "overwrite",
    "variables": {
      "profile.domain": "$('域名解析').domain",
      "cfg.flag": "$global.cfg.flag"
    }
  }
}
```

### 字段说明
- `id`：节点唯一标识，同一工作流内不可重复。
- `name`：节点名称，用于画布展示与上下文引用。
- `type`：节点类型，变量节点使用 `*.variable`。
- `version`：节点版本号，节点实现更新时需递增。
- `is_remote`：是否远端执行；变量节点必须为本地执行，必须为 `false`。
- `parameters_source`：参数来源（仅远程节点可用）。变量节点必须为空字符串。
- `parameters`：节点运行所需的业务参数，详见下文参数说明。

## 参数
- `operation`：变量操作类型
  - `set`：按键写入解析后的值
  - `delete`：删除指定键（未实现）
  - `clear`：清空所有变量
- `overwriteMode`：写入模式
  - `overwrite`：直接覆盖
  - `skip`：键已存在则跳过
  - `merge`：旧值和新值均为 `map` 时进行浅合并
- `variables`：待写入的键值对，键与值均支持表达式与嵌套结构

## 表达式解析规则
- 键与值统一使用评估器解析
- 支持：
  - `$var.xxx`：读取 `__variables__` 中的值
  - `$global.xxx`：读取 `__global__` 中的值
  - `$('node').a.b.c`：读取指定节点最近一次执行数据的路径值
  - `$`：表示上个节点的整条输入数据
  - 非表达式字符串与常量按原样使用
- 嵌套解析：`map` 的键和值、数组元素中的表达式递归解析

## 实现逻辑
- 解析参数，设置默认值
- 对 `variables` 的所有键与值递归解析，生成规范化后的键值对
- 根据 `overwriteMode` 写入到 `__variables__`
  - `overwrite`：设置键的值
  - `skip`：存在则不修改
  - `merge`：旧值与新值均为 `map` 时，进行浅层键合并
- 返回 `__variables__` 的完整快照

## 行为细节
- 键解析必须得到非空字符串
- `merge` 仅在旧值与新值均为 `map[string]interface{}` 时生效
- 写入通过 `WorkflowContext.SetNodeDataKV` 支持点分路径构建嵌套

## 示例
- 写入域名与标志：
```json
{
  "operation": "set",
  "overwriteMode": "overwrite",
  "variables": {
    "profile.domain": "$('域名解析').domain",
    "cfg.flag": "$global.cfg.flag"
  }
}
```

- 动态键与值：
```json
{
  "operation": "set",
  "overwriteMode": "overwrite",
  "variables": {
    "$var.k_user": "$('域名解析').domain"
  }
}
```

## 错误与边界
- 键解析为非字符串或空字符串将返回错误
- 路径解析失败时抛出错误（缺失字段、类型不符、索引越界）
- 在 `skip` 模式下，已存在的键保持不变
- 在 `merge` 模式下，非 `map` 类型跳过合并

## 集成点
- 评估器：`node/expression.go` 统一解析表达式
- 上下文：`WorkflowContext` 负责节点数据与变量存储
- 结果输出：返回变量上下文的完整快照，供后续节点引用
