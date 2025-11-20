# 表达式评估器（expression.go）

## 概述
- 作用：统一解析节点中的动态表达式，访问变量、全局参数、指定节点数据、输入数据与工作流信息。
- 位置：`node/expression.go`

## 关键类型
- `WorkflowContext`：工作流上下文，保存各节点的最新数据快照。
  - `SetNodeData(nodeName, data)`：覆盖设置节点快照。
  - `SetNodeDataKV(nodeName, key, value)`：点分路径递归设置键值。
  - `GetNodeData(nodeName)`：获取节点快照。
  - `GetNodeDataKV(nodeName, key)`：点分路径递归读取键值。
  - `GetAllContext()`：返回完整上下文。
- `ExpressionEvaluator`：表达式解析器，核心方法：
  - `EvaluateExpression(expression, inputData)`：解析并返回表达式结果。
  - `resolveNodeReference(expression)`：解析 `$('Node').path` 形式的节点引用。
  - `extractFieldValue(fieldPath, inputData)`：通用的字段路径提取器，支持数组索引。

## 表达式语法
- 变量上下文：`$var.xxx`
- 全局上下文：`$global.xxx`
- 节点引用：`$('NodeName').a.b.c`
- 输入数据：`$path.to.field` 或 `"$"`（整个输入数据）
- 工作流信息：`$workflow.id`、`$workflow.name`
- 时间：`$now`（`YYYY-MM-DD HH:MM:SS`）、`$today`（`YYYY-MM-DD`）、`$timestamp`（Unix 秒）
- 字符串字面量：`"str"` 或 `'str'`

## 路径访问与数组索引
- 点分路径：`a.b.c`，逐层进入对象。
- 数组访问：`arr[0]`，支持在路径中混合使用，如 `arr[0].field`。
- 错误处理：当字段缺失、类型不符或索引越界时返回详细错误。

## 实现要点
- `EvaluateExpression` 使用前缀与模式匹配选择解析分支，未知表达式原样返回（字符串）。
- `resolveNodeReference` 使用正则抽取节点名与字段路径，从 `WorkflowContext` 获取对应数据并走通用提取器。
- `extractFieldValue` 统一处理对象与数组路径，空路径返回整个输入数据；错误场景明确报错。

## 设计约束
- 解析器不持久化状态，只依赖注入的 `WorkflowContext`。
- 未找到节点数据时返回原表达式，避免硬错误阻断流程。

## 示例
```json
{
  "context": {
    "__variables__": {"user": {"id": 1}},
    "__global__": {"cfg": {"flag": true}},
    "域名解析": {"domain": "ex.com"}
  },
  "expressions": [
    "$var.user.id",
    "$global.cfg.flag",
    "$('域名解析').domain",
    "$json.user",
    "$workflow.id",
    "$now",
    "$today",
    "$timestamp"
  ]
}
```

## 边界与错误
- 仅支持简单路径与数字索引；不支持复杂过滤或脚本。
- 对缺失字段、非对象/非数组的访问会返回详细错误消息。

## 与节点的关系
- 变量节点、条件节点等均通过评估器解析键和值，确保表达式语法一致。
