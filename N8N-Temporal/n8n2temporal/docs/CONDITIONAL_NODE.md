# 条件节点

## 定义
- 类型：`*.conditional`
- 作用：根据配置的条件表达式计算分支路径，支持 AND/OR 逻辑与默认分支。

## 节点定义示例
```json
{
  "id": "conditional-node",
  "name": "条件判断",
  "type": "*.conditional",
  "version": 1,
  "is_remote": false,
  "parameters_source": "",
  "parameters": {
    "defaultBranch": "default",
    "conditions": [
      {
        "id": "rule-1",
        "name": "域名匹配",
        "outputPath": "ok",
        "enabled": true,
        "logicOperator": "AND",
        "conditions": [
          {"leftValue": "$('域名解析').domain", "operator": "equals", "rightValue": "ex.com", "caseSensitive": true}
        ]
      }
    ]
  }
}
```

### 字段说明
- `id`、`name`、`version`：通用字段。
- `type`：`*.conditional`。
- `is_remote`：必须为 `false`。
- `parameters_source`：仅远程节点可用，此节点必须为空。
- `parameters`：包含 `defaultBranch` 与 `conditions`。

## 参数
- `defaultBranch`：匹配失败时的输出分支名。
- `conditions`：规则数组，每条包含：
  - `id`、`name`、`outputPath`、`enabled`、`logicOperator`
  - `conditions`：表达式数组，字段为 `leftValue`、`operator`、`rightValue`、`caseSensitive`

## 表达式解析规则
- 使用评估器解析以 `$` 开头的表达式：
  - `$var.xxx`、`$global.xxx`、`$('node').xxx`
- 非表达式字符串按原样处理，亦支持从输入数据按路径提取。

## 实现逻辑
- 解析参数，构建规则集。
- 遍历启用规则，评估表达式数组并按 `logicOperator` 汇总。
- 首个命中的规则输出其 `outputPath`，否则输出 `defaultBranch`。

## 行为细节
- `logicOperator` 支持 `AND` 与 `OR`，默认 `AND`。
- 若规则无表达式或被禁用，视为不命中。

## 示例
```json
{
  "parameters": {
    "defaultBranch": "default",
    "conditions": [
      {"id": "r1", "name": "匹配域名", "outputPath": "ok", "enabled": true, "logicOperator": "AND", "conditions": [{"leftValue": "$('域名解析').domain", "operator": "equals", "rightValue": "ex.com"}]}
    ]
  }
}
```

## 错误与边界
- `conditions` 非数组时返回校验错误。
- 表达式解析失败、路径缺失或类型不符时返回错误。

## 集成点
- 输出分支将用于引擎的下一跳选择，与连接关系图结合实现动态路由。
