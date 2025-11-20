# 开始节点

## 定义
- 类型：`*.start`
- 作用：收拢节点 `parameters` 与首个 `inputData`，并按优先级合并为初始上下文。

## 节点定义示例
```json
{
  "id": "start-node",
  "name": "开始节点",
  "type": "*.start",
  "version": 1,
  "is_remote": false,
  "parameters_source": "",
  "parameters": {
    "init": "ok",
    "window": 10
  }
}
```

### 字段说明
- `id`：节点唯一标识。
- `name`：节点名称。
- `type`：节点类型，开始节点使用 `*.start`。
- `version`：节点版本号。
- `is_remote`：必须为 `false`。
- `parameters_source`：仅远程节点可用，开始节点必须为空字符串。
- `parameters`：初始静态参数，将与输入数据合并后传递。

## 参数
- 无额外业务参数，直接使用 `parameters` 与首个输入数据进行合并。

## 合并规则
- 若存在 `inputData[0]`，以其键值覆盖 `parameters` 中相同键。
- 输出为单条数据的快照，用于后续节点消费。

## 实现逻辑
- 复制 `parameters` 到结果。
- 检查 `inputData[0]` 并覆盖同名键。
- 返回合并后的结果。

## 行为细节
- 当 `parameters` 为空时，以输入为准。
- 当无输入数据时，直接返回 `parameters`。

## 示例
```json
{
  "parameters": {"a": 1, "b": 2},
  "inputData[0]": {"b": 3, "c": 4},
  "result": {"a": 1, "b": 3, "c": 4}
}
```

## 错误与边界
- 输入为空时不报错，返回空结果或 `parameters`。

## 集成点
- 返回的快照会进入工作流上下文，供后续节点通过表达式读取。
