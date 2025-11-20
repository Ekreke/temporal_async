# Activity 基类与节点结构（base.go）

## 概述
- 作用：定义节点通用接口、标准输入输出结构与基类公共能力（校验、日志、执行包装）。
- 位置：`node/base.go`

## 节点接口与结构
- `Activity` 接口：
  - `GetNodeInfo()`：返回节点元信息。
  - `ValidateInput(input)`：校验节点输入参数。
  - `GetLogger(ctx)`：获取日志记录器。
- `WkFLowNode`：节点定义结构
  - `id`：唯一标识
  - `name`：展示名称
  - `type`：节点类型（如 `*.start`、`*.variable`）
  - `parameters`：节点业务参数
  - `parameters_source`：参数来源，仅远程节点可用
  - `version`：版本号（递增）
  - `is_remote`：是否远程执行
- `ActivityInput`：节点运行输入
  - `inputData`：上游传入的数据列表
  - `signalInput`：工作流信号数据（流式模式使用）
  - `workflowId`、`executionId`：工作流标识
  - `express`：表达式评估器
  - `node`：节点元信息
- `ActivityOutput`：节点运行输出
  - `nodeId`、`nodeName`、`nodeType`：节点标识信息
  - `success`：是否成功
  - `data`：输出数据
  - `error`：错误描述
  - `processedAt`：处理时间
  - `metadata`：信号元数据

## BaseActivity 公共能力
- `ValidateInput(input)`：校验输入不为空与节点类型必填。
- `GetLogger(ctx)`：返回活动上下文日志。
- `GetExpressionEvaluator()`：获取解析器实例。
- `CreateSuccessOutput(input, execRes)`：构建成功输出，附加节点与元数据信息。
- `CreateErrorOutput(input, err)`：构建失败输出。
- `ExecuteWithExecuteTiming(ctx, input, executeFunc)`：统一执行包装，记录耗时与日志，失败返回错误输出。

## 设计约束
- 日志器依赖 Temporal Activity 上下文；在普通单元测试中不要直接调用基类执行包装（否则会触发上下文错误）。
- `parameters_source` 仅在远程节点使用；本地节点必须为空。

## 示例
```json
{
  "node": {"id": "start", "name": "开始", "type": "*.start", "version": 1, "is_remote": false, "parameters": {}},
  "input": {"inputData": [{"json": {"a": 1}}], "express": "ExpressionEvaluator"},
  "output": {"success": true, "data": [{"json": {"a": 1}}]}
}
```

## 错误与边界
- 输入为空或节点类型为空时报错。
- 执行包装中的具体逻辑错误会被记录并返回错误输出结构。

## 与引擎的关系
- 所有节点实现 `Activity`，并通过 `BaseActivity` 获得一致的校验与输出行为；引擎只需调用统一入口方法。
