# 能力调度节点

## 定义
- 类型：`*.abilitySchedule`
- 作用：向后端任务调度服务提交能力任务并获取结果，支持阻塞和流式输出。

## 节点定义示例
```json
{
  "id": "ability-node",
  "name": "能力调度",
  "type": "*.abilitySchedule",
  "version": 1,
  "is_remote": true,
  "parameters_source": "",
  "parameters": {
    "pb_file": "mapper/task/v2/task.proto",
    "req_message": "mapper.task.v2.DomainResolveRequest",
    "rsp_message": "mapper.task.v2.DomainResolveResponse"
  }
}
```

### 字段说明
- `id`、`name`、`version`：通用字段。
- `type`：`*.abilitySchedule`。
- `is_remote`：建议为 `true`，启用流式输出；为 `false` 时使用阻塞轮询。
- `parameters_source`：远程节点可选，允许从上下文动态填充 `parameters`（如 `$global...`），为空时使用静态参数。
- `parameters`：见下。

## 参数
- `pb_file`：必填，协议文件路径或可下载地址。
- `req_message`：必填，请求消息类型全名。
- `rsp_message`：必填，响应消息类型全名。

## 实现逻辑
- 校验参数并初始化 PB 转换器。
- 首次执行创建任务，生成唯一标识并提交。
- 阻塞模式：轮询任务列表，收集全部结果后返回。
- 流式模式：周期查询并对每批增量结果发送信号至工作流，直至任务完成。

## 行为细节
- 心跳维持执行状态；连续错误达到上限时终止。
- 结果以 `Any` PB 返回，转换为 JSON 再写入输出。
- 流式模式会将增量结果通过工作流信号推送到引擎。

## 示例
```json
{
  "parameters": {
    "pb_file": "mapper/task/v2/task.proto",
    "req_message": "mapper.task.v2.DomainResolveRequest",
    "rsp_message": "mapper.task.v2.DomainResolveResponse"
  },
  "is_remote": true
}
```

## 错误与边界
- 缺少必需参数时校验失败。
- 后端服务不可用或结果为空时记录并跳过。
- 流式信号发送失败时返回错误。

## 集成点
- 与工作流引擎结合，阻塞模式用于批量完成；流式模式用于实时推动下游节点执行。
