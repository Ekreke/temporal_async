# Python Docker 节点

## 定义
- 类型：`*.code`
- 作用：在隔离的 Docker 容器中执行用户提供的 Python 代码，返回结构化结果。

## 节点定义示例
```json
{
  "id": "code-node",
  "name": "Python",
  "type": "*.code",
  "version": 1,
  "is_remote": false,
  "parameters_source": "",
  "parameters": {
    "code": "return {'a': 1}",
    "dockerImage": "python:3.11-slim",
    "timeoutSeconds": 30,
    "maxRetries": 2,
    "workingDir": "/app",
    "environmentVars": {"ENV": "prod"},
    "requirements": ["requests"],
    "inputData": {"key": "value"}
  }
}
```

### 字段说明
- `id`、`name`、`version`：通用字段。
- `type`：`*.code`。
- `is_remote`：必须为 `false`。
- `parameters_source`：仅远程节点可用，此节点必须为空。
- `parameters`：见下。

## 参数
- `code`：必填，Python 代码主体，需返回字典作为 `reqArgs`。
- `dockerImage`：Docker 镜像，默认 `python:3.11-slim`。
- `timeoutSeconds`：执行超时秒数，默认 `60`，范围 `1-300`。
- `maxRetries`：最大重试次数，默认 `3`，范围 `0-10`。
- `workingDir`：工作目录，默认 `/app`。
- `environmentVars`：环境变量字典。
- `requirements`：Python 依赖列表。
- `inputData`：静态输入映射，将与上游传入数据共同提供给脚本。

## 实现逻辑
- 解析并校验参数，填充默认值。
- 生成 Python 脚本，将 `_input_data` 与 `_var_data` 注入执行环境。
- 调用 Docker 运行脚本，支持超时与重试策略。
- 解析标准输出为结构化结果。

## 行为细节
- 不依赖网络，容器默认禁用网络，提高安全性。
- `environmentVars` 将通过 `-e` 注入容器环境。
- 脚本模板会包装异常并以 JSON 输出错误结构。

## 示例
```json
{
  "parameters": {
    "code": "return {'user': _input_data.get('json',{}).get('user','guest')}",
    "timeoutSeconds": 30,
    "requirements": ["requests"]
  }
}
```

## 错误与边界
- Docker 不可用时返回错误。
- 参数类型不正确时返回校验错误。
- 超时与重试耗尽时返回最终错误并包含 `stderr`。

## 集成点
- 与变量节点结合，将脚本计算结果写入上下文供后续节点消费。
