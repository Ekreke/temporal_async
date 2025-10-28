# Activity 节点文档

本文档详细介绍了N8N-Temporal项目中实现的所有Activity节点。

## 🚨 重要变更通知

**节点迁移**: 原有的 `if` 和 `switch` 节点已经合并为统一的 **Conditional Node** (`n8n-nodes-base.conditional`)。

- ✅ **推荐**: 使用新的统一条件节点 (`n8n-nodes-base.conditional`)
- ❌ **已废弃**: 原有的switch节点 (`n8n-nodes-base.switch`) 已删除
- ⚠️ **兼容**: 原有的if节点 (`n8n-nodes-base.if`) 仍保留但建议迁移

### 迁移指南

如果您之前使用switch节点，请按以下方式迁移：

```json
// 旧的switch节点配置
{
  "nodeType": "n8n-nodes-base.switch",
  "rules": [...]
}

// 新的统一条件节点配置
{
  "nodeType": "n8n-nodes-base.conditional",
  "nodeType": "switch",
  "conditions": [...]
}
```

## 概述

本节点的实现基于统一的Activity接口，支持在Temporal工作流中执行各种操作。每个节点都继承自BaseActivity，提供统一的功能如表达式评估、日志记录、错误处理等。

## 节点类型

### 1. 开始节点 (Start Node)

**节点类型**: `n8n-nodes-base.start`

**描述**: 工作流的第一个节点，负责收拢所有节点的公共参数并传递给后续节点。

**功能特性**:
- 必须是工作流的第一个节点
- 接收并存储全局参数配置
- 通过WorkflowContext向后续节点传递参数
- 支持为每个节点配置特定的参数

**参数配置**:
```json
{
  "globalParameters": {
    "node1": {
      "param1": "value1",
      "param2": "value2"
    },
    "node2": {
      "paramA": "valueA",
      "paramB": "valueB"
    }
  }
}
```

**使用示例**:
```go
// 创建开始节点
startNode := node.NewStartNodeActivity()

// 配置输入
input := &node.ActivityInput{
    NodeID:   "start-1",
    NodeName: "Start Node",
    NodeType: "n8n-nodes-base.start",
    Parameters: map[string]interface{}{
        "globalParameters": map[string]interface{}{
            "python-node": map[string]interface{}{
                "timeout": 30,
                "requirements": []string{"requests"},
            },
            "conditional-node": map[string]interface{}{
                "mode": "strict",
            },
        },
    },
}
```

---

### 2. 结束节点 (End Node)

**节点类型**: `n8n-nodes-base.end`

**描述**: 工作流的最后一个节点，负责展示工作流执行结果。

**功能特性**:
- 必须是工作流的最后一个节点
- 支持两种结果展示模式：
  - `last`: 只展示最后一个节点的输出结果
  - `all`: 展示所有节点的执行结果汇总
- 提供详细的执行统计信息

**参数配置**:
```json
{
  "resultMode": "all" // 可选值: "last", "all"
}
```

**输出结果**:
```json
{
  "success": true,
  "message": "工作流执行完成",
  "resultMode": "all",
  "result": {
    "node1": {...},
    "node2": {...},
    "statistics": {
      "totalNodes": 2,
      "userNodes": 2,
      "executionTime": "2023-01-01T12:00:00Z07:00"
    }
  },
  "nodeCount": 2,
  "completedAt": "2023-01-01T12:00:00Z07:00"
}
```

---

### 3. 变量节点 (Variable Node)

**节点类型**: `n8n-nodes-base.variable`

**描述**: 用于存储和管理工作流变量，提供并发安全的全局变量存储功能。

**功能特性**:
- 支持多种操作：设置、获取、删除、清空变量
- 并发安全的变量存储
- 支持多种覆盖模式：overwrite、merge、skip
- 支持全局和局部作用域
- 变量数据自动传递给所有后续节点

**参数配置**:
```json
{
  "operation": "set",        // 操作类型: "set", "get", "delete", "clear"
  "variables": {             // 要操作的变量
    "var1": "value1",
    "var2": 42
  },
  "overwriteMode": "overwrite", // 覆盖模式: "overwrite", "merge", "skip"
  "variablesScope": "global"    // 作用域: "global", "local"
}
```

**操作类型说明**:
- **set**: 设置变量
- **get**: 获取变量
- **delete**: 删除指定变量
- **clear**: 清空所有变量

**覆盖模式说明**:
- **overwrite**: 直接覆盖现有值
- **merge**: 对map类型进行合并，其他类型直接覆盖
- **skip**: 如果变量已存在则跳过，否则设置

**使用示例**:
```go
// 设置变量
variableNode := node.NewVariableNodeActivity()
input := &node.ActivityInput{
    NodeType: "n8n-nodes-base.variable",
    Parameters: map[string]interface{}{
        "operation": "set",
        "variables": map[string]interface{}{
            "user_id": "12345",
            "session_data": map[string]interface{}{
                "login_time": "2023-01-01T12:00:00Z",
                "user_agent": "Mozilla/5.0..."
            }
        },
    },
}
```

---

### 4. 统一条件节点 (Conditional Node)

**节点类型**: `n8n-nodes-base.conditional`

**描述**: 统一的条件判断节点，支持if条件和switch分支逻辑，合并了原有的if和switch节点功能。

**功能特性**:
- 支持if和switch两种逻辑模式
- 支持复杂的条件表达式（AND/OR逻辑）
- 支持多种操作符：字符串、数字、日期时间比较
- 支持正则表达式匹配
- 支持默认分支处理

**参数配置**:
```json
{
  "nodeType": "if",                    // 节点类型: "if", "switch"
  "conditions": [                     // 条件规则列表
    {
      "id": "rule1",
      "name": "检查用户状态",
      "outputPath": "active-branch",
      "enabled": true,
      "logicOperator": "AND",         // 逻辑操作符: "AND", "OR"
      "conditions": [
        {
          "leftValue": "status",
          "operator": "equals",        // 操作符
          "rightValue": "active",
          "caseSensitive": true
        }
      ]
    }
  ],
  "defaultBranch": "default",         // 默认分支
  "evaluateMode": "first"             // 评估模式: "all", "first", "any"
}
```

**支持的操作符**:

**字符串操作符**:
- `equals`, `==`: 等于
- `not_equals`, `!=`: 不等于
- `contains`: 包含
- `not_contains`: 不包含
- `starts_with`: 开头是
- `ends_with`: 结尾是
- `regex`: 正则表达式匹配
- `not_regex`: 正则表达式不匹配
- `is_empty`: 为空
- `is_not_empty`: 不为空

**数字操作符**:
- `greater_than`, `>`: 大于
- `greater_than_or_equal`, `>=`: 大于等于
- `less_than`, `<`: 小于
- `less_than_or_equal`, `<=`: 小于等于

**使用示例**:
```go
// if模式条件判断
conditionalNode := node.NewConditionalNodeActivity()
input := &node.ActivityInput{
    NodeType: "n8n-nodes-base.conditional",
    InputData: map[string]interface{}{
        "status": "active",
        "score": 85,
    },
    Parameters: map[string]interface{}{
        "nodeType": "if",
        "conditions": []interface{}{
            map[string]interface{}{
                "id": "premium_check",
                "name": "高级用户检查",
                "outputPath": "premium-branch",
                "enabled": true,
                "logicOperator": "AND",
                "conditions": []interface{}{
                    map[string]interface{}{
                        "leftValue": "status",
                        "operator": "equals",
                        "rightValue": "active",
                    },
                    map[string]interface{}{
                        "leftValue": "score",
                        "operator": "greater_than",
                        "rightValue": 80,
                    },
                },
            },
        },
        "defaultBranch": "regular-branch",
    },
}
```

---

### 5. Python Docker节点 (Python Docker Node)

**节点类型**: `n8n-nodes-base.pythonDocker`

**描述**: 在Docker容器中安全执行Python代码的节点，提供隔离的执行环境。

**功能特性**:
- Docker容器隔离执行，确保系统安全
- 支持自定义Docker镜像
- 支持Python依赖包管理
- 支持环境变量配置
- 支持执行超时和重试机制
- 自动生成包含输入数据的Python脚本
- 安全限制：禁用网络访问，限制资源使用

**参数配置**:
```json
{
  "code": "result = input_data['value'] * 2", // Python代码
  "dockerImage": "python:3.11-slim",          // Docker镜像
  "timeoutSeconds": 60,                       // 超时时间（秒）
  "maxRetries": 3,                           // 最大重试次数
  "workingDir": "/app",                      // 工作目录
  "environmentVars": {                       // 环境变量
    "API_KEY": "your-api-key",
    "DEBUG": "true"
  },
  "requirements": [                          // Python依赖包
    "requests",
    "pandas",
    "numpy"
  ],
  "inputData": {                             // 输入数据
    "value": 21,
    "name": "test"
  }
}
```

**安全特性**:
- 网络访问禁用（--network=none）
- 内存限制（--memory=512m）
- CPU限制（--cpus=1）
- 容器自动清理（--rm）

**Python代码规范**:
- 代码可以访问`input_data`变量（包含输入数据）
- 需要设置`result`变量作为返回结果
- 支持导入标准库和已安装的第三方包
- 异常会被自动捕获并返回错误信息

**使用示例**:
```go
// Python代码执行
pythonNode := node.NewPythonDockerNodeActivity()
input := &node.ActivityInput{
    NodeType: "n8n-nodes-base.pythonDocker",
    InputData: map[string]interface{}{
        "numbers": []interface{}{1, 2, 3, 4, 5},
    },
    Parameters: map[string]interface{}{
        "code": `
import statistics
numbers = input_data['numbers']
result = {
    'count': len(numbers),
    'sum': sum(numbers),
    'average': statistics.mean(numbers),
    'max': max(numbers),
    'min': min(numbers)
}
`,
        "dockerImage": "python:3.11-slim",
        "timeoutSeconds": 30,
        "requirements": []interface{}{},
    },
}
```

**输出结果**:
```json
{
  "success": true,
  "stdout": "Python脚本输出",
  "result": {
    "count": 5,
    "sum": 15,
    "average": 3.0,
    "max": 5,
    "min": 1
  }
}
```

---

## 节点创建

所有节点都通过直接实例化的方式创建：

```go
// 创建表达式评估器
express := NewExpressionEvaluator(nil)

// 直接创建节点实例
startNode := NewStartNodeActivity(express)
endNode := NewEndNodeActivity(express)
variableNode := NewVariableNodeActivity(express)
conditionalNode := NewConditionalNodeActivity(express)
pythonDockerNode := NewPythonDockerNodeActivity(express)
```

## 通用接口

所有节点都实现以下统一接口：

```go
type Activity interface {
    GetNodeInfo() *ActivityInfo           // 获取节点信息
    Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error)  // 执行节点逻辑
    ValidateInput(input *ActivityInput) error  // 验证输入参数
    GetLogger(ctx context.Context) log.Logger  // 获取日志记录器
}
```

## 错误处理

所有节点都提供统一的错误处理机制：

1. **输入验证错误**: 在执行前验证参数格式和有效性
2. **执行错误**: 捕获执行过程中的异常并返回详细错误信息
3. **超时处理**: 支持执行超时机制，防止无限等待
4. **重试机制**: 支持失败重试，提高执行可靠性

## 工作流上下文

节点通过WorkflowContext共享数据：

- **__global__**: 存储全局参数（由开始节点设置）
- **__variables__**: 存储变量数据（由变量节点管理）
- **节点数据**: 存储每个节点的执行结果

## 表达式评估

支持n8n风格的表达式评估：

- `$json.field`: 引用当前节点的JSON数据
- `$('NodeName').field`: 引用其他节点的数据
- `$now`: 当前时间
- `$today`: 当前日期
- 数学表达式和函数调用

## 测试

每个节点都包含完整的单元测试：

```bash
# 运行所有节点测试
go test ./node -v

# 运行特定节点测试
go test ./node -run TestStartNode -v
go test ./node -run TestEndNode -v
go test ./node -run TestVariableNode -v
go test ./node -run TestConditionalNode -v
go test ./node -run TestPythonDockerNode -v
```

## 最佳实践

1. **开始节点**: 始终作为工作流的第一个节点，配置所有全局参数
2. **结束节点**: 始终作为工作流的最后一个节点，收集执行结果
3. **变量节点**: 用于存储需要在多个节点间共享的数据
4. **条件节点**: 使用统一的ConditionalNode替代原有的if/switch节点
5. **Python节点**: 复杂计算和数据处理使用Python Docker节点确保安全性

## 版本信息

- **开始节点**: v1.0.0
- **结束节点**: v1.0.0
- **变量节点**: v1.0.0
- **统一条件节点**: v1.0.0
- **Python Docker节点**: v1.0.0

所有节点都基于Go 1.24.6和Temporal SDK v1.37.0构建。