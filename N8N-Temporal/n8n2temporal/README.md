# N8N-Temporal 工作流转换引擎

[![Go Version](https://img.shields.io/badge/Go-1.24.6+-blue.svg)](https://golang.org)
[![Temporal](https://img.shields.io/badge/Temporal-SDK-orange.svg)](https://temporal.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

N8N-Temporal 是一个将 n8n 工作流转换为 Temporal 工作流的 Go 项目。该项目提供了一个强大的桥接层，支持将 n8n 的 JSON 格式工作流定义转换为可执行的 Temporal 工作流，并提供了丰富的节点库来处理各种业务逻辑。

## 🚨 v2.0.0 重大更新

### ✨ 新增核心节点
- ✅ **开始节点**: 工作流入口，配置全局参数
- ✅ **结束节点**: 工作流出口，展示执行结果
- ✅ **变量节点**: 并发安全的全局变量存储
- ✅ **统一条件节点**: 合并if/switch逻辑，支持复杂分支
- ✅ **Python Docker节点**: 安全容器化Python执行

### 🔄 架构优化
- ❌ **删除废弃节点**: Switch节点、旧版IF节点、旧版Python节点
- ✅ **统一接口**: 所有节点使用统一的Activity接口
- ✅ **向后兼容**: 保持对旧版本节点类型的支持

## 🌟 核心特性

### 🚀 统一节点架构
- **标准化接口**：所有节点都实现统一的 `Activity` 接口
- **动态类型系统**：支持运行时节点类型推断和验证
- **可扩展设计**：轻松添加新的节点类型和功能
- **向后兼容**：保持与旧版本接口的兼容性

### 📊 完整的节点类型支持

| 节点类型 | 功能描述 | 状态 |
|---------|---------|------|
| **开始节点** | `n8n-nodes-base.start` | ✅ 核心节点 |
| **结束节点** | `n8n-nodes-base.end` | ✅ 核心节点 |
| **变量节点** | `n8n-nodes-base.variable` | ✅ 核心节点 |
| **统一条件节点** | `n8n-nodes-base.conditional` | ✅ 核心节点 |
| **Python Docker节点** | `n8n-nodes-base.pythonDocker` | ✅ 核心节点 |
| **域名解析节点** | `n8n-nodes-base.domainResolve` | ✅ 兼容节点 |
| **自定义节点** | `CUSTOM.*` 前缀 | ✅ 兼容节点 |

### 🎯 高级工作流引擎
- **拓扑排序算法**：自动确定节点执行顺序
- **循环依赖检测**：智能检测并报告循环依赖
- **并行执行优化**：无依赖节点自动并行执行
- **数据流管理**：智能合并和传递节点间数据
- **上下文追踪**：完整的执行上下文和状态管理

## 🏗️ 项目架构

```
n8n2temporal/
├── node/                    # 🧩 节点系统
│   ├── base.go             # 🏛️ 基础Activity接口和实现
│   ├── start_node.go       # 🚀 开始节点
│   ├── end_node.go         # ⏹️ 结束节点
│   ├── variable_node.go    # 🗃️ 变量节点
│   ├── conditional_node.go # 🔀 统一条件节点
│   ├── python_docker_node.go # 🐍 Python Docker节点
│   ├── domain_resolve.go   # 🌐 域名解析节点
│   ├── custom_node.go      # 🛠️ 自定义节点
│   └── *_test.go          # 🧪 单元测试文件
├── workflow/              # 🔄 Temporal 工作流层
│   ├── workflow.go       # 主工作流引擎
│   └── *_test.go         # 工作流测试
├── worker/               # 👷 Worker 运行时
│   └── worker.go        # Worker进程管理
├── docs/                 # 📚 项目文档
├── examples/             # 📖 示例和用例
├── go.mod               # Go模块定义
├── go.sum               # 依赖锁定文件
└── README.md            # 项目说明
```

## 🔧 快速开始

### 1. 环境准备

确保你的系统已安装以下依赖：

- **Go 1.24.6+**
- **Docker** (用于运行 Temporal Server 和 Python 节点)

启动本地 Temporal Server：
```bash
docker run --rm -it \
  -p 7233:7233 \
  -p 8233:8233 \
  --name temporal \
  temporalio/auto-setup:latest
```

访问 Temporal UI：http://localhost:8233

### 2. 编译和启动 Worker

```bash
# 编译 Worker
go build -o n8n2temporal-worker ./worker

# 启动 Worker
./n8n2temporal-worker
```

或者直接运行：
```bash
go run ./worker
```

### 3. CLI 命令

```bash
# 格式化代码
go fmt ./...

# 代码检查
go vet ./...

# 安装/更新依赖
go mod tidy

# 运行所有测试
go test ./...

# 运行节点测试
go test ./node -v

# 构建项目
go build ./worker
```

## 🎯 节点使用示例

### 完整工作流示例

```json
{
  "id": "demo-workflow-v2",
  "name": "演示工作流 v2.0.0",
  "nodes": [
    {
      "id": "start",
      "name": "开始节点",
      "type": "n8n-nodes-base.start",
      "position": [240, 100],
      "parameters": {
        "globalParameters": {
          "python-node": {
            "timeout": 30,
            "requirements": ["requests"]
          },
          "conditional-node": {
            "mode": "strict"
          }
        }
      }
    },
    {
      "id": "variables",
      "name": "变量节点",
      "type": "n8n-nodes-base.variable",
      "position": [460, 100],
      "parameters": {
        "operation": "set",
        "variables": {
          "user_id": "12345",
          "session_token": "abc123xyz"
        }
      }
    },
    {
      "id": "condition",
      "name": "条件判断",
      "type": "n8n-nodes-base.conditional",
      "position": [680, 100],
      "parameters": {
        "nodeType": "if",
        "conditions": [{
          "id": "check_user",
          "name": "检查用户状态",
          "outputPath": "valid-user",
          "logicOperator": "AND",
          "conditions": [{
            "leftValue": "user_id",
            "operator": "not_empty",
            "rightValue": ""
          }]
        }],
        "defaultBranch": "invalid-user"
      }
    },
    {
      "id": "python",
      "name": "Python节点",
      "type": "n8n-nodes-base.pythonDocker",
      "position": [900, 100],
      "parameters": {
        "code": "result = {'processed_data': input_data.get('user_id', 'unknown'), 'timestamp': time.time()}",
        "dockerImage": "python:3.11-slim",
        "timeoutSeconds": 30
      }
    },
    {
      "id": "end",
      "name": "结束节点",
      "type": "n8n-nodes-base.end",
      "position": [1120, 100],
      "parameters": {
        "resultMode": "all"
      }
    }
  ],
  "connections": {
    "开始节点": {
      "main": [[{"node": "变量节点", "type": "main", "index": 0}]]
    },
    "变量节点": {
      "main": [[{"node": "条件判断", "type": "main", "index": 0}]]
    },
    "条件判断": {
      "valid-user": [[{"node": "Python节点", "type": "main", "index": 0}]],
      "invalid-user": [[{"node": "结束节点", "type": "main", "index": 0}]]
    },
    "Python节点": {
      "main": [[{"node": "结束节点", "type": "main", "index": 0}]]
    }
  }
}
```

## 🔧 扩展开发

### 创建自定义节点类型

```go
// 实现 Activity 接口
type MyCustomNode struct {
    *BaseActivity
}

func NewMyCustomNodeActivity(express *ExpressionEvaluator) Activity {
    return &MyCustomNode{
        BaseActivity: &BaseActivity{
            NodeInfo: &ActivityInfo{
                ID:          "my-custom-node",
                Name:        "我的自定义节点",
                Type:        "CUSTOM.myCustomNode",
                Description: "这是一个自定义节点示例",
                Version:     "1.0.0",
            },
            ExpressionEvaluator: express,
        },
    }
}

func (a *MyCustomNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
    return a.CreateSuccessOutput(input, map[string]interface{}{
        "message": "Hello from custom node",
    }), nil
}

// 在工作流中直接使用节点
express := NewExpressionEvaluator(nil)
customNode := NewMyCustomNodeActivity(express)
```

## 📊 性能优化

### 并行执行

工作流引擎自动识别无依赖关系的节点并并行执行：

```go
// 这些节点会自动并行执行
节点A -> 节点C
节点B -> 节点C
// 节点A和节点B无依赖关系，会并行执行
```

### 安全特性

- **Docker隔离**: Python节点在隔离容器中执行
- **资源限制**: 内存和CPU使用限制
- **网络安全**: 禁用网络访问（可选）
- **超时控制**: 防止无限执行

## 🛠️ 故障排除

### 常见问题

1. **Worker 无法连接到 Temporal Server**
   ```bash
   # 检查 Temporal Server 是否运行
   docker ps | grep temporal
   ```

2. **Python节点执行失败**
   ```bash
   # 检查Docker是否可用
   docker --version
   ```

3. **节点注册失败**
   ```bash
   # 运行节点注册测试
   go test ./node -run TestNodeRegistry -v
   ```

## 📄 许可证

本项目采用 MIT 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

## 🙏 致谢

感谢以下开源项目：

- [Temporal](https://temporal.io/) - 分布式工作流引擎
- [N8N](https://n8n.io/) - 工作流自动化平台
- [Go](https://golang.org/) - 编程语言

---

**🚀 现在就开始构建你的 N8N 到 Temporal 工作流吧！**