# N8N to Temporal 工作流转换引擎

[![Go Version](https://img.shields.io/badge/Go-1.24.6+-blue.svg)](https://golang.org)
[![Temporal](https://img.shields.io/badge/Temporal-SDK-orange.svg)](https://temporal.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

这个项目实现了一个强大的 N8N 到 Temporal 的工作流转换引擎，可以将 N8N 的 JSON 工作流定义转换为可在 Temporal 平台执行的分布式、可容错的工作流。

## 🌟 核心特性

### 🚀 统一节点架构
- **标准化接口**：所有节点都实现统一的 `NodeActivity` 接口
- **动态类型系统**：支持运行时节点类型推断和验证
- **可扩展设计**：轻松添加新的节点类型和功能
- **向后兼容**：保持与旧版本接口的兼容性

### 🔧 智能分支系统
- **规则索引驱动**：基于规则数组索引的 N8N 原生分支逻辑
- **IF 节点支持**：条件判断分支，支持 AND/OR 组合器
- **Switch 节点支持**：多规则分支选择，支持复杂条件组合
- **自动分支匹配**：根据条件结果自动选择正确的执行路径

### 📊 完整的节点类型支持

| 节点类型 | 功能描述 | 支持程度 |
|---------|---------|---------|
| **手动触发节点** | `n8n-nodes-base.manualTrigger` | ✅ 完全支持 |
| **Python 代码执行** | `n8n-nodes-base.code` | ✅ 完全支持 |
| **条件判断节点** | `n8n-nodes-base.if` | ✅ 完全支持 |
| **分支选择节点** | `n8n-nodes-base.switch` | ✅ 完全支持 |
| **域名解析节点** | `DNS.domainResolve` | ✅ 完全支持 |
| **自定义节点** | `CUSTOM.*` 前缀 | ✅ 完全支持 |

### 🎯 高级工作流引擎
- **拓扑排序算法**：自动确定节点执行顺序
- **循环依赖检测**：智能检测并报告循环依赖
- **并行执行优化**：无依赖节点自动并行执行
- **数据流管理**：智能合并和传递节点间数据
- **上下文追踪**：完整的执行上下文和状态管理

## 🏗️ 项目架构

```
n8n2temporal/
├── activity/                    # Temporal 活动层
│   ├── base.go                 # 🏛️ 基础活动架构和统一接口
│   ├── registry.go             # 📋 节点注册中心
│   ├── executor.go             # ⚡ 统一节点执行器
│   ├── domain_resolve.go       # 🌐 域名解析活动
│   ├── python.go               # 🐍 Python 代码执行活动
│   ├── condition_if.go         # 🔀 IF 条件判断活动
│   ├── switch.go               # 🔀 Switch 分支选择活动
│   ├── custom_node.go          # 🛠️ 自定义节点活动
│   └── *_test.go               # 🧪 单元测试文件
├── workflow/                   # Temporal 工作流层
│   ├── workflow.go            # 🔄 主工作流引擎
│   └── *_test.go              # 🧪 工作流测试
├── worker/                     # Worker 运行时
│   └── worker.go              # 👷 Worker 进程管理
├── docs/                      # 📚 详细文档
│   ├── README.md              # 项目主文档
│   └── ACTIVITY_NODES.md      # 节点详细文档
├── go.mod                     # Go 模块定义
├── go.sum                     # 依赖锁定文件
└── example.go                 # 📖 使用示例
```

## 🚀 快速开始

### 1. 环境准备

确保你的系统已安装以下依赖：

- **Go 1.24.6+**
- **Docker** (用于运行 Temporal Server)

启动本地 Temporal Server：
```bash
# 启动 Temporal Server (包含 UI 界面)
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

Worker 将会：
- 连接到 `localhost:7233` 的 Temporal Server
- 监听 `n8n-conversion-queue` 任务队列
- 注册所有节点类型和活动
- 启动工作流处理引擎

### 3. 运行示例工作流

```bash
# 运行完整示例
go run example.go
```

## 📖 详细使用指南

### 基础工作流执行

```go
package main

import (
    "context"
    "go.temporal.io/sdk/client"
    "n8n2temporal/workflow"
)

func main() {
    // 创建 Temporal 客户端
    c, err := client.Dial(client.Options{
        HostPort: "localhost:7233",
    })
    if err != nil {
        panic(err)
    }
    defer c.Close()

    // N8N 工作流 JSON 定义
    n8nWorkflowJSON := `{
        "id": "demo-workflow",
        "name": "演示工作流",
        "nodes": [
            {
                "id": "1",
                "name": "手动触发",
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [240, 300]
            },
            {
                "id": "2",
                "name": "域名解析",
                "type": "CUSTOM.asmDomainResolve",
                "typeVersion": 1,
                "position": [460, 300],
                "parameters": {
                    "domain": "example.com"
                }
            }
        ],
        "connections": {
            "手动触发": {
                "main": [[{"node": "域名解析", "type": "main", "index": 0}]]
            }
        }
    }`

    // 执行工作流
    workflowOptions := client.StartWorkflowOptions{
        ID:        "demo-workflow-123",
        TaskQueue: "n8n-conversion-queue",
    }

    we, err := c.ExecuteWorkflow(
        context.Background(),
        workflowOptions,
        workflow.GenericWorkflow,
        n8nWorkflowJSON,
        map[string]interface{}{
            "triggerData": map[string]interface{}{
                "message": "Hello World",
            },
        },
    )
    if err != nil {
        panic(err)
    }

    // 等待工作流完成
    var result workflow.Output
    err = we.Get(context.Background(), &result)
    if err != nil {
        panic(err)
    }

    if result.Success {
        fmt.Printf("✅ 工作流执行成功: %+v\n", result.Results)
    } else {
        fmt.Printf("❌ 工作流执行失败: %s\n", result.Error)
    }
}
```

### 使用节点注册中心

```go
package main

import (
    "fmt"
    "n8n2temporal/activity"
)

func main() {
    // 获取全局节点注册中心
    registry := activity.GetGlobalNodeRegistry()

    // 列出所有已注册的节点类型
    fmt.Println("已注册的节点类型:")
    for nodeType := range registry.GetRegisteredNodeTypes() {
        fmt.Printf("- %s\n", nodeType)
    }

    // 创建特定类型的节点
    node, err := registry.CreateNode("n8n-nodes-base.if")
    if err != nil {
        panic(err)
    }

    // 获取节点信息
    info := node.GetNodeInfo()
    fmt.Printf("节点信息: %s (%s)\n", info.Name, info.Type)
}
```

## 🔧 扩展开发

### 创建自定义节点类型

1. **实现 NodeActivity 接口**

```go
package activity

import (
    "context"
)

// MyCustomNode 自定义节点
type MyCustomNode struct {
    BaseActivity
}

// NewMyCustomNode 创建自定义节点实例
func NewMyCustomNode() *MyCustomNode {
    return &MyCustomNode{
        BaseActivity: BaseActivity{
            NodeInfo: &NodeInfo{
                ID:          "my_custom_node",
                Name:        "我的自定义节点",
                Type:        "CUSTOM.myCustomNode",
                Description: "这是一个自定义节点示例",
                Version:     "1.0.0",
                Category:    "custom",
                Icon:        "🛠️",
                Inputs: []NodePort{
                    {
                        ID:          "input",
                        Name:        "输入数据",
                        Type:        "object",
                        Required:    false,
                        Description: "节点的输入数据",
                    },
                },
                Outputs: []NodePort{
                    {
                        ID:          "output",
                        Name:        "输出数据",
                        Type:        "object",
                        Required:    false,
                        Description: "节点的输出数据",
                    },
                },
                Parameters: []NodeParameter{
                    {
                        ID:          "message",
                        Name:        "消息",
                        Type:        "string",
                        Required:    true,
                        Description: "要处理的消息",
                        Default:     "Hello World",
                    },
                },
            },
        },
    }
}

// Execute 执行节点逻辑
func (a *MyCustomNode) Execute(ctx context.Context, input *NodeInput) (*NodeOutput, error) {
    return a.ExecuteWithExecuteTiming(ctx, input, a.executeCustomNode)
}

// executeCustomNode 内部执行逻辑
func (a *MyCustomNode) executeCustomNode(input *NodeInput) (map[string]interface{}, error) {
    // 获取参数
    message := a.GetStringParameter(input.Parameters, "message")
    if message == "" {
        message = "Hello World"
    }

    // 执行自定义逻辑
    result := map[string]interface{}{
        "processed_message": message,
        "timestamp":        time.Now().Unix(),
        "node_info": map[string]interface{}{
            "id":   input.NodeID,
            "name": input.NodeName,
            "type": input.NodeType,
        },
    }

    return result, nil
}

// ValidateInput 验证输入参数
func (a *MyCustomNode) ValidateInput(input *NodeInput) error {
    // 基础验证
    if err := a.BaseActivity.ValidateInput(input); err != nil {
        return err
    }

    // 自定义验证逻辑
    message := a.GetStringParameter(input.Parameters, "message")
    if len(message) > 1000 {
        return fmt.Errorf("消息长度不能超过 1000 个字符")
    }

    return nil
}

// 初始化时注册节点
func init() {
    RegisterNodeActivity("CUSTOM.myCustomNode", NewMyCustomNode)
}
```

2. **在工作流中处理新节点类型**

```go
// 在 workflow/workflow.go 的 executeNode 函数中添加
case "CUSTOM.myCustomNode":
    var customResult map[string]interface{}
    customActivity := activity.NewMyCustomNode()
    err = workflow.ExecuteActivity(ctx, customActivity.Execute, inputData).Get(ctx, &customResult)
    if err == nil {
        result.Success = true
        result.Data = customResult
    }
```

3. **在 Worker 中注册活动**

```go
// 在 worker/worker.go 中添加
w.RegisterActivity(activity.NewMyCustomNode().Execute)
```

### 使用节点执行器

```go
package main

import (
    "context"
    "n8n2temporal/activity"
)

func main() {
    // 创建节点执行器
    executor := activity.NewNodeExecutor()

    // 准备输入数据
    input := &activity.NodeInput{
        NodeID:   "test-node-1",
        NodeName: "测试节点",
        NodeType: "CUSTOM.myCustomNode",
        Parameters: map[string]interface{}{
            "message": "Hello from executor",
        },
        InputData: map[string]interface{}{
            "test": "data",
        },
    }

    // 执行节点
    ctx := context.Background()
    output, err := executor.ExecuteNode(ctx, "CUSTOM.myCustomNode", input)
    if err != nil {
        panic(err)
    }

    if output.Success {
        fmt.Printf("✅ 节点执行成功: %+v\n", output.Data)
    } else {
        fmt.Printf("❌ 节点执行失败: %s\n", output.Error)
    }
}
```

## 🧪 测试指南

### 运行所有测试

```bash
# 运行所有包的测试
go test ./...

# 运行特定包的测试
go test ./activity
go test ./workflow
go test ./worker
```

### 运行特定测试

```bash
# 测试特定节点类型
go test ./activity -v -run TestDomainResolve
go test ./activity -v -run TestCondition
go test ./activity -v -run TestSwitch

# 测试节点注册中心
go test ./activity -v -run TestNodeRegistry

# 测试工作流引擎
go test ./workflow -v -run TestTopologicalSort
```

### 基准测试

```bash
# 运行性能基准测试
go test ./activity -bench=.
go test ./workflow -bench=.
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

### 内存优化

- 使用指针传递大型数据结构
- 及时释放不需要的中间结果
- 批量处理大量数据时使用流式处理

### 重试策略

配置活动重试策略：

```go
// 在 worker 中配置重试选项
workerOptions := worker.Options{
    Interceptors: []worker.Interceptor{
        &activityInterceptor{},
    },
    WorkflowPanicPolicy: worker.FailWorkflow,
}
```

## 🔍 监控和调试

### 日志配置

```go
import (
    "go.temporal.io/sdk/log"
)

// 配置详细日志
logger := log.NewLogger(log.NewStructuredLogger())

// 在活动中使用日志
func (a *MyActivity) Execute(ctx context.Context, input *NodeInput) (*NodeOutput, error) {
    logger := a.GetLogger(ctx)
    logger.Info("开始执行节点",
        "nodeId", input.NodeID,
        "nodeType", input.NodeType,
    )
    // ... 执行逻辑
    logger.Info("节点执行完成", "duration", time.Since(start))
}
```

### Temporal UI 监控

访问 http://localhost:8233 查看：

- 工作流执行历史
- 节点执行状态
- 性能指标
- 错误日志
- 重试记录

### 指标收集

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/metric"
)

// 初始化指标收集
meter := otel.Meter("n8n2temporal")

// 创建计数器
nodeExecutionCounter, _ := meter.Int64Counter(
    "node_executions_total",
    metric.WithDescription("Total number of node executions"),
)

// 在节点执行时记录指标
nodeExecutionCounter.Add(ctx, 1,
    attribute.String("node_type", input.NodeType),
    attribute.String("result", "success"),
)
```

## 🛠️ 故障排除

### 常见问题

1. **Worker 无法连接到 Temporal Server**
   ```bash
   # 检查 Temporal Server 是否运行
   docker ps | grep temporal

   # 检查端口是否被占用
   lsof -i :7233
   ```

2. **节点执行失败**
   ```bash
   # 查看详细错误日志
   go run ./worker --log-level debug

   # 检查节点注册状态
   go test ./activity -v -run TestNodeRegistry
   ```

3. **内存使用过高**
   ```bash
   # 分析内存使用
   go test ./activity -memprofile=mem.prof -bench=.
   go tool pprof mem.prof
   ```

### 调试技巧

1. **启用详细日志**
   ```go
   // 在 worker.go 中配置日志级别
   logger := log.NewLogger(log.NewStructuredLogger())
   ```

2. **单步调试**
   ```go
   // 在节点中添加调试信息
   func (a *MyActivity) Execute(ctx context.Context, input *NodeInput) (*NodeOutput, error) {
       fmt.Printf("DEBUG: 输入数据: %+v\n", input)
       // ... 执行逻辑
       fmt.Printf("DEBUG: 输出数据: %+v\n", output)
   }
   ```

3. **单元测试特定节点**
   ```go
   // 创建节点实例进行测试
   node := activity.NewMyCustomNode()
   input := &activity.NodeInput{
       NodeID:   "test",
       NodeType: "CUSTOM.myCustomNode",
       Parameters: map[string]interface{}{
           "message": "test",
       },
   }

   output, err := node.Execute(context.Background(), input)
   // 验证结果
   ```

## 📚 详细文档

我们提供了完整的中文文档系列：

- **📖 Activity 节点详细文档** ([docs/ACTIVITY_NODES.md](docs/ACTIVITY_NODES.md))
  - 所有节点类型的详细说明
  - 参数配置和使用示例
  - 节点开发指南和最佳实践

- **⚙️ 工作流引擎详细文档** ([docs/WORKFLOW_ENGINE.md](docs/WORKFLOW_ENGINE.md))
  - 引擎架构和算法原理
  - 拓扑排序和数据流管理
  - 分支控制和错误处理机制

- **👷 Worker 配置详细文档** ([docs/WORKER_CONFIG.md](docs/WORKER_CONFIG.md))
  - Worker 配置和部署策略
  - 监控、日志和性能调优
  - Docker 和 Kubernetes 部署指南

- **🔗 API 参考文档** ([docs/API_REFERENCE.md](docs/API_REFERENCE.md))
  - 完整的 API 接口说明
  - 数据结构定义
  - 使用示例和最佳实践

## 🤝 贡献指南

我们欢迎所有形式的贡献！

### 开发流程

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

### 代码规范

- 遵循 Go 标准代码规范
- 添加完整的单元测试
- 更新相关文档
- 确保所有测试通过

## 📄 许可证

本项目采用 MIT 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

## 🙏 致谢

感谢以下开源项目：

- [Temporal](https://temporal.io/) - 分布式工作流引擎
- [N8N](https://n8n.io/) - 工作流自动化平台
- [Go](https://golang.org/) - 编程语言

---

**🚀 现在就开始构建你的 N8N 到 Temporal 工作流吧！**

如有问题，欢迎提交 Issue 或联系我们。