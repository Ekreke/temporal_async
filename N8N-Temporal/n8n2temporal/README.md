# N8N to Temporal 通用工作流引擎

这个项目实现了一个通用的 n8n 工作流转换引擎，可以将任意 n8n JSON 工作流定义转换为可在 Temporal 平台执行的分布式工作流。

## 核心特性

### 🚀 通用工作流引擎
- **与业务逻辑解耦**：引擎本身不关心每个节点具体做什么，只负责流程调度、数据管理和状态控制
- **动态节点调度**：支持任意复杂的节点连接关系和执行顺序
- **自动拓扑排序**：基于节点依赖关系自动确定正确的执行顺序
- **循环依赖检测**：自动检测并报告工作流中的循环依赖问题

### 🔗 连接关系查找 API
- **结构化输入输出**：使用 `N8NNode` 结构体作为输入和输出，确保类型安全
- **直观的接口设计**：`GetNextNodes(currentNode, allNodes, connections)` → `[]N8NNode`
- **完整的节点信息**：返回的下一跳节点包含完整的节点信息（ID、名称、类型、参数等）
- **高效的查找算法**：内部使用映射表优化节点查找性能

### 📊 数据流管理
- **智能数据合并**：自动合并多个前置节点的输出数据作为后置节点的输入
- **上下文传递**：在节点之间安全地传递执行上下文和中间结果
- **类型安全**：使用 Go 的类型系统确保数据传递的安全性

### 🔧 节点类型支持
- **手动触发节点** (`n8n-nodes-base.manualTrigger`)
- **Python 代码执行** (`n8n-nodes-base.code`)
- **条件判断节点** (`n8n-nodes-base.if`)
- **分支选择节点** (`n8n-nodes-base.switch`)
- **自定义节点** (`CUSTOM.*` 前缀的所有节点类型)
- **域名解析节点** (默认处理)

## 项目结构

```
n8n2temporal/
├── activity/          # Temporal 活动实现
│   ├── python.go      # Python 代码执行活动
│   ├── custom_node.go # 自定义节点活动
│   ├── domain_resolve.go # 域名解析活动
│   ├── condition_if.go  # IF 条件判断活动
│   └── switch.go       # Switch 分支选择活动
├── workflow/          # Temporal 工作流定义
│   └── workflow.go    # 主要工作流逻辑和通用引擎
├── worker/           # Worker 进程
│   └── worker.go     # 启动并运行 Temporal Worker
├── go.mod            # Go 模块定义
├── go.sum            # 依赖锁定文件
├── example.go        # 使用示例
└── README.md         # 项目说明文档
```

## 快速开始

### 1. 环境准备

确保你的系统上运行着 Temporal Server：
```bash
# 临时启动一个本地 Temporal Server
docker run --rm -it -p 7233:7233 --name temporal temporalio/auto-setup:latest
```

### 2. 启动 Worker

```bash
go run ./worker
```

Worker 将会：
- 连接到 `localhost:7233` 的 Temporal Server
- 监听 `n8n-conversion-queue` 任务队列
- 注册所有必要的工作流和活动

### 3. 连接关系查找 API

```go
package main

import (
    "fmt"
    "n8n2temporal/workflow"
)

func main() {
    // 解析 n8n 工作流
    workflowDef, err := workflow.ParseN8NWorkflow(n8nWorkflowJSON)
    if err != nil {
        panic(err)
    }

    // 选择一个节点
    currentNode := workflowDef.Nodes[0]

    // 获取该节点的所有下一跳节点
    nextNodes, err := workflow.GetNextNodes(
        currentNode,           // 当前节点 (N8NNode)
        workflowDef.Nodes,     // 所有节点列表
        workflowDef.Connections, // 连接关系
    )
    if err != nil {
        panic(err)
    }

    fmt.Printf("节点 %s 的下一跳节点:\n", currentNode.Name)
    for i, node := range nextNodes {
        fmt.Printf("%d. %s (ID: %s, 类型: %s)\n", i+1, node.Name, node.ID, node.Type)
    }
}
```

### 4. 使用通用工作流引擎

```go
package main

import (
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"
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

    // n8n 工作流 JSON 定义
    n8nWorkflowJSON := `{
        "id": "example-workflow",
        "name": "示例工作流",
        "nodes": [...],
        "connections": {...}
    }`

    // 初始数据
    initialData := map[string]interface{}{
        "triggerData": map[string]interface{}{
            "message": "Hello World",
        },
    }

    // 执行工作流
    workflowOptions := client.StartWorkflowOptions{
        ID:        "generic-workflow-123",
        TaskQueue: "n8n-conversion-queue",
    }

    we, err := c.ExecuteWorkflow(
        context.Background(),
        workflowOptions,
        workflow.GenericWorkflow,
        n8nWorkflowJSON,
        initialData,
    )
    if err != nil {
        panic(err)
    }

    // 等待工作流完成
    var result map[string]interface{}
    err = we.Get(context.Background(), &result)
    if err != nil {
        panic(err)
    }

    fmt.Printf("工作流执行结果: %+v\n", result)
}
```

## 核心算法

### 拓扑排序算法

引擎使用拓扑排序算法来确定节点的执行顺序：

1. **构建依赖图**：根据 n8n 的 `connections` 字段构建节点间的依赖关系
2. **计算入度**：计算每个节点的依赖数量
3. **Kahn 算法**：使用队列逐步移除入度为 0 的节点，生成执行顺序
4. **循环检测**：如果无法处理所有节点，说明存在循环依赖

### 数据流管理

- **输入数据准备**：合并所有前置节点的输出作为当前节点的输入
- **结果收集**：每个节点的执行结果被存储在上下文中
- **元数据管理**：记录执行时间、节点类型等调试信息

## 扩展开发

### 添加新的节点类型

1. 在 `activity/` 目录下创建新的活动实现
2. 在 `executeNode` 函数中添加相应的 case 处理
3. 在 `worker/worker.go` 中注册新活动

```go
// 1. 创建新活动
type NewNodeActivity struct{}

func (a *NewNodeActivity) Execute(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
    // 实现节点逻辑
    return result, nil
}

// 2. 在 executeNode 中添加处理
case "custom.newNodeType":
    var result map[string]interface{}
    newActivity := &activity.NewNodeActivity{}
    err = workflow.ExecuteActivity(ctx, newActivity.Execute, inputData).Get(ctx, &result)

// 3. 在 worker 中注册
w.RegisterActivity(&activity.NewNodeActivity{})
```

### 自定义错误处理

引擎提供了完善的错误处理机制：
- 节点执行失败时会记录错误信息但继续执行其他节点
- 最终结果中包含所有节点的成功/失败状态
- 支持重试策略配置

## 性能特性

- **并行执行**：无依赖关系的节点可以并行执行
- **内存优化**：使用指针传递大型数据结构
- **容错性**：单个节点失败不影响整体工作流
- **可观测性**：详细的日志记录和执行跟踪

## 示例工作流

项目包含了一个完整的示例工作流，展示了以下特性：
- 多个节点的串行和并行执行
- 条件分支 (IF 节点)
- 多分支选择 (Switch 节点)
- 自定义节点类型

运行示例：
```bash
go run example.go
```

## 注意事项

1. **Temporal Server**：确保 Temporal Server 在本地运行
2. **节点类型**：目前支持的节点类型有限，可以轻松扩展
3. **Python 执行**：Python 代码执行是模拟的，实际使用时需要集成 Python 解释器
4. **内存使用**：大型工作流可能需要优化内存使用策略

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个项目！

## 许可证

MIT License