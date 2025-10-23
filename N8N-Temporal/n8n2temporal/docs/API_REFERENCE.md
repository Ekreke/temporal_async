# API 参考文档

本文档提供了 N8N to Temporal 工作流转换引擎的完整 API 接口参考。

## 📋 目录

- [核心接口](#核心接口)
- [数据结构](#数据结构)
- [活动接口](#活动接口)
- [工作流接口](#工作流接口)
- [工具函数](#工具函数)

## 🔧 核心接口

### NodeActivity 接口

所有活动节点都必须实现的核心接口：

```go
type NodeActivity interface {
    // GetNodeInfo 获取节点元信息
    GetNodeInfo() *NodeInfo

    // Execute 执行节点逻辑
    Execute(ctx context.Context, input *NodeInput) (*NodeOutput, error)

    // ValidateInput 验证输入参数
    ValidateInput(input *NodeInput) error

    // GetLogger 获取日志记录器
    GetLogger(ctx context.Context) log.Logger
}
```

### 节点注册接口

```go
// NodeRegistry 节点注册中心接口
type NodeRegistry interface {
    // RegisterNode 注册节点类型
    RegisterNodeActivity(nodeType string, factory NodeFactory) error

    // CreateNode 创建节点实例
    CreateNode(nodeType string) (NodeActivity, error)

    // GetRegisteredNodeTypes 获取已注册的节点类型
    GetRegisteredNodeTypes() map[string]NodeFactory

    // IsNodeTypeRegistered 检查节点类型是否已注册
    IsNodeTypeRegistered(nodeType string) bool
}

// NodeFactory 节点工厂函数类型
type NodeFactory func() NodeActivity
```

## 📊 数据结构

### NodeInfo - 节点信息

```go
type NodeInfo struct {
    ID          string        `json:"id"`           // 节点唯一标识
    Name        string        `json:"name"`         // 节点显示名称
    Type        string        `json:"type"`         // 节点类型标识
    Description string        `json:"description"`  // 节点描述
    Version     string        `json:"version"`      // 节点版本
    Category    string        `json:"category"`     // 节点分类
    Icon        string        `json:"icon"`         // 节点图标
    Inputs      []NodePort    `json:"inputs"`       // 输入端口定义
    Outputs     []NodePort    `json:"outputs"`      // 输出端口定义
    Parameters  []NodeParameter `json:"parameters"` // 参数定义
}
```

### NodePort - 节点端口

```go
type NodePort struct {
    ID          string `json:"id"`          // 端口ID
    Name        string `json:"name"`        // 端口名称
    Type        string `json:"type"`        // 端口类型
    Required    bool   `json:"required"`    // 是否必需
    Description string `json:"description"` // 端口描述
}
```

### NodeParameter - 节点参数

```go
type NodeParameter struct {
    ID          string                 `json:"id"`          // 参数ID
    Name        string                 `json:"name"`        // 参数名称
    Type        string                 `json:"type"`        // 参数类型
    Required    bool                   `json:"required"`    // 是否必需
    Default     interface{}            `json:"default"`     // 默认值
    Description string                 `json:"description"` // 参数描述
    Options     map[string]interface{} `json:"options"`     // 参数选项
}
```

### NodeInput - 节点输入

```go
type NodeInput struct {
    NodeID      string                 `json:"nodeId"`      // 节点ID
    NodeName    string                 `json:"nodeName"`    // 节点名称
    NodeType    string                 `json:"nodeType"`    // 节点类型
    InputData   map[string]interface{} `json:"inputData"`   // 输入数据
    Parameters  map[string]interface{} `json:"parameters"`  // 节点参数
    WorkflowID  string                 `json:"workflowId"`  // 工作流ID
    ExecutionID string                 `json:"executionId"` // 执行实例ID
}
```

### NodeOutput - 节点输出

```go
type NodeOutput struct {
    NodeID      string                 `json:"nodeId"`      // 节点ID
    NodeName    string                 `json:"nodeName"`    // 节点名称
    NodeType    string                 `json:"nodeType"`    // 节点类型
    Success     bool                   `json:"success"`     // 执行是否成功
    Data        map[string]interface{} `json:"data"`        // 输出数据
    Error       string                 `json:"error"`       // 错误信息
    ProcessedAt time.Time              `json:"processedAt"` // 处理时间
    Metadata    map[string]interface{} `json:"metadata"`    // 元数据
}
```

## 🎯 活动接口

### 内置活动节点

#### DomainResolveActivity - 域名解析

```go
// NewDomainResolveActivity 创建域名解析节点
func NewDomainResolveActivity() *DomainResolveActivity

// ExecuteDomainResolve 执行域名解析（向后兼容接口）
func (a *DomainResolveActivity) ExecuteDomainResolve(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
```

#### PythonCodeActivity - Python 代码执行

```go
// NewPythonCodeActivity 创建 Python 代码执行节点
func NewPythonCodeActivity() *PythonCodeActivity

// ExecutePythonCode 执行 Python 代码（向后兼容接口）
func (a *PythonCodeActivity) ExecutePythonCode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
```

#### ConditionCheckActivity - 条件判断

```go
// NewConditionCheckActivity 创建条件判断节点
func NewConditionCheckActivity() *ConditionCheckActivity

// ExecuteIf 执行条件判断（向后兼容接口）
func (a *ConditionCheckActivity) ExecuteIf(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
```

#### SwitchNodeActivity - 分支选择

```go
// NewSwitchNodeActivity 创建分支选择节点
func NewSwitchNodeActivity() *SwitchNodeActivity

// ExecuteSwitchNode 执行分支选择（向后兼容接口）
func (a *SwitchNodeActivity) ExecuteSwitchNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
```

#### CustomNodeActivity - 自定义节点

```go
// NewCustomNodeActivity 创建自定义节点
func NewCustomNodeActivity() *CustomNodeActivity

// ExecuteCustomNode 执行自定义节点（向后兼容接口）
func (a *CustomNodeActivity) ExecuteCustomNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
```

## 🔄 工作流接口

### 工作流执行

```go
// GenericWorkflow 通用工作流执行函数
func GenericWorkflow(ctx workflow.Context, n8nWorkflowJSON string, initialData map[string]interface{}) (Output, error)

// GenericWorkflowWithMaxStep 带最大步数限制的通用工作流
func GenericWorkflowWithMaxStep(ctx workflow.Context, n8nWorkflowJSON string, initialData map[string]interface{}, maxSteps int) (Output, error)
```

### 工作流解析

```go
// ParseN8NWorkflow 解析 N8N 工作流 JSON
func ParseN8NWorkflow(jsonData string) (*N8NWorkflow, error)

// N8NWorkflowGraph N8N 工作流图结构
type N8NWorkflowGraph struct {
    workflow        *N8NWorkflow        // 原始工作流定义
    nextNodesMap    map[string][]string // 节点名称 -> 下一跳节点名称列表
    nodeNameMap     map[string]N8NNode  // 节点名称 -> 节点定义
    nodeIDMap       map[string]string   // 节点ID -> 节点名称
    dependencyGraph map[string][]string // 节点名称 -> 依赖的节点名称列表
}

// NewN8NWorkflowGraph 创建工作流图
func NewN8NWorkflowGraph(workflow *N8NWorkflow) (*N8NWorkflowGraph, error)

// GetNextNodes 获取节点的下一跳节点
func (g *N8NWorkflowGraph) GetNextNodes(currentNode N8NNode, allNodes []N8NNode, connections map[string]interface{}) ([]N8NNode, error)

// TopologicalSort 拓扑排序
func (g *N8NWorkflowGraph) TopologicalSort() ([]string, error)
```

### 节点执行

```go
// ExecuteNode 执行单个节点
func ExecuteNode(ctx workflow.Context, node N8NNode, inputData map[string]interface{}) (*NodeResult, error)

// NodeResult 节点执行结果
type NodeResult struct {
    NodeID      string                 `json:"nodeId"`
    NodeName    string                 `json:"nodeName"`
    NodeType    string                 `json:"nodeType"`
    Success     bool                   `json:"success"`
    Data        map[string]interface{} `json:"data"`
    Error       string                 `json:"error"`
    ExecutedAt  time.Time              `json:"executedAt"`
    Duration    time.Duration          `json:"duration"`
    Metadata    map[string]interface{} `json:"metadata"`
}
```

## 🛠️ 工具函数

### 全局注册中心

```go
// GetGlobalNodeRegistry 获取全局节点注册中心
func GetGlobalNodeRegistry() *NodeRegistry

// RegisterNodeActivity 注册节点活动
func RegisterNodeActivity(nodeType string, factory NodeFactory) error

// CreateNodeByType 根据类型创建节点
func CreateNodeByType(nodeType string) (NodeActivity, error)

// GetRegisteredNodeTypes 获取已注册的节点类型
func GetRegisteredNodeTypes() []string
```

### 节点执行器

```go
// NewNodeExecutor 创建节点执行器
func NewNodeExecutor() *NodeExecutor

// ExecuteNode 执行节点
func (e *NodeExecutor) ExecuteNode(ctx context.Context, nodeType string, input *NodeInput) (*NodeOutput, error)

// ValidateNode 验证节点输入
func (e *NodeExecutor) ValidateNode(nodeType string, input *NodeInput) error
```

### 字符串处理工具

```go
// GetStringParameter 安全获取字符串参数
func (a *BaseActivity) GetStringParameter(params map[string]interface{}, key string) string

// GetMapParameter 安全获取映射参数
func (a *BaseActivity) GetMapParameter(params map[string]interface{}, key string) map[string]interface{}

// GetIntParameter 安全获取整数参数
func (a *BaseActivity) GetIntParameter(params map[string]interface{}, key string) int

// GetBoolParameter 安全获取布尔参数
func (a *BaseActivity) GetBoolParameter(params map[string]interface{}, key string) bool
```

### 数据处理工具

```go
// MergeData 合并数据
func MergeData(target, source map[string]interface{}) map[string]interface{}

// CloneData 克隆数据
func CloneData(data map[string]interface{}) map[string]interface{}

// ValidateData 验证数据结构
func ValidateData(data map[string]interface{}, schema map[string]interface{}) error

// TransformData 转换数据格式
func TransformData(data map[string]interface{}, fromFormat, toFormat string) (map[string]interface{}, error)
```

## 📝 使用示例

### 基本活动执行

```go
package main

import (
    "context"
    "fmt"
    "n8n2temporal/activity"
)

func main() {
    // 创建活动实例
    domainActivity := activity.NewDomainResolveActivity()

    // 准备输入数据
    input := &activity.NodeInput{
        NodeID:   "test-1",
        NodeName: "域名解析测试",
        NodeType: "DNS.domainResolve",
        Parameters: map[string]interface{}{
            "domain": "example.com",
        },
        InputData: map[string]interface{}{
            "source": "test",
        },
    }

    // 执行活动
    ctx := context.Background()
    output, err := domainActivity.Execute(ctx, input)
    if err != nil {
        fmt.Printf("执行失败: %v\n", err)
        return
    }

    if output.Success {
        fmt.Printf("执行成功: %+v\n", output.Data)
    } else {
        fmt.Printf("执行失败: %s\n", output.Error)
    }
}
```

### 工作流解析和执行

```go
package main

import (
    "context"
    "fmt"
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

    // N8N 工作流 JSON
    n8nWorkflowJSON := `{
        "id": "test-workflow",
        "name": "测试工作流",
        "nodes": [
            {
                "id": "1",
                "name": "手动触发",
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [240, 300]
            }
        ],
        "connections": {}
    }`

    // 执行工作流
    workflowOptions := client.StartWorkflowOptions{
        ID:        "test-workflow-123",
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

    // 等待结果
    var result workflow.Output
    err = we.Get(context.Background(), &result)
    if err != nil {
        panic(err)
    }

    fmt.Printf("工作流结果: %+v\n", result)
}
```

### 自定义节点开发

```go
package main

import (
    "context"
    "fmt"
    "n8n2temporal/activity"
)

// MyCustomNode 自定义节点
type MyCustomNode struct {
    activity.BaseActivity
}

// NewMyCustomNode 创建自定义节点
func NewMyCustomNode() *MyCustomNode {
    return &MyCustomNode{
        BaseActivity: activity.BaseActivity{
            NodeInfo: &activity.NodeInfo{
                ID:          "my_custom",
                Name:        "我的自定义节点",
                Type:        "CUSTOM.myCustom",
                Description: "自定义业务逻辑处理",
                Version:     "1.0.0",
                Category:    "custom",
                Icon:        "🛠️",
            },
        },
    }
}

// Execute 执行节点逻辑
func (a *MyCustomNode) Execute(ctx context.Context, input *activity.NodeInput) (*activity.NodeOutput, error) {
    return a.ExecuteWithExecuteTiming(ctx, input, a.execute)
}

// execute 内部执行逻辑
func (a *MyCustomNode) execute(input *activity.NodeInput) (map[string]interface{}, error) {
    // 自定义业务逻辑
    result := map[string]interface{}{
        "processed": true,
        "data":      input.InputData,
        "timestamp": time.Now().Unix(),
    }

    return result, nil
}

// ValidateInput 验证输入
func (a *MyCustomNode) ValidateInput(input *activity.NodeInput) error {
    // 自定义验证逻辑
    return nil
}

func main() {
    // 注册自定义节点
    activity.RegisterNodeActivity("CUSTOM.myCustom", func() activity.NodeActivity {
        return NewMyCustomNode()
    })

    // 使用自定义节点
    customNode := activity.NewMyCustomNode()
    fmt.Printf("节点信息: %+v\n", customNode.GetNodeInfo())
}
```

## 🔗 相关文档

- [项目主文档](../README.md)
- [Activity 节点详细文档](ACTIVITY_NODES.md)
- [工作流引擎详细文档](WORKFLOW_ENGINE.md)
- [Worker 配置详细文档](WORKER_CONFIG.md)

## 📞 技术支持

如有 API 使用相关问题，请：

1. 查阅本文档中的接口说明
2. 参考使用示例代码
3. 查看源代码中的注释
4. 提交 GitHub Issue 获取支持

---

**最后更新**: 2023年10月18日
**文档版本**: v1.0.0