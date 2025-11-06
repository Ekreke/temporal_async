# N8N工作流图对象优化

## 优化内容

本次优化将n8n工作流的解析逻辑封装成了一个独立的图对象 `N8NWorkflowGraph`，提供了更清晰、更易用的API。

## 新增功能

### 1. N8NWorkflowGraph 结构体

```go
type N8NWorkflowGraph struct {
    workflow       *N8NWorkflow                 // 原始工作流定义
    nextNodesMap   map[string][]string          // 节点名称 -> 下一跳节点名称列表
    nodeNameMap    map[string]N8NNode           // 节点名称 -> 节点定义
    nodeIDMap      map[string]string            // 节点ID -> 节点名称
    dependencyGraph map[string][]string         // 节点名称 -> 依赖的节点名称列表
}
```

### 2. 核心方法

#### ParseFromJSON - 从JSON创建图对象
```go
func (g *N8NWorkflowGraph) ParseFromJSON(workflowDefJSON string) error
```

#### GetNextNodes - 获取下一跳节点数组 ⭐
```go
func (g *N8NWorkflowGraph) GetNextNodes(node interface{}) ([]N8NNode, error)
```
- 支持传入节点名称（string）或节点对象（N8NNode）
- 返回下一跳节点的完整对象数组
- 如果没有下一跳节点，返回空数组

#### 辅助方法
- `GetNextNodeNames()` - 获取下一跳节点名称数组
- `GetNodeByName()` - 根据名称获取节点对象
- `GetNodeByID()` - 根据ID获取节点对象
- `GetAllNodes()` - 获取所有节点
- `GetStartNodes()` - 获取起始节点
- `GetEndNodes()` - 获取结束节点
- `GetDependencyGraph()` - 获取依赖图

### 3. 便捷构造函数

```go
// 创建空的图对象
func NewN8NWorkflowGraph() *N8NWorkflowGraph

// 从JSON直接创建图对象
func NewN8NWorkflowGraphFromJSON(workflowDefJSON string) (*N8NWorkflowGraph, error)
```

## 使用示例

### 基本用法
```go
// 方法1：从JSON直接创建图对象
graph, err := workflow.NewN8NWorkflowGraphFromJSON(n8nJSON)
if err != nil {
    return err
}

// 方法2：分步创建
var graph workflow.N8NWorkflowGraph
err := graph.ParseFromJSON(n8nJSON)
```

### 获取下一跳节点
```go
// 使用节点名称
nextNodes, err := graph.GetNextNodes("域名解析")

// 使用节点对象
domainNode, _ := graph.GetNodeByName("域名解析")
nextNodes, err := graph.GetNextNodes(domainNode)

// 获取下一跳节点名称
nextNames, err := graph.GetNextNodeNames("域名解析")
```

### 遍历工作流
```go
// 获取起始节点
startNodes := graph.GetStartNodes()
for _, startNode := range startNodes {
    fmt.Printf("起始节点: %s\n", startNode.Name)

    // 获取下一跳节点
    nextNodes, _ := graph.GetNextNodes(startNode)
    for _, nextNode := range nextNodes {
        fmt.Printf("  -> %s\n", nextNode.Name)
    }
}
```

## 优化效果

1. **封装性**：将复杂的图解析逻辑封装在对象内部
2. **易用性**：提供简单直观的API接口
3. **灵活性**：支持多种查询方式（名称、ID、对象）
4. **性能**：预先构建所有索引，查询效率高
5. **可维护性**：代码结构清晰，易于扩展

## 代码优化详情

### parseConnections 方法重构

**优化前问题**：
- 嵌套层级过深（6层嵌套）
- 单一函数承担过多职责
- 错误处理分散
- 可读性差

**优化后改进**：
- 将6层嵌套拆分为4个独立函数
- 每个函数职责单一，易于理解
- 提前返回，减少嵌套
- 增加详细注释说明数据结构

**重构后的函数结构**：
```go
parseConnections()           // 主函数，遍历所有源节点
├── extractTargetNodesFromMain()    // 处理main端口连接
    ├── extractTargetNodesFromBranch()   // 处理单个分支
        └── extractTargetNodeFromConnection()  // 处理单个连接
```

**优化效果**：
- ✅ 代码可读性大幅提升
- ✅ 每个函数职责明确
- ✅ 易于单独测试和维护
- ✅ 减少了认知复杂度

## 重构内容

- 重构了 `GenericWorkflow` 函数使用新的图对象
- 保持了原有功能的完整性
- 简化了工作流执行逻辑
- 提供了完整的测试用例

## 测试验证

```bash
# 运行测试
go test ./workflow -v

# 运行示例
go build . && ./n8n2temporal
```

测试覆盖了：
- 图对象创建和解析
- 下一跳节点查询
- 节点查找功能
- 依赖图构建
- 起始和结束节点识别