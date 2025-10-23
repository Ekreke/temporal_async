# 工作流引擎详细文档

本文档详细介绍了 N8N to Temporal 工作流转换引擎的核心架构、算法原理、配置方法和最佳实践。

## 📋 目录

- [引擎架构概述](#引擎架构概述)
- [N8N 工作流解析](#n8n-工作流解析)
- [拓扑排序算法](#拓扑排序算法)
- [节点执行引擎](#节点执行引擎)
- [数据流管理](#数据流管理)
- [分支控制逻辑](#分支控制逻辑)
- [错误处理机制](#错误处理机制)
- [性能优化策略](#性能优化策略)
- [监控和调试](#监控和调试)

## 🏗️ 引擎架构概述

### 核心组件

```
工作流引擎架构
┌─────────────────────────────────────────────────────────────┐
│                    N8N 工作流引擎                            │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ N8N 解析器   │  │ 拓扑排序器   │  │ 执行调度器   │         │
│  │ Parser      │  │ Topological │  │ Scheduler   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ 数据流管理   │  │ 分支控制器   │  │ 错误处理器   │         │
│  │ Data Flow   │  │ Branch Ctrl │  │ Error Handler│         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                    Temporal SDK 层                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Activity    │  │ Workflow    │  │ Client      │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### 主要数据结构

```go
// N8NWorkflow N8N 工作流定义
type N8NWorkflow struct {
    ID          string                 `json:"id"`          // 工作流ID
    Name        string                 `json:"name"`        // 工作流名称
    Nodes       []N8NNode              `json:"nodes"`       // 节点列表
    Connections map[string]interface{} `json:"connections"` // 连接关系
    Active      bool                   `json:"active"`      // 是否激活
    Settings    map[string]interface{} `json:"settings"`    // 工作流设置
}

// N8NNode N8N 节点定义
type N8NNode struct {
    ID           string                 `json:"id"`           // 节点ID
    Name         string                 `json:"name"`         // 节点名称
    Type         string                 `json:"type"`         // 节点类型
    TypeVersion  int                    `json:"typeVersion"`  // 类型版本
    Position     [2]int                 `json:"position"`     // 位置坐标
    Parameters   map[string]interface{} `json:"parameters"`   // 节点参数
    Credentials  map[string]interface{} `json:"credentials"`  // 凭证信息
    WebhookID    string                 `json:"webhookId"`    // Webhook ID
}

// N8NWorkflowGraph 封装工作流图结构
type N8NWorkflowGraph struct {
    workflow        *N8NWorkflow        // 原始工作流定义
    nextNodesMap    map[string][]string // 节点名称 -> 下一跳节点名称列表
    nodeNameMap     map[string]N8NNode  // 节点名称 -> 节点定义
    nodeIDMap       map[string]string   // 节点ID -> 节点名称
    dependencyGraph map[string][]string // 节点名称 -> 依赖的节点名称列表
}
```

## 🔍 N8N 工作流解析

### 解析流程

1. **JSON 解析**: 将 N8N JSON 转换为 Go 结构体
2. **节点映射**: 建立 ID 和名称的双向映射
3. **连接分析**: 解析节点间的连接关系
4. **依赖图构建**: 构建依赖关系图
5. **验证检查**: 验证工作流的完整性

### 解析代码实现

```go
// ParseN8NWorkflow 解析 N8N 工作流 JSON
func ParseN8NWorkflow(jsonData string) (*N8NWorkflow, error) {
    var workflow N8NWorkflow

    // 解析 JSON
    if err := json.Unmarshal([]byte(jsonData), &workflow); err != nil {
        return nil, fmt.Errorf("解析 N8N 工作流 JSON 失败: %v", err)
    }

    // 验证工作流
    if err := validateWorkflow(&workflow); err != nil {
        return nil, fmt.Errorf("工作流验证失败: %v", err)
    }

    return &workflow, nil
}

// validateWorkflow 验证工作流的完整性
func validateWorkflow(workflow *N8NWorkflow) error {
    if workflow.ID == "" {
        return errors.New("工作流 ID 不能为空")
    }

    if len(workflow.Nodes) == 0 {
        return errors.New("工作流必须包含至少一个节点")
    }

    // 验证节点 ID 唯一性
    nodeIDs := make(map[string]bool)
    for _, node := range workflow.Nodes {
        if nodeIDs[node.ID] {
            return fmt.Errorf("节点 ID 重复: %s", node.ID)
        }
        nodeIDs[node.ID] = true
    }

    // 验证连接关系
    if err := validateConnections(workflow); err != nil {
        return fmt.Errorf("连接关系验证失败: %v", err)
    }

    return nil
}
```

### 连接关系解析

N8N 的连接关系采用复杂的嵌套结构：

```json
{
  "connections": {
    "节点名称": {
      "main": [
        [
          {"node": "下一节点1", "type": "main", "index": 0},
          {"node": "下一节点2", "type": "main", "index": 1}
        ]
      ]
    }
  }
}
```

解析函数实现：

```go
// ParseConnections 解析连接关系
func ParseConnections(connections map[string]interface{}) (map[string][]string, error) {
    nextNodesMap := make(map[string][]string)

    for nodeName, nodeConnections := range connections {
        connectionsMap, ok := nodeConnections.(map[string]interface{})
        if !ok {
            continue
        }

        // 解析 main 连接
        if mainConnections, exists := connectionsMap["main"]; exists {
            mainList, ok := mainConnections.([]interface{})
            if !ok {
                continue
            }

            var nextNodes []string
            for _, connectionGroup := range mainList {
                group, ok := connectionGroup.([]interface{})
                if !ok {
                    continue
                }

                for _, conn := range group {
                    connMap, ok := conn.(map[string]interface{})
                    if !ok {
                        continue
                    }

                    if nextNode, exists := connMap["node"]; exists {
                        if nextNodeName, ok := nextNode.(string); ok {
                            nextNodes = append(nextNodes, nextNodeName)
                        }
                    }
                }
            }

            if len(nextNodes) > 0 {
                nextNodesMap[nodeName] = nextNodes
            }
        }
    }

    return nextNodesMap, nil
}
```

## 🔄 拓扑排序算法

### 算法原理

使用 **Kahn 算法**进行拓扑排序：

1. **计算入度**: 统计每个节点的依赖数量
2. **初始化队列**: 将入度为 0 的节点加入队列
3. **处理队列**: 逐个移除节点并更新相邻节点的入度
4. **检测循环**: 如果剩余节点未被处理，说明存在循环依赖

### 算法实现

```go
// TopologicalSort 拓扑排序算法
func (g *N8NWorkflowGraph) TopologicalSort() ([]string, error) {
    // 构建依赖关系
    dependencies := g.BuildDependencyGraph()

    // 计算每个节点的入度
    inDegree := make(map[string]int)
    for node := range g.nodeNameMap {
        inDegree[node] = 0
    }

    for _, deps := range dependencies {
        for _, dep := range deps {
            inDegree[dep]++
        }
    }

    // 初始化队列（入度为 0 的节点）
    queue := []string{}
    for node, degree := range inDegree {
        if degree == 0 {
            queue = append(queue, node)
        }
    }

    // 拓扑排序结果
    result := []string{}

    // 处理队列
    for len(queue) > 0 {
        // 取出队首节点
        current := queue[0]
        queue = queue[1:]
        result = append(result, current)

        // 更新相邻节点的入度
        for _, neighbor := range dependencies[current] {
            inDegree[neighbor]--
            if inDegree[neighbor] == 0 {
                queue = append(queue, neighbor)
            }
        }
    }

    // 检查是否存在循环依赖
    if len(result) != len(g.nodeNameMap) {
        return nil, fmt.Errorf("检测到循环依赖，无法确定执行顺序")
    }

    return result, nil
}

// BuildDependencyGraph 构建依赖关系图
func (g *N8NWorkflowGraph) BuildDependencyGraph() map[string][]string {
    dependencies := make(map[string][]string)

    // 遍历所有连接关系
    for sourceNode, targetNodes := range g.nextNodesMap {
        for _, targetNode := range targetNodes {
            // targetNode 依赖于 sourceNode
            if dependencies[targetNode] == nil {
                dependencies[targetNode] = []string{}
            }
            dependencies[targetNode] = append(dependencies[targetNode], sourceNode)
        }
    }

    return dependencies
}
```

### 循环依赖检测

```go
// DetectCycles 检测循环依赖
func (g *N8NWorkflowGraph) DetectCycles() error {
    // 使用 DFS 检测循环
    visited := make(map[string]bool)
    recStack := make(map[string]bool)

    for node := range g.nodeNameMap {
        if !visited[node] {
            if g.hasCycleDFS(node, visited, recStack) {
                return fmt.Errorf("检测到循环依赖，涉及节点: %s", node)
            }
        }
    }

    return nil
}

// hasCycleDFS 使用深度优先搜索检测循环
func (g *N8NWorkflowGraph) hasCycleDFS(node string, visited, recStack map[string]bool) bool {
    // 标记当前节点为已访问和在递归栈中
    visited[node] = true
    recStack[node] = true

    // 遍历所有相邻节点
    if nextNodes, exists := g.nextNodesMap[node]; exists {
        for _, nextNode := range nextNodes {
            if !visited[nextNode] {
                if g.hasCycleDFS(nextNode, visited, recStack) {
                    return true
                }
            } else if recStack[nextNode] {
                return true
            }
        }
    }

    // 从递归栈中移除当前节点
    recStack[node] = false
    return false
}
```

## ⚡ 节点执行引擎

### 执行流程

```
节点执行流程
┌─────────────────┐
│  1. 输入准备     │
│  - 合并前置数据   │
│  - 验证参数       │
│  - 准备上下文     │
└─────────────────┘
          ↓
┌─────────────────┐
│  2. 节点执行     │
│  - 调用活动接口   │
│  - 处理业务逻辑   │
│  - 异常处理       │
└─────────────────┘
          ↓
┌─────────────────┐
│  3. 结果处理     │
│  - 验证输出       │
│  - 记录日志       │
│  - 更新上下文     │
└─────────────────┘
```

### 核心执行函数

```go
// ExecuteNode 执行单个节点
func ExecuteNode(ctx workflow.Context, node N8NNode, inputData map[string]interface{}) (*NodeResult, error) {
    logger := workflow.GetLogger(ctx)

    // 准备输入数据
    activityInput := prepareActivityInput(node, inputData)

    // 选择执行器
    switch node.Type {
    case "n8n-nodes-base.manualTrigger":
        return executeManualTrigger(ctx, activityInput)
    case "n8n-nodes-base.code":
        return executePythonCode(ctx, activityInput)
    case "n8n-nodes-base.if":
        return executeIfCondition(ctx, activityInput)
    case "n8n-nodes-base.switch":
        return executeSwitch(ctx, activityInput)
    case "CUSTOM.asmDomainResolve":
        return executeDomainResolve(ctx, activityInput)
    default:
        return executeCustomNode(ctx, activityInput)
    }
}

// prepareActivityInput 准备活动输入数据
func prepareActivityInput(node N8NNode, inputData map[string]interface{}) *activity.NodeInput {
    return &activity.NodeInput{
        NodeID:      node.ID,
        NodeName:    node.Name,
        NodeType:    node.Type,
        InputData:   inputData,
        Parameters:  node.Parameters,
        WorkflowID:  workflowID,
        ExecutionID: executionID,
    }
}
```

### 并行执行优化

```go
// ExecuteParallelNodes 并行执行无依赖关系的节点
func ExecuteParallelNodes(ctx workflow.Context, nodes []N8NNode, inputData map[string]interface{}) ([]*NodeResult, error) {
    logger := workflow.GetLogger(ctx)

    // 创建 Future 列表
    futures := make([]workflow.Future, len(nodes))
    results := make([]*NodeResult, len(nodes))

    // 并行启动所有节点
    for i, node := range nodes {
        node := node // 创建局部变量
        inputData := inputData // 创建局部变量

        futures[i] = workflow.ExecuteActivity(ctx, executeNodeActivityOptions, node, inputData)
    }

    // 等待所有节点完成
    for i, future := range futures {
        var result NodeResult
        if err := future.Get(ctx, &result); err != nil {
            logger.Error("节点执行失败",
                "nodeId", nodes[i].ID,
                "nodeName", nodes[i].Name,
                "error", err,
            )
            result = NodeResult{
                NodeID: nodes[i].ID,
                Success: false,
                Error:   err.Error(),
            }
        }
        results[i] = &result
    }

    return results, nil
}
```

### 执行选项配置

```go
// executeNodeActivityOptions 节点执行选项
func executeNodeActivityOptions() workflow.ActivityOptions {
    return workflow.ActivityOptions{
        ScheduleToCloseTimeout: time.Minute * 10,  // 调度到关闭超时
        ScheduleToStartTimeout: time.Minute * 1,   // 调度到开始超时
        StartToCloseTimeout:    time.Minute * 5,   // 开始到关闭超时
        HeartbeatTimeout:       time.Minute * 1,   // 心跳超时
        RetryPolicy: &temporal.RetryPolicy{
            InitialInterval:    time.Second * 1,
            BackoffCoefficient: 2.0,
            MaximumInterval:    time.Second * 30,
            MaximumAttempts:    3,
        },
    }
}
```

## 🌊 数据流管理

### 数据合并策略

```go
// MergeInputData 合并多个前置节点的输出数据
func MergeInputData(nodeResults map[string]*NodeResult, targetNode string) map[string]interface{} {
    // 获取目标节点的所有前置节点
    previousNodes := getPreviousNodes(targetNode)

    if len(previousNodes) == 0 {
        // 没有前置节点，使用初始数据
        return getInitialData()
    }

    // 合并所有前置节点的输出
    mergedData := make(map[string]interface{})

    for _, prevNode := range previousNodes {
        if result, exists := nodeResults[prevNode]; exists && result.Success {
            // 合并输出数据
            for key, value := range result.Data {
                // 处理键冲突
                if existingValue, exists := mergedData[key]; exists {
                    // 如果键已存在，转换为数组
                    if arrayValue, ok := existingValue.([]interface{}); ok {
                        mergedData[key] = append(arrayValue, value)
                    } else {
                        mergedData[key] = []interface{}{existingValue, value}
                    }
                } else {
                    mergedData[key] = value
                }
            }
        }
    }

    return mergedData
}

// PrepareNodeInput 准备节点输入数据
func PrepareNodeInput(ctx workflow.Context, node N8NNode, executionContext *ExecutionContext) (map[string]interface{}, error) {
    // 获取节点的所有前置节点
    previousNodes := getPreviousNodesForNode(node.Name)

    if len(previousNodes) == 0 {
        // 起始节点，使用触发数据
        return executionContext.TriggerData, nil
    }

    // 合并前置节点的输出数据
    input := make(map[string]interface{})

    for _, prevNodeName := range previousNodes {
        if prevResult, exists := executionContext.NodeResults[prevNodeName]; exists {
            // 合并数据
            mergeData(input, prevResult.Data)

            // 添加节点元数据
            input[fmt.Sprintf("from_%s", prevNodeName)] = prevResult.Data
        }
    }

    return input, nil
}

// mergeData 智能合并数据
func mergeData(target, source map[string]interface{}) {
    for key, value := range source {
        if existingValue, exists := target[key]; exists {
            // 智能处理键冲突
            switch v := value.(type) {
            case map[string]interface{}:
                // 如果是对象，递归合并
                if existingMap, ok := existingValue.(map[string]interface{}); ok {
                    mergeData(existingMap, v)
                    target[key] = existingMap
                } else {
                    target[key] = []interface{}{existingValue, v}
                }
            case []interface{}:
                // 如果是数组，连接数组
                if existingArray, ok := existingValue.([]interface{}); ok {
                    target[key] = append(existingArray, v...)
                } else {
                    target[key] = []interface{}{existingValue, v}
                }
            default:
                // 其他类型，创建数组
                target[key] = []interface{}{existingValue, v}
            }
        } else {
            target[key] = value
        }
    }
}
```

### 数据验证和转换

```go
// ValidateNodeData 验证节点数据
func ValidateNodeData(data map[string]interface{}, nodeType string) error {
    // 根据节点类型验证数据结构
    switch nodeType {
    case "n8n-nodes-base.if":
        return validateIfNodeData(data)
    case "n8n-nodes-base.switch":
        return validateSwitchNodeData(data)
    case "DNS.domainResolve":
        return validateDomainResolveData(data)
    default:
        return validateGenericNodeData(data)
    }
}

// TransformNodeData 转换节点数据格式
func TransformNodeData(data map[string]interface{}, targetFormat string) (map[string]interface{}, error) {
    switch targetFormat {
    case "json":
        return data, nil
    case "xml":
        return convertToXML(data)
    case "yaml":
        return convertToYAML(data)
    case "csv":
        return convertToCSV(data)
    default:
        return nil, fmt.Errorf("不支持的目标格式: %s", targetFormat)
    }
}
```

## 🔀 分支控制逻辑

### IF 节点分支处理

```go
// ProcessIfBranch 处理 IF 节点的分支选择
func ProcessIfBranch(result map[string]interface{}) (string, error) {
    // 获取匹配的规则索引
    matchedIndex, ok := result["matchedIndex"].(int)
    if !ok {
        return "", fmt.Errorf("无效的 matchedIndex")
    }

    // 根据 N8N 规则映射分支
    switch matchedIndex {
    case 0:
        return "main", nil // 条件为真，执行 main 分支
    case 1:
        return "alternative", nil // 条件为假，执行 alternative 分支
    default:
        return "", fmt.Errorf("无效的分支索引: %d", matchedIndex)
    }
}

// GetNextNodesForIf 获取 IF 节点的下一跳节点
func GetNextNodesForIf(node N8NNode, result map[string]interface{}, connections map[string]interface{}) ([]string, error) {
    branch, err := ProcessIfBranch(result)
    if err != nil {
        return nil, err
    }

    // 从连接关系中获取对应的下一跳节点
    return getNextNodesForBranch(node.Name, branch, connections), nil
}
```

### Switch 节点分支处理

```go
// ProcessSwitchBranch 处理 Switch 节点的分支选择
func ProcessSwitchBranch(result map[string]interface{}) (int, error) {
    // 获取匹配的规则索引
    matchedIndex, ok := result["matchedIndex"].(int)
    if !ok {
        return -1, fmt.Errorf("无效的 matchedIndex")
    }

    return matchedIndex, nil
}

// GetNextNodesForSwitch 获取 Switch 节点的下一跳节点
func GetNextNodesForSwitch(node N8NNode, result map[string]interface{}, connections map[string]interface{}) ([]string, error) {
    matchedIndex, err := ProcessSwitchBranch(result)
    if err != nil {
        return nil, err
    }

    // 从连接关系中获取对应的下一跳节点
    return getNextNodesForIndex(node.Name, matchedIndex, connections), nil
}

// getNextNodesForIndex 根据索引获取下一跳节点
func getNextNodesForIndex(nodeName string, index int, connections map[string]interface{}) []string {
    // 解析连接关系
    if nodeConnections, exists := connections[nodeName]; exists {
        if connectionsMap, ok := nodeConnections.(map[string]interface{}); ok {
            if mainConnections, exists := connectionsMap["main"]; exists {
                if mainList, ok := mainConnections.([]interface{}); ok && len(mainList) > 0 {
                    if firstGroup, ok := mainList[0].([]interface{}); ok {
                        // 根据索引选择节点
                        if index >= 0 && index < len(firstGroup) {
                            if conn, ok := firstGroup[index].(map[string]interface{}); ok {
                                if nextNode, exists := conn["node"]; exists {
                                    if nodeName, ok := nextNode.(string); ok {
                                        return []string{nodeName}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return []string{} // 返回空列表表示没有下一跳节点
}
```

## ⚠️ 错误处理机制

### 错误分类

```go
// ErrorType 错误类型枚举
type ErrorType string

const (
    ErrorTypeValidation    ErrorType = "validation"    // 输入验证错误
    ErrorTypeExecution     ErrorType = "execution"     // 执行错误
    ErrorTypeTimeout       ErrorType = "timeout"       // 超时错误
    ErrorTypeResource      ErrorType = "resource"      // 资源错误
    ErrorTypePermission    ErrorType = "permission"    // 权限错误
    ErrorTypeNetwork       ErrorType = "network"       // 网络错误
    ErrorTypeConfiguration ErrorType = "configuration" // 配置错误
)

// WorkflowError 工作流错误
type WorkflowError struct {
    Type      ErrorType `json:"type"`       // 错误类型
    Code      string    `json:"code"`       // 错误代码
    Message   string    `json:"message"`    // 错误消息
    NodeID    string    `json:"nodeId"`     // 节点ID
    NodeName  string    `json:"nodeName"`   // 节点名称
    Timestamp time.Time `json:"timestamp"`  // 错误时间
    Details   interface{} `json:"details"`  // 错误详情
    Retry     int       `json:"retry"`      // 重试次数
}

func (e *WorkflowError) Error() string {
    return fmt.Sprintf("[%s:%s] %s (node: %s)", e.Type, e.Code, e.Message, e.NodeName)
}
```

### 错误处理策略

```go
// HandleNodeError 处理节点错误
func HandleNodeError(ctx workflow.Context, node N8NNode, err error) (*NodeResult, error) {
    logger := workflow.GetLogger(ctx)

    // 分类错误
    workflowErr := classifyError(node, err)

    // 记录错误日志
    logger.Error("节点执行错误",
        "nodeId", node.ID,
        "nodeName", node.Name,
        "errorType", workflowErr.Type,
        "errorCode", workflowErr.Code,
        "errorMessage", workflowErr.Message,
        "errorDetails", workflowErr.Details,
    )

    // 根据错误类型决定处理策略
    switch workflowErr.Type {
    case ErrorTypeValidation:
        // 验证错误，不重试
        return &NodeResult{
            NodeID: node.ID,
            Success: false,
            Error:   workflowErr.Error(),
        }, nil

    case ErrorTypeTimeout, ErrorTypeNetwork:
        // 网络或超时错误，可以重试
        if workflowErr.Retry < 3 {
            return nil, workflowErr // 返回错误触发重试
        }
        return &NodeResult{
            NodeID: node.ID,
            Success: false,
            Error:   fmt.Sprintf("节点执行失败，已达到最大重试次数: %s", workflowErr.Message),
        }, nil

    default:
        // 其他错误，不重试
        return &NodeResult{
            NodeID: node.ID,
            Success: false,
            Error:   workflowErr.Error(),
        }, nil
    }
}

// classifyError 分类错误
func classifyError(node N8NNode, err error) *WorkflowError {
    errMsg := err.Error()
    timestamp := time.Now()

    // 根据错误消息分类
    if strings.Contains(errMsg, "validation") || strings.Contains(errMsg, "invalid") {
        return &WorkflowError{
            Type:      ErrorTypeValidation,
            Code:      "VALIDATION_ERROR",
            Message:   "输入数据验证失败",
            NodeID:    node.ID,
            NodeName:  node.Name,
            Timestamp: timestamp,
            Details:   errMsg,
        }
    }

    if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "deadline") {
        return &WorkflowError{
            Type:      ErrorTypeTimeout,
            Code:      "TIMEOUT_ERROR",
            Message:   "节点执行超时",
            NodeID:    node.ID,
            NodeName:  node.Name,
            Timestamp: timestamp,
            Details:   errMsg,
        }
    }

    if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "network") {
        return &WorkflowError{
            Type:      ErrorTypeNetwork,
            Code:      "NETWORK_ERROR",
            Message:   "网络连接错误",
            NodeID:    node.ID,
            NodeName:  node.Name,
            Timestamp: timestamp,
            Details:   errMsg,
        }
    }

    // 默认为执行错误
    return &WorkflowError{
        Type:      ErrorTypeExecution,
        Code:      "EXECUTION_ERROR",
        Message:   "节点执行错误",
        NodeID:    node.ID,
        NodeName:  node.Name,
        Timestamp: timestamp,
        Details:   errMsg,
    }
}
```

### 错误恢复机制

```go
// ErrorRecoveryStrategy 错误恢复策略
type ErrorRecoveryStrategy struct {
    MaxRetries    int           `json:"maxRetries"`    // 最大重试次数
    RetryDelay    time.Duration `json:"retryDelay"`    // 重试延迟
    BackoffFactor float64       `json:"backoffFactor"` // 退避因子
    FallbackNodes []string      `json:"fallbackNodes"` // 回退节点
}

// RecoverFromError 从错误中恢复
func RecoverFromError(ctx workflow.Context, node N8NNode, err error, strategy *ErrorRecoveryStrategy) (*NodeResult, error) {
    logger := workflow.GetLogger(ctx)

    // 检查是否可以使用回退节点
    if len(strategy.FallbackNodes) > 0 {
        logger.Info("尝试使用回退节点",
            "primaryNode", node.Name,
            "fallbackNodes", strategy.FallbackNodes,
        )

        // 执行第一个可用的回退节点
        for _, fallbackNodeName := range strategy.FallbackNodes {
            if fallbackNode := findNodeByName(fallbackNodeName); fallbackNode != nil {
                return ExecuteNode(ctx, *fallbackNode, getCurrentInputData())
            }
        }
    }

    // 检查是否可以重试
    if strategy.MaxRetries > 0 {
        logger.Info("准备重试节点",
            "node", node.Name,
            "retryDelay", strategy.RetryDelay,
        )

        // 等待重试延迟
        workflow.Sleep(ctx, strategy.RetryDelay)

        // 返回错误触发重试
        return nil, fmt.Errorf("节点执行失败，准备重试: %v", err)
    }

    // 无法恢复，返回错误结果
    return &NodeResult{
        NodeID: node.ID,
        Success: false,
        Error:   fmt.Sprintf("节点执行失败且无法恢复: %v", err),
    }, nil
}
```

## 🚀 性能优化策略

### 并行执行优化

```go
// ParallelExecutor 并行执行器
type ParallelExecutor struct {
    MaxConcurrency int           // 最大并发数
    Timeout        time.Duration // 超时时间
    Semaphore      chan struct{} // 信号量
}

// NewParallelExecutor 创建并行执行器
func NewParallelExecutor(maxConcurrency int, timeout time.Duration) *ParallelExecutor {
    return &ParallelExecutor{
        MaxConcurrency: maxConcurrency,
        Timeout:        timeout,
        Semaphore:      make(chan struct{}, maxConcurrency),
    }
}

// ExecuteNodes 并行执行节点列表
func (e *ParallelExecutor) ExecuteNodes(ctx workflow.Context, nodes []N8NNode, inputData map[string]interface{}) ([]*NodeResult, error) {
    logger := workflow.GetLogger(ctx)

    // 按依赖关系分组
    groups := e.groupNodesByDependency(nodes)

    var allResults []*NodeResult

    // 按组顺序执行，组内并行执行
    for i, group := range groups {
        logger.Info("执行节点组",
            "groupIndex", i,
            "groupSize", len(group),
            "maxConcurrency", e.MaxConcurrency,
        )

        // 并行执行当前组
        groupResults, err := e.executeGroup(ctx, group, inputData)
        if err != nil {
            return nil, fmt.Errorf("节点组 %d 执行失败: %v", i, err)
        }

        allResults = append(allResults, groupResults...)

        // 合并当前组的输出数据作为下一组的输入
        inputData = e.mergeGroupResults(groupResults)
    }

    return allResults, nil
}

// groupNodesByDependency 按依赖关系分组节点
func (e *ParallelExecutor) groupNodesByDependency(nodes []N8NNode) [][]N8NNode {
    // 使用拓扑排序算法分组
    // 返回的每组中的节点可以并行执行

    // 构建依赖图
    dependencies := buildDependencyGraph(nodes)

    // 计算每个节点的入度
    inDegree := make(map[string]int)
    for _, node := range nodes {
        inDegree[node.Name] = 0
    }

    for _, deps := range dependencies {
        for _, dep := range deps {
            inDegree[dep]++
        }
    }

    // 分组算法
    var groups [][]N8NNode
    remainingNodes := make([]N8NNode, len(nodes))
    copy(remainingNodes, nodes)

    for len(remainingNodes) > 0 {
        var currentGroup []N8NNode
        var nextRound []N8NNode

        for _, node := range remainingNodes {
            if inDegree[node.Name] == 0 {
                currentGroup = append(currentGroup, node)
            } else {
                nextRound = append(nextRound, node)
            }
        }

        if len(currentGroup) == 0 {
            // 没有可执行的节点，说明存在循环依赖
            break
        }

        groups = append(groups, currentGroup)

        // 更新剩余节点的入度
        for _, node := range currentGroup {
            for _, dependent := range getDependents(node.Name, dependencies) {
                inDegree[dependent]--
            }
        }

        remainingNodes = nextRound
    }

    return groups
}
```

### 内存优化

```go
// MemoryOptimizer 内存优化器
type MemoryOptimizer struct {
    MaxDataSize   int64         // 最大数据大小
    CleanupPolicy CleanupPolicy // 清理策略
}

// CleanupPolicy 清理策略
type CleanupPolicy struct {
    KeepResults      bool          // 是否保留结果
    ResultTTL        time.Duration // 结果保留时间
    CompressData     bool          // 是否压缩数据
    StreamLargeData  bool          // 是否流式处理大数据
}

// OptimizeData 优化数据结构
func (m *MemoryOptimizer) OptimizeData(data map[string]interface{}) (map[string]interface{}, error) {
    optimized := make(map[string]interface{})

    for key, value := range data {
        // 递归优化嵌套结构
        optimizedValue, err := m.optimizeValue(value)
        if err != nil {
            return nil, fmt.Errorf("优化字段 %s 失败: %v", key, err)
        }
        optimized[key] = optimizedValue
    }

    return optimized, nil
}

// optimizeValue 优化单个值
func (m *MemoryOptimizer) optimizeValue(value interface{}) (interface{}, error) {
    switch v := value.(type) {
    case map[string]interface{}:
        return m.OptimizeData(v)
    case []interface{}:
        return m.optimizeArray(v)
    case string:
        return m.optimizeString(v)
    default:
        return v, nil
    }
}

// optimizeArray 优化数组
func (m *MemoryOptimizer) optimizeArray(array []interface{}) (interface{}, error) {
    if len(array) > 1000 {
        // 大数组使用流式处理
        return m.processLargeArray(array), nil
    }

    optimized := make([]interface{}, len(array))
    for i, item := range array {
        optimizedItem, err := m.optimizeValue(item)
        if err != nil {
            return nil, err
        }
        optimized[i] = optimizedItem
    }

    return optimized, nil
}

// optimizeString 优化字符串
func (m *MemoryOptimizer) optimizeString(str string) (string, error) {
    if len(str) > 1024*1024 { // 1MB
        // 大字符串压缩
        if m.CleanupPolicy.CompressData {
            return m.compressString(str)
        }
    }
    return str, nil
}
```

### 缓存策略

```go
// WorkflowCache 工作流缓存
type WorkflowCache struct {
    cache      map[string]*CacheEntry
    mutex      sync.RWMutex
    ttl        time.Duration
    maxSize    int
    cleanupInterval time.Duration
}

// CacheEntry 缓存条目
type CacheEntry struct {
    Key       string
    Value     interface{}
    ExpiresAt time.Time
    AccessCount int64
    LastAccess  time.Time
}

// NewWorkflowCache 创建工作流缓存
func NewWorkflowCache(ttl time.Duration, maxSize int) *WorkflowCache {
    cache := &WorkflowCache{
        cache:      make(map[string]*CacheEntry),
        ttl:        ttl,
        maxSize:    maxSize,
        cleanupInterval: time.Minute * 5,
    }

    // 启动清理协程
    go cache.startCleanup()

    return cache
}

// Get 获取缓存值
func (c *WorkflowCache) Get(key string) (interface{}, bool) {
    c.mutex.RLock()
    defer c.mutex.RUnlock()

    entry, exists := c.cache[key]
    if !exists {
        return nil, false
    }

    // 检查是否过期
    if time.Now().After(entry.ExpiresAt) {
        delete(c.cache, key)
        return nil, false
    }

    // 更新访问统计
    entry.AccessCount++
    entry.LastAccess = time.Now()

    return entry.Value, true
}

// Set 设置缓存值
func (c *WorkflowCache) Set(key string, value interface{}) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    // 检查缓存大小限制
    if len(c.cache) >= c.maxSize {
        c.evictLRU()
    }

    c.cache[key] = &CacheEntry{
        Key:         key,
        Value:       value,
        ExpiresAt:   time.Now().Add(c.ttl),
        AccessCount: 1,
        LastAccess:  time.Now(),
    }
}

// evictLRU 淘汰最少使用的条目
func (c *WorkflowCache) evictLRU() {
    var oldestKey string
    var oldestTime time.Time

    for key, entry := range c.cache {
        if oldestKey == "" || entry.LastAccess.Before(oldestTime) {
            oldestKey = key
            oldestTime = entry.LastAccess
        }
    }

    if oldestKey != "" {
        delete(c.cache, oldestKey)
    }
}
```

## 📊 监控和调试

### 执行指标收集

```go
// WorkflowMetrics 工作流指标
type WorkflowMetrics struct {
    NodeExecutionCount    map[string]int64         // 节点执行次数
    NodeExecutionDuration map[string]time.Duration  // 节点执行耗时
    NodeErrorCount       map[string]int64         // 节点错误次数
    WorkflowDuration     time.Duration            // 工作流总耗时
    ParallelExecutions   int64                    // 并行执行次数
    MemoryUsage          int64                    // 内存使用量
    mutex                sync.RWMutex
}

// RecordNodeExecution 记录节点执行
func (m *WorkflowMetrics) RecordNodeExecution(nodeType string, duration time.Duration, success bool) {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    m.NodeExecutionCount[nodeType]++
    m.NodeExecutionDuration[nodeType] += duration

    if !success {
        m.NodeErrorCount[nodeType]++
    }
}

// GetMetricsSummary 获取指标摘要
func (m *WorkflowMetrics) GetMetricsSummary() map[string]interface{} {
    m.mutex.RLock()
    defer m.mutex.RUnlock()

    summary := map[string]interface{}{
        "totalNodes":      len(m.NodeExecutionCount),
        "totalExecutions": m.getTotalExecutions(),
        "totalErrors":     m.getTotalErrors(),
        "avgDuration":     m.getAverageDuration(),
        "memoryUsage":     m.MemoryUsage,
        "parallelExecutions": m.ParallelExecutions,
    }

    return summary
}
```

### 调试工具

```go
// WorkflowDebugger 工作流调试器
type WorkflowDebugger struct {
    enabled    bool
    logger     log.Logger
    traceData  map[string]interface{}
    traceMutex sync.RWMutex
}

// EnableDebugging 启用调试
func (d *WorkflowDebugger) EnableDebugging(logger log.Logger) {
    d.enabled = true
    d.logger = logger
    d.traceData = make(map[string]interface{})
}

// TraceExecution 跟踪执行
func (d *WorkflowDebugger) TraceExecution(nodeName, step string, data interface{}) {
    if !d.enabled {
        return
    }

    d.traceMutex.Lock()
    defer d.traceMutex.Unlock()

    traceEntry := map[string]interface{}{
        "node":     nodeName,
        "step":     step,
        "data":     data,
        "timestamp": time.Now(),
    }

    key := fmt.Sprintf("%s_%s", nodeName, step)
    d.traceData[key] = traceEntry

    d.logger.Debug("执行跟踪",
        "node", nodeName,
        "step", step,
        "data", data,
    )
}

// GetTraceData 获取跟踪数据
func (d *WorkflowDebugger) GetTraceData() map[string]interface{} {
    d.traceMutex.RLock()
    defer d.traceMutex.RUnlock()

    // 返回数据的副本
    traceCopy := make(map[string]interface{})
    for k, v := range d.traceData {
        traceCopy[k] = v
    }

    return traceCopy
}

// ExportTrace 导出跟踪数据
func (d *WorkflowDebugger) ExportTrace(filename string) error {
    if !d.enabled {
        return errors.New("调试未启用")
    }

    traceData := d.GetTraceData()
    jsonData, err := json.MarshalIndent(traceData, "", "  ")
    if err != nil {
        return fmt.Errorf("序列化跟踪数据失败: %v", err)
    }

    return os.WriteFile(filename, jsonData, 0644)
}
```

---

## 🔗 相关文档

- [项目主文档](../README.md)
- [Activity 节点文档](ACTIVITY_NODES.md)
- [Worker 配置文档](WORKER_CONFIG.md)
- [API 参考文档](API_REFERENCE.md)

## 📞 技术支持

如有工作流引擎相关问题，请：

1. 查阅本文档中的算法说明
2. 检查单元测试和集成测试
3. 使用调试工具跟踪执行流程
4. 提交 GitHub Issue 获取支持

---

**最后更新**: 2023年10月18日
**文档版本**: v1.0.0