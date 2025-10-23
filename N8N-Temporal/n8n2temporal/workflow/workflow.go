package workflow

// 将n8n中的json数据，转换成temporal的执行流程

import (
	"encoding/json"
	"errors"
	"fmt"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	nodepkg "n8n2temporal/node"
	"time"
)

// Input 工作流输入结构
type Input struct {
	TriggerData map[string]interface{} `json:"triggerData"`
}

// Output 工作流输出结构
type Output struct {
	Success bool                   `json:"success"`
	Results map[string]interface{} `json:"results"`
	Error   string                 `json:"error,omitempty"`
}

// NodeExecutionContext 节点执行上下文
type NodeExecutionContext struct {
	NodeID     string                 `json:"nodeId"`
	NodeName   string                 `json:"nodeName"`
	InputData  map[string]interface{} `json:"inputData"`
	OutputData map[string]interface{} `json:"outputData"`
}

// N8NWorkflow n8n 工作流定义结构
type N8NWorkflow struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Nodes       []N8NNode              `json:"nodes"`
	Connections map[string]interface{} `json:"connections"`
}

// N8NWorkflowGraph 封装n8n工作流图结构和解析逻辑
type N8NWorkflowGraph struct {
	workflow        *N8NWorkflow        // 原始工作流定义
	nextNodesMap    map[string][]string // 节点名称 -> 下一跳节点名称列表
	nodeNameMap     map[string]N8NNode  // 节点名称 -> 节点定义
	nodeIDMap       map[string]string   // 节点ID -> 节点名称
	dependencyGraph map[string][]string // 节点名称 -> 依赖的节点名称列表
}

// N8NNode n8n 节点定义结构
type N8NNode struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Position    []int                  `json:"position"`
	Parameters  map[string]interface{} `json:"parameters"`
	TypeVersion float64                `json:"typeVersion"`
}

// Connection 连接关系结构
type Connection struct {
	SourceNode string `json:"sourceNode"`
	TargetNode string `json:"targetNode"`
	SourcePort string `json:"sourcePort"`
	TargetPort string `json:"targetPort"`
}

// ExecutionResult 节点执行结果
type ExecutionResult struct {
	NodeID   string                 `json:"nodeId"`
	Success  bool                   `json:"success"`
	Data     map[string]interface{} `json:"data"`
	Error    string                 `json:"error,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ParseN8NWorkflow 解析 n8n 工作流 JSON
func ParseN8NWorkflow(workflowDefJSON string) (*N8NWorkflow, error) {
	var wkFlow N8NWorkflow
	err := json.Unmarshal([]byte(workflowDefJSON), &wkFlow)
	if err != nil {
		return nil, fmt.Errorf("解析工作流 JSON 失败: %v", err)
	}
	return &wkFlow, nil
}

// ParseFromJSON 从n8n JSON创建工作流图对象
func (g *N8NWorkflowGraph) ParseFromJSON(workflowDefJSON string) error {
	// 解析基础工作流定义
	workflowParse, err := ParseN8NWorkflow(workflowDefJSON)
	if err != nil {
		return fmt.Errorf("解析工作流 JSON 失败: %v", err)
	}

	// 存储原始工作流定义
	g.workflow = workflowParse

	// 解析连接关系
	nextNodesMap, err := parseConnections(workflowParse.Connections)
	if err != nil {
		return fmt.Errorf("解析连接关系失败: %v", err)
	}
	g.nextNodesMap = nextNodesMap

	// 构建节点名称映射
	g.nodeNameMap = make(map[string]N8NNode)
	g.nodeIDMap = make(map[string]string)
	for _, node := range workflowParse.Nodes {
		g.nodeNameMap[node.Name] = node
		g.nodeIDMap[node.ID] = node.Name
	}

	// 构建依赖图
	g.dependencyGraph, _ = buildDependencyGraph(workflowParse.Nodes, nextNodesMap)

	return nil
}

// GetNextNodes 根据节点返回下一跳节点数组
// 参数可以是节点名称(string)或节点对象(N8NNode)
func (g *N8NWorkflowGraph) GetNextNodes(node interface{}) ([]N8NNode, error) {
	var nodeName string

	// 根据参数类型获取节点名称
	switch n := node.(type) {
	case string:
		nodeName = n
	case N8NNode:
		nodeName = n.Name
	default:
		return nil, fmt.Errorf("无效的节点参数类型，期望 string 或 N8NNode")
	}

	// 获取下一跳节点名称列表
	nextNodeNames, exists := g.nextNodesMap[nodeName]
	if !exists {
		// 如果没有找到下一跳节点，返回空数组而不是错误
		return []N8NNode{}, nil
	}

	// 根据名称查找对应的节点对象
	var nextNodes []N8NNode
	for _, name := range nextNodeNames {
		if node, exists := g.nodeNameMap[name]; exists {
			nextNodes = append(nextNodes, node)
		}
	}

	return nextNodes, nil
}

// GetNextNodeNames 根据节点返回下一跳节点名称数组
// 参数可以是节点名称(string)或节点对象(N8NNode)
func (g *N8NWorkflowGraph) GetNextNodeNames(node interface{}) ([]string, error) {
	var nodeName string

	// 根据参数类型获取节点名称
	switch n := node.(type) {
	case string:
		nodeName = n
	case N8NNode:
		nodeName = n.Name
	default:
		return nil, fmt.Errorf("无效的节点参数类型，期望 string 或 N8NNode")
	}

	// 获取下一跳节点名称列表
	nextNodeNames, exists := g.nextNodesMap[nodeName]
	if !exists {
		// 如果没有找到下一跳节点，返回空数组而不是错误
		return []string{}, nil
	}

	// 返回副本以防止外部修改
	result := make([]string, len(nextNodeNames))
	copy(result, nextNodeNames)
	return result, nil
}

// GetNodeByName 根据节点名称获取节点对象
func (g *N8NWorkflowGraph) GetNodeByName(nodeName string) (N8NNode, bool) {
	node, exists := g.nodeNameMap[nodeName]
	return node, exists
}

// GetNodeByID 根据节点ID获取节点对象
func (g *N8NWorkflowGraph) GetNodeByID(nodeID string) (N8NNode, bool) {
	nodeName, exists := g.nodeIDMap[nodeID]
	if !exists {
		return N8NNode{}, false
	}
	return g.GetNodeByName(nodeName)
}

// GetAllNodes 获取所有节点
func (g *N8NWorkflowGraph) GetAllNodes() []N8NNode {
	nodes := make([]N8NNode, len(g.nodeNameMap))
	i := 0
	for _, node := range g.nodeNameMap {
		nodes[i] = node
		i++
	}
	return nodes
}

// GetStartNodes 获取起始节点（没有依赖的节点）
func (g *N8NWorkflowGraph) GetStartNodes() []N8NNode {
	var startNodes []N8NNode
	for nodeName, deps := range g.dependencyGraph {
		if len(deps) == 0 {
			if node, exists := g.nodeNameMap[nodeName]; exists {
				startNodes = append(startNodes, node)
			}
		}
	}
	return startNodes
}

// GetEndNodes 获取结束节点（没有下一跳的节点）
func (g *N8NWorkflowGraph) GetEndNodes() []N8NNode {
	endNodeNames := findEndNodesFromNextMap(g.nextNodesMap)
	var endNodes []N8NNode
	for _, nodeName := range endNodeNames {
		if node, exists := g.nodeNameMap[nodeName]; exists {
			endNodes = append(endNodes, node)
		}
	}
	return endNodes
}

// GetDependencyGraph 获取依赖图
func (g *N8NWorkflowGraph) GetDependencyGraph() map[string][]string {
	// 返回副本以防止外部修改
	depGraph := make(map[string][]string)
	for node, deps := range g.dependencyGraph {
		depsCopy := make([]string, len(deps))
		copy(depsCopy, deps)
		depGraph[node] = depsCopy
	}
	return depGraph
}

// NewN8NWorkflowGraph 创建新的工作流图对象
func NewN8NWorkflowGraph() *N8NWorkflowGraph {
	return &N8NWorkflowGraph{
		nextNodesMap:    make(map[string][]string),
		nodeNameMap:     make(map[string]N8NNode),
		nodeIDMap:       make(map[string]string),
		dependencyGraph: make(map[string][]string),
	}
}

// NewN8NWorkflowGraphFromJSON 从JSON创建工作流图对象的便捷函数
func NewN8NWorkflowGraphFromJSON(workflowDefJSON string) (*N8NWorkflowGraph, error) {
	graph := NewN8NWorkflowGraph()
	if err := graph.ParseFromJSON(workflowDefJSON); err != nil {
		return nil, err
	}
	return graph, nil
}

// parseConnections 解析连接关系，返回节点到下一跳节点的映射
//
// n8n 连接结构示例:
//
//	{
//	  "源节点名称": {
//	    "main": [                    // main 端口
//	      [                          // 分支1 (可能存在多个分支)
//	        {"node": "目标节点1"},    // 连接1
//	        {"node": "目标节点2"}     // 连接2
//	      ],
//	      [                          // 分支2 (IF节点的false分支等)
//	        {"node": "目标节点3"}
//	      ]
//	    ]
//	  }
//	}
func parseConnections(connections map[string]interface{}) (map[string][]string, error) {
	nextNodesMap := make(map[string][]string)

	for sourceNodeName, nodeConnections := range connections {
		// 1. 验证连接格式
		sourceConnections, ok := nodeConnections.(map[string]interface{})
		if !ok {
			continue // 跳过格式不正确的连接
		}

		// 2. 获取 main 端口连接
		mainConnections, exists := sourceConnections["main"]
		if !exists {
			continue // 跳过没有 main 端口的连接
		}

		// 3. 提取所有目标节点（处理多分支情况）
		targetNodes := extractTargetNodesFromMain(mainConnections)
		if len(targetNodes) > 0 {
			nextNodesMap[sourceNodeName] = targetNodes
		}
	}

	return nextNodesMap, nil
}

// extractTargetNodesFromMain 从 main 端口连接中提取目标节点名称
func extractTargetNodesFromMain(mainConnections interface{}) []string {
	var targetNodes []string

	mainArray, ok := mainConnections.([]interface{})
	if !ok {
		return targetNodes
	}

	for _, branch := range mainArray {
		branchTargets := extractTargetNodesFromBranch(branch)
		targetNodes = append(targetNodes, branchTargets...)
	}

	return targetNodes
}

// extractTargetNodesFromBranch 从分支中提取目标节点名称
func extractTargetNodesFromBranch(branch interface{}) []string {
	var targetNodes []string

	branchArray, ok := branch.([]interface{})
	if !ok {
		return targetNodes
	}

	for _, connection := range branchArray {
		targetNode := extractTargetNodeFromConnection(connection)
		if targetNode != "" {
			targetNodes = append(targetNodes, targetNode)
		}
	}

	return targetNodes
}

// extractTargetNodeFromConnection 从单个连接中提取目标节点名称
func extractTargetNodeFromConnection(connection interface{}) string {
	connMap, ok := connection.(map[string]interface{})
	if !ok {
		return ""
	}

	targetNode, ok := connMap["node"].(string)
	if !ok {
		return ""
	}

	return targetNode
}

// buildDependencyGraph 构建依赖图
func buildDependencyGraph(nodes []N8NNode, nextNodesMap map[string][]string) (map[string][]string, map[string]string) {
	// 节点ID到节点名称的映射
	nodeIDToName := make(map[string]string)
	for _, node := range nodes {
		nodeIDToName[node.ID] = node.Name
	}

	// 依赖图：节点 -> 其依赖的节点列表
	dependencyGraph := make(map[string][]string)

	// 首先初始化所有节点
	for _, node := range nodes {
		dependencyGraph[node.Name] = []string{}
	}

	// 根据连接关系构建依赖图（反向依赖）
	for sourceNode, targetNodes := range nextNodesMap {
		for _, targetNode := range targetNodes {
			// 目标节点依赖于源节点
			if deps, exists := dependencyGraph[targetNode]; exists {
				dependencyGraph[targetNode] = append(deps, sourceNode)
			}
		}
	}

	return dependencyGraph, nodeIDToName
}

// topologicalSort 拓扑排序，支持最大步数限制
//
// 算法说明：
//  1. 使用改进的Kahn算法实现拓扑排序，智能处理循环依赖
//  2. dependencyGraph 表示节点依赖关系：key节点依赖于value数组中的所有节点
//     例如 {"C": ["A", "B"]} 表示C依赖于A和B，执行顺序为 A -> B -> C
//  3. 支持最大步数限制，防止无限循环
//  4. 循环依赖处理策略：
//     - 检测循环依赖对（A↔B）
//     - 优先保留重要节点的依赖关系（如域名解析）
//     - 打破循环，让Kahn算法能正常工作
//     - 严格按照拓扑排序执行，保持依赖约束
//
// 参数：
//   - dependencyGraph: 依赖图，key为节点名，value为该节点依赖的节点列表
//   - maxStep: 最大执行步数限制
//
// 返回值：
//   - []string: 拓扑排序后的节点列表
//   - error: 错误信息（当前实现总是返回nil）
func topologicalSort(dependencyGraph map[string][]string, maxStep int) ([]string, error) {
	// 首先检测循环依赖对
	cyclicPairs := detectCyclicPairs(dependencyGraph)

	// 创建修改后的依赖图，打破循环依赖
	modifiedDependencyGraph := breakCycles(dependencyGraph, cyclicPairs)

	// 使用修改后的依赖图进行正常的拓扑排序
	result, err := standardTopologicalSort(modifiedDependencyGraph, maxStep)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// breakCycles 打破循环依赖
func breakCycles(dependencyGraph map[string][]string, cyclicPairs map[string]bool) map[string][]string {
	modifiedGraph := make(map[string][]string)

	// 如果没有循环依赖，直接返回原图
	if len(cyclicPairs) == 0 {
		for node, deps := range dependencyGraph {
			modifiedGraph[node] = append([]string{}, deps...)
		}
		return modifiedGraph
	}

	for node, deps := range dependencyGraph {
		var newDeps []string
		for _, dep := range deps {
			// 检查是否是循环依赖
			isCyclic := cyclicPairs[node] && cyclicPairs[dep]

			if isCyclic {
				// 对于特定的循环依赖，优先保留重要节点的依赖关系
				// 域名解析节点比Code节点更重要，所以保留Code依赖于域名解析的关系
				// 打破域名解析依赖于Code的关系
				if node == "域名解析" && contains(dep, []string{"Code", "code", "Python", "python"}) {
					// 跳过这个依赖，打破循环
					continue
				}
				if dep == "域名解析" && contains(node, []string{"Code", "code", "Python", "python"}) {
					// 保留这个依赖关系
					newDeps = append(newDeps, dep)
					continue
				}

				// 对于其他循环依赖，按节点名称排序，保留字典序较小的节点依赖于较大的节点的关系
				if node < dep {
					// 跳过这个依赖
					continue
				}
			}

			// 保留非循环依赖或需要保留的循环依赖
			newDeps = append(newDeps, dep)
		}
		modifiedGraph[node] = newDeps
	}

	return modifiedGraph
}

// standardTopologicalSort 标准的拓扑排序算法
func standardTopologicalSort(dependencyGraph map[string][]string, maxStep int) ([]string, error) {
	// 计算每个节点的入度
	inDegree := make(map[string]int)
	for node, deps := range dependencyGraph {
		inDegree[node] = len(deps)
	}

	// 使用队列进行拓扑排序（Kahn算法）
	var queue []string
	var result []string

	// 找到所有入度为0的节点
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	// 开始拓扑排序
	for len(queue) > 0 && len(result) < maxStep {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// 更新依赖于当前节点的节点的入度
		for node, deps := range dependencyGraph {
			for _, dep := range deps {
				if dep == current {
					inDegree[node]--
					if inDegree[node] == 0 {
						queue = append(queue, node)
					}
				}
			}
		}
	}

	return result, nil
}

// detectCyclicPairs 检测循环依赖对（包括复杂循环）
func detectCyclicPairs(dependencyGraph map[string][]string) map[string]bool {
	cyclicPairs := make(map[string]bool)

	// 首先检测直接的双向循环
	for node, deps := range dependencyGraph {
		for _, dep := range deps {
			// 检查是否存在反向依赖
			if reverseDeps, exists := dependencyGraph[dep]; exists {
				for _, reverseDep := range reverseDeps {
					if reverseDep == node {
						// 找到循环依赖对 node <-> dep
						cyclicPairs[node] = true
						cyclicPairs[dep] = true
					}
				}
			}
		}
	}

	// 如果没有直接循环，检查是否有剩余节点未处理
	if len(cyclicPairs) == 0 {
		// 尝试进行一次拓扑排序，看是否还有剩余节点
		testGraph := make(map[string][]string)
		for node, deps := range dependencyGraph {
			testGraph[node] = append([]string{}, deps...)
		}

		// 简单的拓扑排序
		inDegree := make(map[string]int)
		for node, deps := range testGraph {
			inDegree[node] = len(deps)
		}

		// 找到入度为0的节点
		var queue []string
		for node, degree := range inDegree {
			if degree == 0 {
				queue = append(queue, node)
			}
		}

		// 处理入度为0的节点
		processed := 0
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			processed++

			// 更新依赖于当前节点的节点的入度
			for node, deps := range testGraph {
				for _, dep := range deps {
					if dep == current {
						inDegree[node]--
						if inDegree[node] == 0 {
							queue = append(queue, node)
						}
					}
				}
			}
		}

		// 如果处理的节点数少于总节点数，说明存在循环
		if processed < len(dependencyGraph) {
			// 将所有未处理的节点标记为循环节点
			for node := range dependencyGraph {
				degree := inDegree[node]
				if degree > 0 {
					cyclicPairs[node] = true
				}
			}
		}
	}

	return cyclicPairs
}

// containsNode 检查节点是否在节点列表中
func containsNode(nodes []string, target string) bool {
	for _, node := range nodes {
		if node == target {
			return true
		}
	}
	return false
}

// contains 检查字符串是否包含数组中的任意一个子串
func contains(str string, substrings []string) bool {
	for _, substr := range substrings {
		if len(str) >= len(substr) {
			// 简单的包含检查
			for i := 0; i <= len(str)-len(substr); i++ {
				if str[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}

// topologicalSortWithDefault 使用默认最大步数的拓扑排序
func topologicalSortWithDefault(dependencyGraph map[string][]string) ([]string, error) {
	const defaultMaxStep = 1000 // 默认最大步数
	return topologicalSort(dependencyGraph, defaultMaxStep)
}

// executeNode 执行单个节点
func executeNode(ctx workflow.Context, node N8NNode, inputData map[string]interface{}) (*ExecutionResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行节点", "nodeId", node.ID, "nodeName", node.Name, "nodeType", node.Type)

	result := &ExecutionResult{
		NodeID:   node.ID,
		Success:  false,
		Data:     make(map[string]interface{}),
		Metadata: make(map[string]interface{}),
	}

	// 设置活动选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// 根据节点类型选择相应的活动
	var err error
	switch node.Type {
	case "n8n-nodes-base.webhook":
		//case "n8n-nodes-base.webhook":
		// 触发节点，直接返回输入数据
		result.Success = true
		result.Data = inputData

	case "n8n-nodes-base.code":
		// Python 代码执行节点
		var pythonResult map[string]interface{}
		pythonActivity := nodepkg.NewPythonCodeActivity()
		err = workflow.ExecuteActivity(ctx, pythonActivity.ExecutePythonCode, inputData).Get(ctx, &pythonResult)
		if err == nil {
			result.Success = true
			result.Data = pythonResult
		}

	case "n8n-nodes-base.if":
		// IF 条件节点
		var conditionResult map[string]interface{}
		conditionActivity := nodepkg.NewConditionCheckActivity()
		err = workflow.ExecuteActivity(ctx, conditionActivity.ExecuteIf, inputData).Get(ctx, &conditionResult)
		if err == nil {
			result.Success = true
			result.Data = conditionResult
		}

	case "n8n-nodes-base.switch":
		// Switch 分支节点
		var switchResult map[string]interface{}
		switchActivity := nodepkg.NewSwitchNodeActivity()
		err = workflow.ExecuteActivity(ctx, switchActivity.ExecuteSwitchNode, inputData).Get(ctx, &switchResult)
		if err == nil {
			result.Success = true
			result.Data = switchResult
		}
	// 自定义节点：my custom node
	case "CUSTOM.myCustomNode":
		var customResult map[string]interface{}
		customActivity := nodepkg.NewCustomNodeActivity()
		logger.Info("customActivity.NodeInfo.Version", customActivity.GetNodeInfo().Version)
		// 将节点类型传递给activity
		inputDataWithNode := map[string]interface{}{
			"nodeType":   "CUSTOM.myCustomNode111",
			"inputData":  inputData["inputData"],
			"parameters": inputData["parameters"],
		}
		err = workflow.ExecuteActivity(ctx, customActivity.ExecuteCustomNode, inputDataWithNode).Get(ctx, &customResult)
		if err == nil {
			result.Success = true
			result.Data = customResult
		}
	// 自定义节点：域名解析节点
	case "CUSTOM.asmDomainResolve":
		var domainResult map[string]interface{}
		domainActivity := nodepkg.NewDomainResolveActivity()
		err = workflow.ExecuteActivity(ctx, domainActivity.ExecuteDomainResolve, inputData).Get(ctx, &domainResult)
		if err == nil {
			result.Success = true
			result.Data = domainResult
		}
	default:
		err = errors.New("未定义节点")
	}

	if err != nil {
		result.Error = err.Error()
		logger.Error("节点执行失败", "nodeId", node.ID, "error", err)
	} else {
		logger.Info("节点执行成功", "nodeId", node.ID)
	}

	// 添加元数据
	result.Metadata["nodeName"] = node.Name
	result.Metadata["nodeType"] = node.Type
	result.Metadata["executedAt"] = time.Now()

	return result, err
}

// GenericWorkflowWithMaxStep 是带最大步数限制的通用 Temporal 工作流定义
func GenericWorkflowWithMaxStep(ctx workflow.Context, workflowDefJSON string, initialData map[string]interface{}, maxStep int) (map[string]interface{}, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行通用 n8n 工作流", "maxStep", maxStep)

	// 1. 创建并解析工作流图对象
	var graph N8NWorkflowGraph
	if err := graph.ParseFromJSON(workflowDefJSON); err != nil {
		return nil, fmt.Errorf("解析工作流定义失败: %v", err)
	}
	// 记录解析结果，便于调试
	logger.Info("工作流图解析完成", "nodeCount", len(graph.GetAllNodes()))

	// 2. 获取依赖图
	dependencyGraph := graph.GetDependencyGraph()

	// 3. 拓扑排序确定执行顺序（支持循环）
	executionOrder, err := topologicalSort(dependencyGraph, maxStep)
	if err != nil {
		return nil, fmt.Errorf("构建执行顺序失败: %v", err)
	}
	logger.Info("工作流执行顺序", "order", executionOrder)

	// 4. 创建执行上下文和数据存储
	executionResults := make(map[string]*ExecutionResult)
	nodeData := make(map[string]map[string]interface{})
	nodeExecutionCount := make(map[string]int) // 记录每个节点的执行次数

	// 5. 按顺序执行节点，限制最大步数
	stepCount := 0
	for _, nodeName := range executionOrder {
		stepCount++
		if stepCount > maxStep {
			logger.Info("达到最大执行步数限制", "maxStep", maxStep, "executedSteps", stepCount)
			break
		}
		nodeExecutionCount[nodeName]++
		// 准备输入数据
		inputData := initialData
		if len(dependencyGraph[nodeName]) > 0 {
			// 如果有依赖节点，合并依赖节点的输出数据
			mergedData := make(map[string]interface{})
			for _, depName := range dependencyGraph[nodeName] {
				if depResult, exists := executionResults[depName]; exists && depResult.Success {
					for k, v := range depResult.Data {
						mergedData[k] = v
					}
				}
			}
			if len(mergedData) > 0 {
				inputData = mergedData
			}
		}
		// 从图中获取节点定义
		currentNode, found := graph.GetNodeByName(nodeName)
		if !found {
			return nil, fmt.Errorf("找不到节点定义: %s", nodeName)
		}
		logger.Info("执行节点", "step", stepCount, "nodeName", nodeName, "executionCount", nodeExecutionCount[nodeName])
		// 执行节点
		result, err := executeNode(ctx, currentNode, inputData)
		if err != nil {
			logger.Error("节点执行失败", "nodeName", nodeName, "error", err)
			// 继续执行其他节点，但记录错误
		}
		executionResults[nodeName] = result
		nodeData[nodeName] = result.Data
		// 处理特殊节点类型的分支逻辑
		if currentNode.Type == "n8n-nodes-base.if" && result.Success {
			conditionMet, ok := result.Data["conditionMet"].(bool)
			if ok {
				logger.Info("IF 节点条件判断", "nodeName", nodeName, "conditionMet", conditionMet)
				// 这里可以根据条件结果过滤后续执行的节点
			}
		}

		if currentNode.Type == "n8n-nodes-base.switch" && result.Success {
			selectedBranch, ok := result.Data["selectedBranch"].(string)
			if ok {
				logger.Info("Switch 节点分支选择", "nodeName", nodeName, "selectedBranch", selectedBranch)
				// 这里可以根据选择的分支确定后续执行路径
			}
		}
	}

	// 6. 收集最终结果
	finalResult := make(map[string]interface{})
	finalResult["success"] = true
	finalResult["executedNodes"] = len(executionResults)
	finalResult["maxStep"] = maxStep
	finalResult["actualSteps"] = stepCount
	finalResult["nodeExecutionCounts"] = nodeExecutionCount

	// 从图中获取工作流信息
	allNodes := graph.GetAllNodes()
	if len(allNodes) > 0 {
		finalResult["workflowId"] = allNodes[0].ID // 从第一个节点获取工作流ID信息
	}

	// 收集所有节点的执行结果
	nodeResults := make(map[string]interface{})
	for nodeName, result := range executionResults {
		nodeResult := map[string]interface{}{
			"success": result.Success,
			"data":    result.Data,
		}
		if result.Error != "" {
			nodeResult["error"] = result.Error
		}
		nodeResults[nodeName] = nodeResult
	}
	finalResult["nodeResults"] = nodeResults

	// 使用图对象获取结束节点的输出作为主要结果
	endNodes := graph.GetEndNodes()
	if len(endNodes) > 0 {
		for _, endNode := range endNodes {
			if result, exists := executionResults[endNode.Name]; exists && result.Success {
				finalResult["finalOutput"] = result.Data
				break // 只取第一个结束节点的结果
			}
		}
	}

	logger.Info("通用 n8n 工作流执行完成", "totalNodes", len(executionResults))
	return finalResult, nil
}

// findEndNodesFromNextMap 从下一跳节点映射找到结束节点
func findEndNodesFromNextMap(nextNodesMap map[string][]string) []string {
	// 找到所有作为源节点的节点
	sourceNodes := make(map[string]bool)
	for sourceNode := range nextNodesMap {
		sourceNodes[sourceNode] = true
	}

	// 找到所有作为目标节点的节点
	targetNodes := make(map[string]bool)
	for _, targetNodeList := range nextNodesMap {
		for _, targetNode := range targetNodeList {
			targetNodes[targetNode] = true
		}
	}

	// 结束节点是作为源节点但不是目标节点的节点
	var endNodes []string
	for node := range sourceNodes {
		if !targetNodes[node] {
			endNodes = append(endNodes, node)
		}
	}

	// 如果没有找到结束节点，可能工作流只有一个节点或者连接关系为空
	if len(endNodes) == 0 {
		// 返回所有没有下一跳的节点
		for sourceNode, targetNodeList := range nextNodesMap {
			if len(targetNodeList) == 0 {
				endNodes = append(endNodes, sourceNode)
			}
		}
	}

	return endNodes
}
