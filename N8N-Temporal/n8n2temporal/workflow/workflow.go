package workflow

// 将n8n中的json数据，转换成temporal的执行流程

import (
	"context"
	"fmt"
	"n8n2temporal/consts"
	nodepkg "n8n2temporal/node"
	"n8n2temporal/notify"
	"strings"
	"sync/atomic"
	"time"

	"github.acme.red/wego/pkg/utils/arrayutil/v2"
	"github.com/bytedance/sonic"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type A struct {
	Main [][]struct {
		Node  string `json:"node"`
		Type  string `json:"type"`
		Index int    `json:"index"`
	} `json:"main"`
}

// Workflow 工作流定义结构
type Workflow struct {
	ID          string               `json:"id"`          // 工作流ID，全局唯一
	Name        string               `json:"name"`        // 工作流名称，由用户自定义
	Nodes       []nodepkg.WkFLowNode `json:"nodes"`       // 工作流所有节点
	Connections WkFlowConn           `json:"connections"` // 工作流节点关系，key表示当前节点，value表示后续节点的名称
	Active      bool                 `json:"active"`      // 是否激活 -- 只有激活的节点才能通过平台发起调用
	VersionId   string               `json:"version_id"`  // 工作流版本ID
}

type WkFlowConn map[string]map[string][][]struct {
	NodeName string `json:"node"`
	Type     string `json:"type"`
	Index    int    `json:"index"`
}

// WkFlowGraph 封装工作流图结构和解析逻辑
type WkFlowGraph struct {
	workflow *Workflow // 原始工作流定义
	//nextNodesMap    map[string][]string   // 节点名称 -> 下一跳节点名称列表 todo 这我就使用workflow中的connection就好
	nodeNameMap map[string]nodepkg.WkFLowNode // 节点名称 -> 节点定义
	nodeIDMap   map[string]string             // 节点ID -> 节点名称
	//dependencyGraph map[string][]string   // 节点名称 -> 依赖的节点名称列表
}

// NodeExecResult 节点执行结果
type NodeExecResult struct {
	NodeID   string                 `json:"node_id"`            // 节点ID
	NodeName string                 `json:"node_name"`          // 节点名称
	Success  bool                   `json:"success"`            // 执行成功与否
	Data     map[string]interface{} `json:"data"`               // 执行响应数据(不一定是节点的结果，只能说执行响应)
	Error    string                 `json:"error,omitempty"`    // 错误原因
	Metadata map[string]interface{} `json:"metadata,omitempty"` // 头数据，用于透传到下一个执行节点
}

// NewWkFlowGraph 初始化工作流图对象
func NewWkFlowGraph(workflowJson string) (*WkFlowGraph, error) {
	// 解析基础工作流定义
	var wkFlow *Workflow
	err := sonic.UnmarshalString(workflowJson, &wkFlow)
	if err != nil {
		return nil, fmt.Errorf("解析工作流 JSON 失败: %v", err)
	}
	// 存储原始工作流定义
	graph := &WkFlowGraph{
		workflow: wkFlow,
	}

	//// 解析连接关系
	//nextNodesMap, err := parseConnections(wkFlow.Connections)
	//if err != nil {
	//	return nil, fmt.Errorf("解析连接关系失败: %v", err)
	//}
	//graph.nextNodesMap = nextNodesMap

	// 构建依赖图
	//graph.dependencyGraph = buildDependencyGraph(wkFlow.Nodes, nextNodesMap)

	// 构建节点映射
	graph.nodeNameMap = make(map[string]nodepkg.WkFLowNode)
	graph.nodeIDMap = make(map[string]string)
	for _, node := range wkFlow.Nodes {
		// 节点检查
		if err := node.Check(); err != nil {
			return nil, err
		}
		graph.nodeNameMap[node.Name] = node
		graph.nodeIDMap[node.ID] = node.Name
	}

	return graph, nil
}

// GetNodeByName 根据节点名称获取节点对象
func (g *WkFlowGraph) GetNodeByName(nodeName string) (nodepkg.WkFLowNode, bool) {
	node, exists := g.nodeNameMap[nodeName]
	return node, exists
}

// GetAllNodes 获取所有节点 -- 返回副本
func (g *WkFlowGraph) GetAllNodes() []nodepkg.WkFLowNode {
	nodes := make([]nodepkg.WkFLowNode, len(g.nodeNameMap))
	i := 0
	for _, node := range g.nodeNameMap {
		nodes[i] = node
		i++
	}
	return nodes
}

// GetNextNodes 获取下一个节点
/*
* @param nodeName string 节点名称
* @param branch string 下个节点分支名称
* return nodepkg.WkFLowNode 节点对象
* return error 错误原因
 */
func (g *WkFlowGraph) GetNextNodes(nodeName string, branch string) ([]nodepkg.WkFLowNode, error) {
	var newNode = make([]nodepkg.WkFLowNode, 0)   // 默认响应
	branchMap := g.workflow.Connections[nodeName] // 获取当前节点的连接信息
	// 遍历所有分支
	for branchName, con := range branchMap {
		if branchName != branch {
			continue
		}
		// 正常情况第一层的长度一定是1,目前该层无特殊意义，兼容格式
		if len(con) < 1 || len(con[0]) < 1 {
			return newNode, consts.WorkFlowConnExcept
		}
		nextCon := con[0]
		// 遍历其分支下所有节点
		for _, nextConn := range nextCon {
			nextNode, ok := g.GetNodeByName(nextConn.NodeName)
			if !ok {
				return newNode, consts.WorkFlowNotFound
			}
			newNode = append(newNode, nextNode)
		}
	}
	return newNode, nil
}

//// GetDependencyGraph 获取依赖图 -- 返回副本 -- todo 待删除
//func (g *WkFlowGraph) GetDependencyGraph() map[string][]string {
//	// 返回副本以防止外部修改
//	depGraph := make(map[string][]string)
//	for node, deps := range g.dependencyGraph {
//		depsCopy := make([]string, len(deps))
//		copy(depsCopy, deps)
//		depGraph[node] = depsCopy
//	}
//	return depGraph
//}

// parseConnections 解析连接关系，返回节点到下一跳节点的映射 -- 这个暂时不需要  -- todo 待删除
/*
 * 连接结构示例:
{
  "节点名称": {
	"main": [                    // main 端口（默认为main），目前只会有一个分支，condition节点会有多个端口，对应多个分支
	  [                          // 分支1 (可能存在多个分支)
		{"node": "目标节点1"},    // 连接1
		{"node": "目标节点2"}     // 连接2
	  ],
	  [                          // 分支2 (IF节点的false分支等)
		{"node": "目标节点3"}
	  ]
	]
  }
}
*/
func parseConnections(connections WkFlowConn) (map[string][]string, error) {
	nextNodesMap := make(map[string][]string)
	// 解析连接信息
	for nodeName, conn := range connections {
		var targetNodes []string
		// 获取所有端口连接
		for _, branchConn := range conn {
			for _, branch := range branchConn {
				branchTargets := extractTargetNodesFromBranch(branch)
				targetNodes = append(targetNodes, branchTargets...)
			}
		}
		if len(targetNodes) > 0 {
			nextNodesMap[nodeName] = targetNodes
		}
	}
	return nextNodesMap, nil
}

// extractTargetNodesFromMain 从 main 端口连接中提取目标节点名称 -- todo 待删除
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

// extractTargetNodesFromBranch 从分支中提取目标节点名称 -- todo 待删除
func extractTargetNodesFromBranch(branch interface{}) []string {
	var targetNodes []string

	branchArray, ok := branch.([]interface{})
	if !ok {
		return targetNodes
	}

	for _, connection := range branchArray {
		// 处理n8n分支连接的嵌套数组结构
		if connectionArray, ok := connection.([]interface{}); ok {
			// 分支连接可能是嵌套数组：[ [ {node: "xxx"} ] ]
			for _, nestedConnection := range connectionArray {
				targetNode := extractTargetNodeFromConnection(nestedConnection)
				if targetNode != "" {
					targetNodes = append(targetNodes, targetNode)
				}
			}
		} else {
			// 直接的连接对象：{node: "xxx"}
			targetNode := extractTargetNodeFromConnection(connection)
			if targetNode != "" {
				targetNodes = append(targetNodes, targetNode)
			}
		}
	}

	return targetNodes
}

// extractTargetNodeFromConnection 从单个连接中提取目标节点名称 -- todo 待删除
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

// buildDependencyGraph 构建依赖图 -- todo 待删除
func buildDependencyGraph(nodes []nodepkg.WkFLowNode, nextNodesMap map[string][]string) map[string][]string {
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

	return dependencyGraph
}

// topologicalSort 拓扑排序，支持最大步数限制  -- todo 待删除
/*
*算法说明：
 1. 使用改进的Kahn算法实现拓扑排序，智能处理循环依赖
 2. dependencyGraph 表示节点依赖关系：key节点依赖于value数组中的所有节点
    例如 {"C": ["A", "B"]} 表示C依赖于A和B，执行顺序为 A -> B -> C
 3. 支持最大步数限制，防止无限循环
 4. 循环依赖处理策略：
    - 检测循环依赖对（A↔B）
    - 优先保留重要节点的依赖关系（如域名解析）
    - 打破循环，让Kahn算法能正常工作
    - 严格按照拓扑排序执行，保持依赖约束

参数：
  - dependencyGraph: 依赖图，key为节点名，value为该节点依赖的节点列表
  - maxStep: 最大执行步数限制

返回值：
  - []string: 拓扑排序后的节点列表
  - error: 错误信息（当前实现总是返回nil）
*/
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

// breakCycles 打破循环依赖  -- todo 待删除
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

// detectCyclicPairs 检测循环依赖对（包括复杂循环）   -- todo 待删除
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

// contains 检查字符串是否包含数组中的任意一个子串  -- todo 待删除
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

// executeNode 执行单个节点
func executeNode(ctx workflow.Context, node nodepkg.WkFLowNode, inputData map[string]interface{}, express *nodepkg.ExpressionEvaluator) (*NodeExecResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行节点", "nodeId", node.ID, "nodeName", node.Name, "nodeType", node.Type)
	result := &NodeExecResult{
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
	var err error                               // 节点执行错误
	var nodeResult = &nodepkg.ActivityOutput{}  // 节点执行结果
	var execNode ExecuteNode                    // 执行节点
	var activityInput = &nodepkg.ActivityInput{ // 节点输入参数
		NodeID:     node.ID,
		NodeName:   node.Name,
		NodeType:   node.Type,
		InputData:  inputData,
		Parameters: node.Parameters,
	}
	switch {
	// 开始节点
	case strings.HasSuffix(node.Type, ".start") || strings.HasSuffix(node.Type, ".manualTrigger"):
		execNode = ExecuteStartNode

	// 变量节点
	case strings.HasSuffix(node.Type, ".variable"):
		execNode = ExecuteVariableNode

	// Python代码执行节点
	case strings.HasSuffix(node.Type, ".pythonDocker") || strings.HasSuffix(node.Type, ".code"):
		// Python 代码执行节点 - 使用Python Docker节点
		execNode = ExecutePythonDockerNode

	// 条件判断节点
	case strings.HasSuffix(node.Type, ".condition") || strings.HasSuffix(node.Type, ".if") || strings.HasSuffix(node.Type, ".conditional"):
		// IF 条件节点 - 使用统一条件节点
		execNode = ExecuteConditionalNode

	// 域名解析节点
	case strings.HasSuffix(node.Type, ".domainResolve") || strings.HasSuffix(node.Type, ".asmDomainResolve") || strings.HasSuffix(node.Type, ".asm_domain_resolve"):
		execNode = ExecuteDomainResolve

	// 自定义节点 - 支持多种自定义节点类型 / 支持ASM相关自定义节点
	case strings.HasPrefix(node.Type, "CUSTOM.") || strings.HasSuffix(node.Type, ".customNode") || strings.Contains(node.Type, "asm_"):
		execNode = ExecuteCustomNode

	// 结束节点
	case strings.HasSuffix(node.Type, ".end"):
		execNode = ExecuteEndNode

	default:
		logger.Error("不支持的节点类型", "nodeType", node.Type, "nodeName", node.Name, "nodeId", node.ID)
	}
	// 执行实际节点
	ctxv, err := sonic.MarshalString(express.GetWorkflowContext())
	fmt.Printf("Before ExecuteActivity: express = %v, workflowContext = %v\n", ctxv, express.GetWorkflowContext() == nil)
	if execNode != nil {
		err = workflow.ExecuteActivity(ctx, execNode, activityInput, express).Get(ctx, &nodeResult)
	} else {
		err = fmt.Errorf("未定义节点类型: %s (节点名称: %s)", node.Type, node.Name)
	}

	// 统一处理响应
	if err != nil {
		result.Error = err.Error()
		logger.Error("节点执行失败", "nodeId", node.ID, "error", err)
	} else {
		result.Success = nodeResult.Success
		result.Data = nodeResult.Data
		logger.Info("节点执行成功", "nodeId", node.ID)
	}

	// 添加元数据
	result.Metadata["nodeName"] = node.Name
	result.Metadata["nodeType"] = node.Type
	result.Metadata["executedAt"] = time.Now()
	return result, err
}

// GenericWorkflowWithMaxStep 是带最大步数限制的通用 Temporal 工作流定义
func GenericWorkflowWithMaxStep(ctx workflow.Context, workflowJson string, initialData map[string]interface{}, maxStep int64) (map[string]interface{}, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行通用 n8n 工作流", "maxStep", maxStep)
	// 创建工作流图对象
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		return nil, fmt.Errorf("解析工作流定义失败: %v", err)
	}
	logger.Info("工作流图解析完成", "nodeCount", len(graph.GetAllNodes()))
	// 初始化全局变量对象
	express := nodepkg.NewExpressionEvaluator(nil)

	// 创建执行上下文和数据存储
	executionResults := make(map[string]*NodeExecResult) // 节点名称对应的执行结果
	nodeData := make(map[string]map[string]interface{})  // 节点执行结果相应data数据
	nodeExecutionCount := make(map[string]int)           // 记录每个节点的执行次数
	skippedNodes := 0

	// 创建可取消的上下文用于协程管理
	childCtx, cancelFunc := workflow.WithCancel(ctx)
	defer cancelFunc()
	// 获取类别
	var nodeTypes = make([]string, 0)
	for _, node := range graph.workflow.Nodes {
		nodeTypes = append(nodeTypes, node.Type)
	}
	nodeTypes = arrayutil.Duplicate(nodeTypes)
	// 异步信号监听器
	workflow.Go(childCtx, func(gCtx workflow.Context) {
		selector := workflow.NewSelector(gCtx)
		// 全局定义
		stepCount := atomic.Int64{}
		// 监听所有信号，且只注册一次
		for _, nodeType := range nodeTypes {
			nt := nodeType
			selector.AddReceive(workflow.GetSignalChannel(gCtx, nt), func(c workflow.ReceiveChannel, more bool) {
				// more标识信号通道是否关闭
				if !more {
					logger.Info(nt+"信号通道已关闭", "nodeType", nodeType)
					return
				}
				// 获取信号相关信息
				var input *notify.SignalInput
				c.Receive(gCtx, &input)
				if input == nil {
					logger.Error("信号数据解析失败")
					return
				}
				logger.Info("startNode收到信号", "value", input.NodeName)
				currentNode, exits := graph.GetNodeByName(input.NodeName)
				if !exits {
					logger.Error(fmt.Sprintf("找不到节点定义: %s", input.NodeName))
					return
				}
				// 获取代执行节点
				nodes, err := graph.GetNextNodes(currentNode.Name, input.Metadata.NextBranch)
				if err != nil {
					logger.Error("获取next节点失败", "error", err)
					return
				}
				logger.Info("待执行节点", "step", stepCount.Load(), "nodes", nodes)
				// 组装待执行节点的输入数据：上一个节点的输出（以节点名称为key） + initialData（开始节点携带的数据）
				inputData := initialData
				inputData[input.NodeName] = input.Data
				// 节点执行。开启并发，对于该节点的多个下级节点，应该是并发执行的
				//errGroup, _ := errgroup.WithContext(context.Background())
				//errGroup.SetLimit(len(nodes))
				for _, node := range nodes {
					//errGroup.Go(func() error { // todo 这里要改成兼容并发
					// 判断是否超过节点执行数量上限
					if stepCount.Load() >= maxStep {
						logger.Warn("over maxStep stop")
						return
					}
					// 实际执行节点
					stepCount.Add(1)
					result, err := executeNode(ctx, node, inputData, express)
					if err != nil {
						logger.Error("executeNode error", "error", err)
						return
					}
					// 针对于变量设置节点，更新workflowContext节点的数据
					if strings.Contains(node.Type, ".variable") && node.Parameters["operation"] == "set" {
						express.GetWorkflowContext().SetNodeData(nodepkg.ExpressVariablesNodeName, result.Data)
					}
					// 节点执行响应处理
					executionResults[node.Name] = result
					nodeData[node.Name] = result.Data
					//return nil
					//})
				}
				//if err := errGroup.Wait(); err != nil {
				//	logger.Error("节点执行失败", "nodeName", currentNode.Name, "error", err.Error())
				//	return
				//}
			})
		}
		// 循环监听，直到上下文取消
		for gCtx.Err() == nil {
			selector.Select(gCtx)
		}
		logger.Info("信号监听器退出")
	})

	// todo 启动所有节点，而且所有节点需要监听两类信号。一类是外部异步执行，他们会通过信号来响应；另一类是内部节点（例如Condition），这类执行完毕会直接推送到内部channel（就不用等待了）

	//// 获取依赖图
	//dependencyGraph := graph.GetDependencyGraph()
	//logger.Info("获取依赖图", "dependencyGraph", dependencyGraph)
	//// 拓扑排序确定执行顺序（支持循环检测） todo 这里的循环检测逻辑有问题，不能进行解环
	//executionOrder, err := topologicalSort(dependencyGraph, maxStep)
	//if err != nil {
	//	return nil, fmt.Errorf("构建执行顺序失败: %v", err)
	//}
	//logger.Info("工作流执行顺序", "order", executionOrder)
	//// 检测是否存在循环依赖 todo 这里的循环检测也需要修改
	//cyclicPairs := detectCyclicPairs(dependencyGraph)
	//if len(cyclicPairs) > 0 {
	//	logger.Warn("检测到循环依赖", "cyclicNodes", cyclicPairs, "count", len(cyclicPairs))
	//	// 如果循环依赖数量过多，提前终止
	//	if len(cyclicPairs) > len(dependencyGraph)/2 {
	//		return nil, fmt.Errorf("循环依赖过多，可能存在无限循环风险，终止执行")
	//	}
	//}

	// 6. 按顺序执行节点，限制最大步数
	// todo 这是按照顺序串行执行节点的逻辑，但实际我们要支持异步逻辑，所以应该是异步信号监听，每次传入信号，根据信号选择下一步要执行的节点，以及增加各种停止条件
	//stepCount := 0
	//selectedBranches := make(map[string]string) // 记录条件节点选择的分支
	//for _, nodeName := range executionOrder {
	//	// 6.1 检查是否应该跳过此节点（条件分支过滤）
	//	if shouldSkipNode(nodeName, selectedBranches, dependencyGraph) {
	//		logger.Info("跳过节点（未被选中的条件分支）", "nodeName", nodeName)
	//		skippedNodes++
	//		continue
	//	}
	//	// 6.2 检测节点执行次数是否异常（可能的循环）
	//	if nodeExecutionCount[nodeName] > 0 {
	//		// 如果节点执行次数超过阈值，可能是循环依赖导致的
	//		if nodeExecutionCount[nodeName] > 3 {
	//			logger.Warn("节点执行次数异常，可能存在循环", "nodeName", nodeName, "executionCount", nodeExecutionCount[nodeName])
	//			continue
	//		}
	//	}
	//	nodeExecutionCount[nodeName]++
	//	// 6.3 组装所有节点的输出数据 + 输入数据（input） todo 暂时关闭这里的逻辑，这里需要将逻辑优化，对于并发节点，需要开并发，同时执行（要判断是否有回归）
	//	inputData := initialData
	//	if len(dependencyGraph[nodeName]) > 1000 {
	//		// 如果有依赖节点，合并依赖节点的输出数据
	//		mergedData := make(map[string]interface{})
	//		// 是否所有依赖失败
	//		allDependenciesFailed := true
	//		for _, depName := range dependencyGraph[nodeName] {
	//			depResult, exists := executionResults[depName]
	//			if !exists {
	//				logger.Warn("依赖节点未找到执行结果1", "nodeName", nodeName, "dependency", depName)
	//				continue
	//			}
	//			if !depResult.Success {
	//				logger.Warn("依赖节点执行失败1", "nodeName", nodeName, "dependency", depName, "error", depResult.Error)
	//				continue
	//			}
	//			for k, v := range depResult.Data {
	//				if _, ok := mergedData[depResult.NodeID]; !ok {
	//					mergedData[depResult.NodeID] = make(map[string]interface{})
	//				}
	//				mergedData[depResult.NodeID] = map[string]interface{}{k: v}
	//			}
	//			allDependenciesFailed = false
	//		}
	//		// 如果所有依赖都失败了，跳过当前节点
	//		if allDependenciesFailed {
	//			logger.Warn("所有依赖节点均失败，跳过当前节点", "nodeName", nodeName)
	//			continue
	//		}
	//		// 合并input数据 + exec节点的执行数据
	//		if len(mergedData) > 0 {
	//			for mk, mv := range mergedData {
	//				inputData[mk] = mv
	//			}
	//		}
	//	}
	//	// 6.4 从图中获取节点定义
	//	currentNode, found := graph.GetNodeByName(nodeName)
	//	if !found {
	//		return nil, fmt.Errorf("找不到节点定义: %s", nodeName)
	//	}
	//	logger.Info("执行节点", "step", stepCount, "nodeName", nodeName, "executionCount", nodeExecutionCount[nodeName])
	//	// 6.5 执行节点
	//	result, err := executeNode(ctx, currentNode, inputData, express)
	//	if err != nil {
	//		logger.Error("节点执行失败", "nodeName", nodeName, "error", err.Error())
	//	} else if strings.HasSuffix(currentNode.Type, ".variable") && currentNode.Parameters["operation"] == "set" { // 针对于变量设置节点，变更workflowContext节点的数据
	//		express.GetWorkflowContext().SetNodeData(nodepkg.ExpressVariablesNodeName, result.Data)
	//	}
	//	executionResults[nodeName] = result // 节点执行响应
	//	nodeData[nodeName] = result.Data    // 节点执行对应数据
	//	// 6.7 处理条件判断节点执行成功 todo 我感觉这里不需要，等回过头来详细理逻辑
	//	if currentNode.Type == "n8n-nodes-base.conditional" && result.Success {
	//		outputPaths, ok := result.Data["outputPaths"].([]interface{})
	//		if ok && len(outputPaths) > 0 {
	//			if selectedBranch, ok := outputPaths[0].(string); ok {
	//				logger.Info("条件判断节点分支选择", "nodeName", nodeName, "selectedBranch", selectedBranch)
	//				selectedBranches[nodeName] = selectedBranch
	//			}
	//		}
	//	}
	//}

	// 7. 收集最终结果
	finalResult := make(map[string]interface{}) // 最终结果
	successfulNodes := 0                        // 执行成功节点次数
	failedNodes := 0                            // 执行失败节点次数
	for _, result := range executionResults {
		if result.Success {
			successfulNodes++
		} else {
			failedNodes++
		}
	}

	// 8. 计算跳过的节点
	//for _, nodeName := range executionOrder {
	//	if _, exists := executionResults[nodeName]; !exists {
	//		skippedNodes++
	//	}
	//}
	finalResult["success"] = failedNodes == 0
	finalResult["executedNodes"] = len(executionResults)
	finalResult["successfulNodes"] = successfulNodes
	finalResult["failedNodes"] = failedNodes
	finalResult["skippedNodes"] = skippedNodes
	finalResult["maxStep"] = maxStep
	//finalResult["actualSteps"] = stepCount
	finalResult["nodeExecutionCounts"] = nodeExecutionCount
	// 8.2. 循环依赖检测结果
	//if len(cyclicPairs) > 0 {
	//	finalResult["hasCyclicDependencies"] = true
	//	finalResult["cyclicNodes"] = cyclicPairs
	//	finalResult["cyclicNodeCount"] = len(cyclicPairs)
	//} else {
	//	finalResult["hasCyclicDependencies"] = false
	//}

	// 9. 从图中获取工作流信息
	allNodes := graph.GetAllNodes()
	if len(allNodes) > 0 {
		finalResult["workflowId"] = allNodes[0].ID // 从第一个节点获取工作流ID信息
		finalResult["workflowName"] = graph.workflow.Name
		finalResult["totalNodes"] = len(allNodes)
	}
	// 9.1. 收集所有节点的执行结果
	nodeResults := make(map[string]interface{})
	for nodeName, result := range executionResults {
		nodeResult := map[string]interface{}{
			"success": result.Success,
			"data":    result.Data,
		}
		if result.Error != "" {
			nodeResult["error"] = result.Error
		}
		if result.Metadata != nil {
			nodeResult["metadata"] = result.Metadata
		}
		nodeResults[nodeName] = nodeResult
	}
	finalResult["nodeResults"] = nodeResults
	// 9.2. 使用图对象获取结束节点的输出作为主要结果 todo 要考虑下怎么将执行结果传递给使用方，结束节点只能有一个
	//endNodes := graph.GetEndNodes()
	//if len(endNodes) > 0 {
	//	for _, endNode := range endNodes {
	//		if result, exists := executionResults[endNode.Name]; exists && result.Success {
	//			finalResult["finalOutput"] = result.Data
	//			finalResult["endNodeType"] = endNode.Type
	//			break // 只取第一个结束节点的结果
	//		}
	//	}
	//}
	// 9.3. 异常情况处理和警告信息
	if failedNodes > 0 {
		logger.Warn("工作流执行完成，但有节点失败", "failedNodes", failedNodes, "totalNodes", len(allNodes))
	}
	//if skippedNodes > 0 {
	//	logger.Info("部分节点被跳过", "skippedNodes", skippedNodes, "reason", "依赖失败或循环依赖")
	//}
	//if len(cyclicPairs) > 0 {
	//	logger.Warn("工作流包含循环依赖，但已安全处理", "cyclicNodeCount", len(cyclicPairs))
	//}
	logger.Info("通用 n8n 工作流执行完成",
		"totalNodes", len(executionResults),
		"successfulNodes", successfulNodes,
		"failedNodes", failedNodes,
		"skippedNodes", skippedNodes,
		//"hasCyclicDependencies", len(cyclicPairs) > 0,
	)
	return finalResult, nil
}

// ExecuteNode 执行节点格式定义
type ExecuteNode func(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error)

// ExecuteStartNode 开始节点活动
func ExecuteStartNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewStartNodeActivity(express).Execute(ctx, input)
}

// ExecuteEndNode 结束节点活动
func ExecuteEndNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewEndNodeActivity(express).Execute(ctx, input)
}

// ExecuteVariableNode 变量节点活动
func ExecuteVariableNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewVariableNodeActivity(express).Execute(ctx, input)
}

// ExecuteConditionalNode 条件节点活动
func ExecuteConditionalNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewConditionalNodeActivity(express).Execute(ctx, input)
}

// ExecutePythonDockerNode Python Docker节点活动
func ExecutePythonDockerNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewPythonDockerNodeActivity(express).Execute(ctx, input)
}

// ExecuteDomainResolve 域名解析节点活动
func ExecuteDomainResolve(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewDomainResolveActivity(express).Execute(ctx, input)
}

// ExecuteCustomNode 自定义节点活动
func ExecuteCustomNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewCustomNodeActivity(express).Execute(ctx, input)
}

// shouldSkipNode 判断节点是否应该被跳过（条件分支过滤）  -- todo 待删除
func shouldSkipNode(nodeName string, selectedBranches map[string]string, dependencyGraph map[string][]string) bool {
	// 检查节点的依赖中是否有条件节点
	for _, depName := range dependencyGraph[nodeName] {
		if selectedBranch, exists := selectedBranches[depName]; exists {
			// 如果依赖是条件节点，且已经选择了分支
			// 检查当前节点是否属于被选中的分支

			// 根据节点名称判断属于哪个分支
			if isNodeInSelectedBranch(nodeName, depName, selectedBranch) {
				// 节点属于选中的分支，不跳过
				return false
			} else {
				// 节点不属于选中的分支，跳过
				return true
			}
		}
	}

	// 没有条件节点依赖，不跳过
	return false
}

// isNodeInSelectedBranch 判断节点是否属于被选中的分支  -- todo 待删除
func isNodeInSelectedBranch(nodeName, conditionalNodeName, selectedBranch string) bool {
	// 根据节点名称和分支判断节点是否属于该分支
	// 这是一个简化的实现，实际应该基于连接关系来判断

	switch selectedBranch {
	case "premium-branch":
		return contains(nodeName, []string{"Python处理（高级用户）"})
	case "regular-branch":
		return contains(nodeName, []string{"Python处理（普通用户）"})
	case "invalid-user-branch":
		return contains(nodeName, []string{"结束节点"})
	case "true-branch":
		// IF节点的true分支，这里需要根据实际情况判断
		return false
	case "false-branch":
		// IF节点的false分支，这里需要根据实际情况判断
		return false
	default:
		// 未知分支，默认不跳过
		return false
	}
}
