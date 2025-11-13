package wkGraph

import (
	"fmt"
	nodepkg "n8n2temporal/node"
	"slices"
	"strings"
)

// RingInfo 环信息结构
type RingInfo struct {
	EntryNode       string   // 环的入口节点
	Path            []string // 环的路径
	HasConditional  bool     // 环中是否包含条件节点
	CanExit         bool     // 是否能够脱离环
	ConditionalNode string   // 条件节点名称（如果存在）
}

// RingDetectionResult 环检测结果
type RingDetectionResult struct {
	HasRings         bool                 // 是否存在环
	Rings            []RingInfo           // 所有环的信息
	AllNodes         []nodepkg.WkFLowNode // 所有涉及的节点
	ConditionalRings []RingInfo           // 包含条件节点的环
}

// NodeConnection 节点连接信息
type NodeConnection struct {
	NodeName   string
	BranchName string
	TargetNode string
}

// DetectRings 检测工作流中的环
func (g *WkFlowGraph) DetectRings() *RingDetectionResult {
	// 初始化环检测结果
	result := &RingDetectionResult{
		HasRings: false,
		Rings:    []RingInfo{},
		AllNodes: []nodepkg.WkFLowNode{},
	}
	// 构建邻接表表示有向图（表示从每个节点出发，能够到达的下级节点）
	adjList := buildAdjacencyList(g.Workflow.Connections)
	// 获取所有节点，并排序确保确定性
	allNodes := g.GetAllNodes()
	//allNodes := make([]string, 0, len(g.nodeNameMap))
	//for nodeName := range g.nodeNameMap {
	//	allNodes = append(allNodes, nodeName)
	//}
	// 对节点名称进行排序，确保DFS遍历顺序一致
	for i := 0; i < len(allNodes); i++ {
		for j := i + 1; j < len(allNodes); j++ {
			if allNodes[i].Name > allNodes[j].Name {
				allNodes[i], allNodes[j] = allNodes[j], allNodes[i]
			}
		}
	}
	result.AllNodes = allNodes
	// 使用DFS检测环
	visited := make(map[string]bool)
	onStack := make(map[string]bool)
	parent := make(map[string]string)
	// 创建DFS上下文，传入外部result.Rings的引用
	dfs := NewDFSContext(adjList, visited, onStack, parent, g.nodeNameMap, &result.Rings)
	for _, node := range allNodes {
		if !visited[node.Name] {
			dfs.dfsDetectRings(node.Name)
		}
	}
	// 分析每个环的条件节点情况
	for i := range result.Rings {
		ring := &result.Rings[i]
		analyzeRingConditions(ring, g.nodeNameMap, adjList)
		if ring.HasConditional && ring.CanExit {
			result.ConditionalRings = append(result.ConditionalRings, *ring)
		}
	}
	result.HasRings = len(result.Rings) > 0
	// 在响应之前确保数组排序的位置一致，以便于workflow重新执行时，可以复现逻辑
	slices.SortFunc(result.AllNodes, func(a, b nodepkg.WkFLowNode) int {
		return strings.Compare(a.Name, b.Name)
	})
	slices.SortFunc(result.Rings, func(a, b RingInfo) int {
		return strings.Compare(a.EntryNode, b.EntryNode)
	})
	slices.SortFunc(result.ConditionalRings, func(a, b RingInfo) int {
		return strings.Compare(a.EntryNode, b.EntryNode)
	})
	return result
}

// buildAdjacencyList 构建邻接表
func buildAdjacencyList(connections WkFlowConn) map[string][]NodeConnection {
	adjList := make(map[string][]NodeConnection)

	for nodeName, branchMap := range connections {
		// 先收集所有连接，然后排序确保确定性
		var allEdges []NodeConnection
		for branchName, conns := range branchMap {
			if len(conns) > 0 && len(conns[0]) > 0 {
				for _, conn := range conns[0] {
					edge := NodeConnection{
						NodeName:   nodeName,
						BranchName: branchName,
						TargetNode: conn.NodeName,
					}
					allEdges = append(allEdges, edge)
				}
			}
		}

		// 对连接进行排序：先按分支名排序，再按目标节点排序
		for i := 0; i < len(allEdges); i++ {
			for j := i + 1; j < len(allEdges); j++ {
				edgeI, edgeJ := allEdges[i], allEdges[j]
				// 比较分支名
				if edgeI.BranchName > edgeJ.BranchName ||
					(edgeI.BranchName == edgeJ.BranchName && edgeI.TargetNode > edgeJ.TargetNode) {
					allEdges[i], allEdges[j] = allEdges[j], allEdges[i]
				}
			}
		}

		adjList[nodeName] = allEdges
	}

	return adjList
}

// DFSContext DFS递归遍历
type DFSContext struct {
	Visited     map[string]bool
	OnStack     map[string]bool
	Parent      map[string]string
	NodeNameMap map[string]nodepkg.WkFLowNode
	AdjList     map[string][]NodeConnection
	Rings       *[]RingInfo // 指向外部环结果的指针，确保同步
}

// NewDFSContext 初始化DFS定义
func NewDFSContext(adjList map[string][]NodeConnection, visited, onStack map[string]bool, parent map[string]string,
	nodeNameMap map[string]nodepkg.WkFLowNode, result *[]RingInfo) *DFSContext {
	return &DFSContext{
		Visited:     visited,
		OnStack:     onStack,
		Parent:      parent,
		NodeNameMap: nodeNameMap,
		AdjList:     adjList,
		Rings:       result,
	}
}

// dfsDetectRings 重构后的DFS环检测
func (d *DFSContext) dfsDetectRings(startNode string) {
	var dfs func(node string, path []string)
	dfs = func(node string, path []string) {
		// 标记当前节点为已访问和在栈中
		d.Visited[node] = true
		d.OnStack[node] = true
		path = append(path, node)
		// 遍历所有邻接节点
		for _, edge := range d.AdjList[node] {
			nextNode := edge.TargetNode
			// 如果节点不存在于节点映射中，跳过
			if _, exists := d.NodeNameMap[nextNode]; !exists {
				continue
			}
			if !d.Visited[nextNode] {
				// 未访问过的节点，设置父节点并继续DFS
				d.Parent[nextNode] = node
				dfs(nextNode, path)
			} else if d.OnStack[nextNode] {
				// 发现环：当前节点指向一个已在DFS栈中的节点
				ringPath := extractRingPath(nextNode, node, d.Parent, path)
				entryNode := findRingEntryNode(ringPath, d.AdjList)
				ring := RingInfo{
					EntryNode: entryNode,
					Path:      ringPath,
				}
				*d.Rings = append(*d.Rings, ring)
			}
		}
		// 回溯：将当前节点从栈中移除
		d.OnStack[node] = false
		path = path[:len(path)-1]
	}
	// 开始DFS遍历
	dfs(startNode, []string{})
}

// extractRingPath 提取环路径
func extractRingPath(cycleStart, cycleEnd string, parent map[string]string, currentPath []string) []string {
	// 从当前节点回溯到环的起始节点
	ringPath := []string{cycleStart}
	current := cycleEnd

	for current != cycleStart && current != "" {
		ringPath = append([]string{current}, ringPath...)
		current = parent[current]
	}

	return ringPath
}

// findRingEntryNode 找到环的入口节点
func findRingEntryNode(ringPath []string, adjList map[string][]NodeConnection) string {
	if len(ringPath) == 0 {
		return ""
	}

	// 找到入度最多的节点作为入口节点
	inDegree := make(map[string]int)
	for _, node := range ringPath {
		inDegree[node] = 0
	}

	// 计算环内每个节点的入度（只计算环内边的贡献）
	for _, connections := range adjList {
		for _, conn := range connections {
			target := conn.TargetNode
			source := conn.NodeName

			// 如果源和目标都在环内
			inRing := false
			targetInRing := false

			for _, node := range ringPath {
				if node == source {
					inRing = true
				}
				if node == target {
					targetInRing = true
				}
			}

			if inRing && targetInRing {
				inDegree[target]++
			}
		}
	}

	// 返回入度最多的节点
	maxDegree := -1
	entryNode := ringPath[0]

	for _, node := range ringPath {
		if inDegree[node] > maxDegree {
			maxDegree = inDegree[node]
			entryNode = node
		}
	}

	return entryNode
}

// analyzeRingConditions 分析环的条件节点情况
func analyzeRingConditions(ring *RingInfo, nodeNameMap map[string]nodepkg.WkFLowNode, adjList map[string][]NodeConnection) {
	// 检查环中是否包含条件节点，并检查所有条件节点的脱离能力
	hasAnyConditional := false
	anyCanExit := false
	var conditionalNodes []string

	for _, nodeName := range ring.Path {
		node, exists := nodeNameMap[nodeName]
		if !exists {
			continue
		}
		// 只检查包含 "conditional" 的节点类型
		if strings.Contains(node.Type, "conditional") {
			hasAnyConditional = true
			conditionalNodes = append(conditionalNodes, nodeName)

			// 检查当前条件节点是否有能够脱离环的出口
			if canExitRing(nodeName, ring.Path, adjList, nodeNameMap) {
				anyCanExit = true
			}
		}
	}

	ring.HasConditional = hasAnyConditional
	ring.CanExit = anyCanExit
	if len(conditionalNodes) > 0 {
		ring.ConditionalNode = conditionalNodes[0] // 记录第一个条件节点
	}
}

// canExitRing 检查条件节点是否能够脱离环
func canExitRing(conditionalNode string, ringPath []string, adjList map[string][]NodeConnection, nodeNameMap map[string]nodepkg.WkFLowNode) bool {
	// 构建环内节点集合
	ringNodeSet := make(map[string]bool)
	for _, node := range ringPath {
		ringNodeSet[node] = true
	}
	// 检查条件节点的所有出口
	for _, edge := range adjList[conditionalNode] {
		targetNode := edge.TargetNode
		// 如果目标节点不在环内，说明可以脱离环
		if !ringNodeSet[targetNode] {
			// 确保目标节点存在于节点映射中
			if _, exists := nodeNameMap[targetNode]; exists {
				return true
			}
		}
	}
	return false
}

// PrintRingDetectionResult 打印环检测结果
func (r *RingDetectionResult) PrintRingDetectionResult() string {
	var builder strings.Builder

	builder.WriteString("=== 环检测结果 ===\n")
	builder.WriteString(fmt.Sprintf("是否存在环: %v\n", r.HasRings))
	builder.WriteString(fmt.Sprintf("发现环数量: %d\n", len(r.Rings)))
	builder.WriteString(fmt.Sprintf("包含条件节点的环数量: %d\n", len(r.ConditionalRings)))

	if len(r.Rings) > 0 {
		builder.WriteString("\n=== 环详细信息 ===\n")
		for i, ring := range r.Rings {
			builder.WriteString(fmt.Sprintf("\n环 %d:\n", i+1))
			builder.WriteString(fmt.Sprintf("  入口节点: %s\n", ring.EntryNode))
			builder.WriteString(fmt.Sprintf("  路径: %s\n", strings.Join(ring.Path, " -> ")))
			builder.WriteString(fmt.Sprintf("  包含条件节点: %v\n", ring.HasConditional))
			if ring.HasConditional {
				builder.WriteString(fmt.Sprintf("  条件节点: %s\n", ring.ConditionalNode))
				builder.WriteString(fmt.Sprintf("  可脱离环: %v\n", ring.CanExit))
			}
		}
	}

	if len(r.ConditionalRings) > 0 {
		builder.WriteString("\n=== 符合条件的环（包含条件节点且可脱离）===\n")
		for i, ring := range r.ConditionalRings {
			builder.WriteString(fmt.Sprintf("\n符合条件的环 %d:\n", i+1))
			builder.WriteString(fmt.Sprintf("  入口节点: %s\n", ring.EntryNode))
			builder.WriteString(fmt.Sprintf("  条件节点: %s\n", ring.ConditionalNode))
			builder.WriteString(fmt.Sprintf("  路径: %s\n", strings.Join(ring.Path, " -> ")))
		}
	}

	return builder.String()
}

// ValidateRings 验证所有环是否符合条件（包含条件节点且可脱离）
func (r *RingDetectionResult) ValidateRings() bool {
	// 如果没有环，返回true
	if !r.HasRings {
		return true
	}

	// 检查每个环是否符合条件
	for _, ring := range r.Rings {
		if !ring.HasConditional || !ring.CanExit {
			return false
		}
	}

	return true
}

// GetInvalidRings 获取不符合条件的环
func (r *RingDetectionResult) GetInvalidRings() []RingInfo {
	var invalidRings []RingInfo

	for _, ring := range r.Rings {
		if !ring.HasConditional || !ring.CanExit {
			invalidRings = append(invalidRings, ring)
		}
	}

	return invalidRings
}
