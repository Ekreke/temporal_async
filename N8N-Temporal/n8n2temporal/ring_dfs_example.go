package main

import (
	"fmt"
	"strings"
)

// 简化的节点结构（用于演示）
type SimpleNode struct {
	ID   string
	Name string
	Type string
}

// 简化的连接结构（用于演示）
type SimpleConnection struct {
	NodeName   string
	TargetNode string
}

// RingInfo 环信息结构
type RingInfo struct {
	EntryNode string
	Path      []string
}

// DFSContext 重构后的DFS遍历上下文
// 这个结构体封装了所有DFS遍历需要的状态，避免了函数参数过多的问题
type DFSContext struct {
	Visited map[string]bool               // 已访问的节点
	OnStack map[string]bool               // 当前DFS路径上的节点（在栈中）
	Parent  map[string]string             // 父节点映射，用于回溯构建环路径
	Nodes   map[string]SimpleNode         // 节点名称到节点定义的映射
	AdjList map[string][]SimpleConnection // 邻接表表示的图结构
	Rings   []RingInfo                    // 检测到的环列表
}

// NewDFSContext 创建新的DFS上下文
func NewDFSContext(nodes map[string]SimpleNode, adjList map[string][]SimpleConnection) *DFSContext {
	return &DFSContext{
		Visited: make(map[string]bool),
		OnStack: make(map[string]bool),
		Parent:  make(map[string]string),
		Nodes:   nodes,
		AdjList: adjList,
		Rings:   make([]RingInfo, 0),
	}
}

// dfsDetectRings 重构后的DFS环检测函数
// 使用方法接收者模式，将原本的6个参数封装到DFSContext结构体中
func (ctx *DFSContext) dfsDetectRings(startNode string) {
	// 定义内部递归DFS函数
	var dfs func(node string, path []string)
	dfs = func(node string, path []string) {
		// 标记当前节点为已访问和在栈中
		ctx.Visited[node] = true
		ctx.OnStack[node] = true
		path = append(path, node)

		// 遍历所有邻接节点
		for _, edge := range ctx.AdjList[node] {
			nextNode := edge.TargetNode

			// 如果节点不存在于节点映射中，跳过（防止无效节点）
			if _, exists := ctx.Nodes[nextNode]; !exists {
				continue
			}

			if !ctx.Visited[nextNode] {
				// 未访问过的节点，设置父节点并继续DFS
				ctx.Parent[nextNode] = node
				dfs(nextNode, path)
			} else if ctx.OnStack[nextNode] {
				// 发现环：当前节点指向一个已在DFS栈中的节点
				// 这表明找到了一个后向边，即环的存在

				ringPath := ctx.extractRingPath(nextNode, node, path)
				entryNode := ctx.findRingEntryNode(ringPath)

				ring := RingInfo{
					EntryNode: entryNode,
					Path:      ringPath,
				}

				ctx.Rings = append(ctx.Rings, ring)
			}
		}

		// 回溯：将当前节点从栈中移除
		ctx.OnStack[node] = false
		path = path[:len(path)-1]
	}

	// 开始DFS遍历
	dfs(startNode, []string{})
}

// extractRingPath 提取环路径
func (ctx *DFSContext) extractRingPath(cycleStart, cycleEnd string, currentPath []string) []string {
	// 从当前节点回溯到环的起始节点
	ringPath := []string{cycleStart}
	current := cycleEnd

	for current != cycleStart && current != "" {
		ringPath = append([]string{current}, ringPath...)
		current = ctx.Parent[current]
	}

	return ringPath
}

// findRingEntryNode 找到环的入口节点
func (ctx *DFSContext) findRingEntryNode(ringPath []string) string {
	if len(ringPath) == 0 {
		return ""
	}

	// 找到入度最多的节点作为入口节点
	inDegree := make(map[string]int)
	for _, node := range ringPath {
		inDegree[node] = 0
	}

	// 计算环内每个节点的入度
	for _, connections := range ctx.AdjList {
		for _, conn := range connections {
			target := conn.TargetNode
			source := conn.NodeName

			// 检查源和目标是否都在环内
			sourceInRing := false
			targetInRing := false
			for _, node := range ringPath {
				if node == source {
					sourceInRing = true
				}
				if node == target {
					targetInRing = true
				}
			}

			if sourceInRing && targetInRing {
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

// DetectAllRings 检测图中所有环的便利方法
func (ctx *DFSContext) DetectAllRings() []RingInfo {
	// 重置环列表
	ctx.Rings = make([]RingInfo, 0)

	// 重置访问状态（支持多次调用）
	ctx.Visited = make(map[string]bool)
	ctx.OnStack = make(map[string]bool)
	ctx.Parent = make(map[string]string)

	// 对每个未访问的节点执行DFS
	for nodeName := range ctx.Nodes {
		if !ctx.Visited[nodeName] {
			ctx.dfsDetectRings(nodeName)
		}
	}

	return ctx.Rings
}

// String 返回DFS上下文的字符串表示（用于调试）
func (ctx *DFSContext) String() string {
	var builder strings.Builder

	builder.WriteString("=== DFS Context ===\n")
	builder.WriteString(fmt.Sprintf("Total Nodes: %d\n", len(ctx.Nodes)))
	builder.WriteString(fmt.Sprintf("Visited Nodes: %d\n", len(ctx.Visited)))
	builder.WriteString(fmt.Sprintf("Rings Found: %d\n", len(ctx.Rings)))

	if len(ctx.Rings) > 0 {
		builder.WriteString("\nRing Details:\n")
		for i, ring := range ctx.Rings {
			builder.WriteString(fmt.Sprintf("  Ring %d: Entry='%s', Path=%v\n",
				i+1, ring.EntryNode, ring.Path))
		}
	}

	return builder.String()
}

func main() {
	fmt.Println("=== DFS环检测重构示例 ===")
	fmt.Println()

	// 演示重构前后的对比
	fmt.Println("原始函数签名（参数过多）:")
	fmt.Println("func dfsDetectRings(startNode string, adjList map[string][]NodeConnection,")
	fmt.Println("                    visited, onStack map[string]bool, parent map[string]string,")
	fmt.Println("                    nodeNameMap map[string]WkFLowNode) []RingInfo")
	fmt.Println("参数数量: 6个")
	fmt.Println()

	fmt.Println("重构后函数签名（结构体封装）:")
	fmt.Println("func (ctx *DFSContext) dfsDetectRings(startNode string)")
	fmt.Println("参数数量: 1个")
	fmt.Println()

	// 创建测试节点
	nodes := map[string]SimpleNode{
		"Start":   {ID: "start-1", Name: "Start", Type: "n8n-nodes-base.start"},
		"Process": {ID: "process-1", Name: "Process", Type: "n8n-nodes-base.pythonDocker"},
		"Check":   {ID: "check-1", Name: "Check", Type: "n8n-nodes-base.conditional"},
		"End":     {ID: "end-1", Name: "End", Type: "n8n-nodes-base.end"},
	}

	// 创建邻接表：Start -> Process -> Check -> Start (环)，Check -> End (出口)
	adjList := map[string][]SimpleConnection{
		"Start":   {{NodeName: "Start", TargetNode: "Process"}},
		"Process": {{NodeName: "Process", TargetNode: "Check"}},
		"Check": {
			{NodeName: "Check", TargetNode: "Start"}, // 环内连接
			{NodeName: "Check", TargetNode: "End"},   // 脱离环
		},
		"End": {}, // 结束节点
	}

	// 创建DFS上下文
	ctx := NewDFSContext(nodes, adjList)

	fmt.Println("=== 环检测示例 ===")

	// 检测所有环
	rings := ctx.DetectAllRings()

	fmt.Printf("检测到 %d 个环:\n", len(rings))
	for i, ring := range rings {
		fmt.Printf("环 %d:\n", i+1)
		fmt.Printf("  入口节点: %s\n", ring.EntryNode)
		fmt.Printf("  环路径: %s\n", strings.Join(ring.Path, " -> "))

		// 分析条件节点
		for _, nodeName := range ring.Path {
			if node, exists := nodes[nodeName]; exists {
				if strings.Contains(node.Type, "conditional") {
					fmt.Printf("  条件节点: %s (类型: %s)\n", nodeName, node.Type)
					break
				}
			}
		}
	}

	// 打印完整的上下文信息
	fmt.Println()
	fmt.Print(ctx.String())

	fmt.Println()
	fmt.Println("=== 重构优势总结 ===")
	fmt.Println("1. 参数数量从6个减少到1个")
	fmt.Println("2. 相关状态被封装在一个结构体中")
	fmt.Println("3. 更容易扩展新的状态字段")
	fmt.Println("4. 更好的可读性和可维护性")
	fmt.Println("5. 符合Go语言的接收者方法惯例")
	fmt.Println("6. 支持更复杂的操作（如多次调用、状态重置等）")
}
