package wkGraph

import (
	nodepkg "n8n2temporal/node"
	"testing"
)

func TestDetectRings_LinearWorkflow(t *testing.T) {
	// 测试线性工作流（无环）
	workflowJson := GetLinearWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if result.HasRings {
		t.Errorf("线性工作流不应该检测到环，但检测到了 %d 个环", len(result.Rings))
	}

	if len(result.Rings) != 0 {
		t.Errorf("线性工作流应该有 0 个环，但有 %d 个", len(result.Rings))
	}

	if len(result.ConditionalRings) != 0 {
		t.Errorf("线性工作流应该有 0 个条件环，但有 %d 个", len(result.ConditionalRings))
	}

	if !result.ValidateRings() {
		t.Error("线性工作流应该通过环验证")
	}
}

func TestDetectRings_SimpleRing(t *testing.T) {
	// 测试简单环（无条件节点）
	workflowJson := GetSimpleRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if !result.HasRings {
		t.Error("简单环工作流应该检测到环")
	}

	if len(result.Rings) == 0 {
		t.Error("应该检测到至少一个环")
	}

	// 检查环中是否包含条件节点
	hasConditional := false
	for _, ring := range result.Rings {
		if ring.HasConditional {
			hasConditional = true
			break
		}
	}

	if hasConditional {
		t.Error("简单环中不应该包含条件节点")
	}

	// 简单环应该无法通过验证（因为没有条件节点）
	if result.ValidateRings() {
		t.Error("简单环工作流不应该通过环验证（缺少条件节点）")
	}

	invalidRings := result.GetInvalidRings()
	if len(invalidRings) == 0 {
		t.Error("应该有无效的环")
	}
}

func TestDetectRings_ConditionalRing(t *testing.T) {
	// 测试包含条件节点的环（符合条件）
	workflowJson := GetConditionalRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if !result.HasRings {
		t.Error("条件环工作流应该检测到环")
	}

	if len(result.Rings) == 0 {
		t.Error("应该检测到至少一个环")
	}

	// 检查环中是否包含条件节点且能够脱离环
	hasValidConditionalRing := false
	for _, ring := range result.Rings {
		if ring.HasConditional && ring.CanExit && ring.ConditionalNode == "条件节点" {
			hasValidConditionalRing = true
			break
		}
	}

	if !hasValidConditionalRing {
		t.Error("应该检测到包含条件节点且能脱离环的有效环")
	}

	// 条件环应该能通过验证
	if !result.ValidateRings() {
		t.Error("条件环工作流应该通过环验证")
	}

	if len(result.ConditionalRings) == 0 {
		t.Error("应该有至少一个符合条件的环")
	}

	invalidRings := result.GetInvalidRings()
	if len(invalidRings) != 0 {
		t.Errorf("不应该有无效的环，但有 %d 个", len(invalidRings))
	}
}

func TestDetectRings_MultiRing(t *testing.T) {
	// 测试多环工作流
	workflowJson := GetMultiRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if !result.HasRings {
		t.Error("多环工作流应该检测到环")
	}

	if len(result.Rings) < 2 {
		t.Errorf("应该检测到至少 2 个环，但只检测到 %d 个", len(result.Rings))
	}

	// 多环工作流应该无法通过验证（因为没有条件节点）
	if result.ValidateRings() {
		t.Error("多环工作流不应该通过环验证（缺少条件节点）")
	}

	invalidRings := result.GetInvalidRings()
	if len(invalidRings) != len(result.Rings) {
		t.Errorf("所有环都应该是无效的，但只有 %d/%d 个无效", len(invalidRings), len(result.Rings))
	}
}

func TestDetectRings_NestedRing(t *testing.T) {
	// 测试嵌套环工作流
	workflowJson := GetNestedRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if !result.HasRings {
		t.Error("嵌套环工作流应该检测到环")
	}

	if len(result.Rings) < 2 {
		t.Errorf("应该检测到至少 2 个环（内环和外环），但只检测到 %d 个", len(result.Rings))
	}

	// 嵌套环应该检测到2个环
	if len(result.Rings) < 2 {
		t.Errorf("应该检测到至少 2 个环，但只有 %d 个", len(result.Rings))
	}

	// 检查是否有条件节点
	hasConditional := false
	for _, ring := range result.Rings {
		if ring.HasConditional {
			hasConditional = true
			break
		}
	}

	if !hasConditional {
		t.Error("应该检测到包含条件节点的环")
	}

	// 嵌套环应该能部分通过验证（至少有一个符合条件的环）
	if len(result.ConditionalRings) < 1 {
		t.Errorf("应该有至少 1 个符合条件的环，但只有 %d 个", len(result.ConditionalRings))
	}
}

func TestDetectRings_InvalidConditionalRing(t *testing.T) {
	// 测试包含条件节点但无法脱离环的工作流
	workflowJson := GetInvalidConditionalRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if !result.HasRings {
		t.Error("无效条件环工作流应该检测到环")
	}

	if len(result.Rings) == 0 {
		t.Error("应该检测到至少一个环")
	}

	// 检查是否有条件节点但无法脱离环
	hasInvalidConditionalRing := false
	for _, ring := range result.Rings {
		if ring.HasConditional && !ring.CanExit {
			hasInvalidConditionalRing = true
			break
		}
	}

	if !hasInvalidConditionalRing {
		t.Error("应该检测到包含条件节点但无法脱离环的无效环")
	}

	// 无效条件环应该无法完全通过验证（因为存在无效环）
	invalidRings := result.GetInvalidRings()
	if len(invalidRings) == 0 {
		t.Error("应该有无效的环")
	}

	// 应该检测到至少1个环，其中至少一个是无效的
	if len(result.Rings) < 1 {
		t.Errorf("应该检测到至少 1 个环，但只有 %d 个", len(result.Rings))
	}
}

func TestDetectRings_ComplexWorkflow(t *testing.T) {
	// 测试复杂工作流（用户提供的原始数据，无环）
	workflowJson := GetComplexWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()

	if result.HasRings {
		t.Errorf("复杂工作流不应该检测到环，但检测到了 %d 个环", len(result.Rings))
	}

	if len(result.Rings) != 0 {
		t.Errorf("复杂工作流应该有 0 个环，但有 %d 个", len(result.Rings))
	}

	if len(result.AllNodes) == 0 {
		t.Error("应该检测到多个节点")
	}

	if !result.ValidateRings() {
		t.Error("复杂工作流应该通过环验证")
	}
}

func TestRingDetectionResult_PrintRingDetectionResult(t *testing.T) {
	// 测试环检测结果打印功能
	workflowJson := GetConditionalRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		t.Fatalf("创建工作流图失败: %v", err)
	}

	result := graph.DetectRings()
	output := result.PrintRingDetectionResult()

	if output == "" {
		t.Error("环检测结果输出不应该为空")
	}

	// 检查输出中是否包含关键信息
	if !contains(output, "环检测结果") {
		t.Error("输出应该包含'环检测结果'")
	}

	if !contains(output, "条件节点") && result.HasRings {
		t.Error("有环的输出应该包含'条件节点'相关信息")
	}
}

func TestFindRingEntryNode(t *testing.T) {
	// 测试找环入口节点的功能
	adjList := map[string][]NodeConnection{
		"节点A": {{NodeName: "节点A", TargetNode: "节点B"}},
		"节点B": {{NodeName: "节点B", TargetNode: "节点C"}},
		"节点C": {{NodeName: "节点C", TargetNode: "节点A"}},
	}

	ringPath := []string{"节点A", "节点B", "节点C"}
	entryNode := findRingEntryNode(ringPath, adjList)

	if entryNode == "" {
		t.Error("应该能找到入口节点")
	}

	// 入口节点应该是环中的一个节点
	found := false
	for _, node := range ringPath {
		if node == entryNode {
			found = true
			break
		}
	}

	if !found {
		t.Error("入口节点应该在环的路径中")
	}
}

func TestCanExitRing(t *testing.T) {
	// 测试检查条件节点是否能脱离环的功能
	nodeNameMap := map[string]nodepkg.WkFLowNode{
		"条件节点": {Name: "条件节点", Type: "n8n-nodes-base.conditional"},
		"环内节点": {Name: "环内节点", Type: "n8n-nodes-base.pythonDocker"},
		"环外节点": {Name: "环外节点", Type: "n8n-nodes-base.pythonDocker"},
	}

	// 测试可以脱离环的情况
	adjListWithExit := map[string][]NodeConnection{
		"条件节点": {
			{NodeName: "条件节点", TargetNode: "环内节点"}, // 环内连接
			{NodeName: "条件节点", TargetNode: "环外节点"}, // 环外连接
		},
	}

	ringPath := []string{"条件节点", "环内节点"}
	canExit := canExitRing("条件节点", ringPath, adjListWithExit, nodeNameMap)

	if !canExit {
		t.Error("条件节点应该能够脱离环")
	}

	// 测试无法脱离环的情况
	adjListNoExit := map[string][]NodeConnection{
		"条件节点": {
			{NodeName: "条件节点", TargetNode: "环内节点"}, // 只有环内连接
		},
	}

	canExit = canExitRing("条件节点", ringPath, adjListNoExit, nodeNameMap)

	if canExit {
		t.Error("条件节点不应该能够脱离环")
	}
}

func TestBuildAdjacencyList(t *testing.T) {
	// 测试构建邻接表的功能
	connections := WkFlowConn{
		"节点A": {
			"main": [][]struct {
				NodeName string `json:"node"`
				Type     string `json:"type"`
				Index    int    `json:"index"`
			}{
				{
					{NodeName: "节点B", Type: "main", Index: 0},
					{NodeName: "节点C", Type: "main", Index: 1},
				},
			},
		},
		"节点B": {
			"branch1": [][]struct {
				NodeName string `json:"node"`
				Type     string `json:"type"`
				Index    int    `json:"index"`
			}{
				{
					{NodeName: "节点D", Type: "main", Index: 0},
				},
			},
		},
	}

	adjList := buildAdjacencyList(connections)

	if len(adjList) != 2 {
		t.Errorf("应该有 2 个源节点，但有 %d 个", len(adjList))
	}

	// 检查节点A的连接
	connectionsA := adjList["节点A"]
	if len(connectionsA) != 2 {
		t.Errorf("节点A应该有 2 个连接，但有 %d 个", len(connectionsA))
	}

	// 检查节点B的连接
	connectionsB := adjList["节点B"]
	if len(connectionsB) != 1 {
		t.Errorf("节点B应该有 1 个连接，但有 %d 个", len(connectionsB))
	}

	if connectionsB[0].TargetNode != "节点D" {
		t.Errorf("节点B应该连接到节点D，但连接到了 %s", connectionsB[0].TargetNode)
	}

	if connectionsB[0].BranchName != "branch1" {
		t.Errorf("节点B的分支名应该是 'branch1'，但是是 '%s'", connectionsB[0].BranchName)
	}
}

// 辅助函数：检查字符串是否包含子字符串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			func() bool {
				for i := 1; i <= len(s)-len(substr); i++ {
					if s[i:i+len(substr)] == substr {
						return true
					}
				}
				return false
			}())))
}

// 基准测试
func BenchmarkDetectRings_ComplexWorkflow(b *testing.B) {
	workflowJson := GetComplexWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		b.Fatalf("创建工作流图失败: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = graph.DetectRings()
	}
}

func BenchmarkDetectRings_MultiRing(b *testing.B) {
	workflowJson := GetMultiRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		b.Fatalf("创建工作流图失败: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = graph.DetectRings()
	}
}

func BenchmarkDetectRings_NestedRing(b *testing.B) {
	workflowJson := GetNestedRingWorkflowData()
	graph, err := NewWkFlowGraph(workflowJson)
	if err != nil {
		b.Fatalf("创建工作流图失败: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = graph.DetectRings()
	}
}
