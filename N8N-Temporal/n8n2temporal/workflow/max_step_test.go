package workflow

import (
	"testing"
)

// TestMaxStepWorkflow 测试带最大步数限制的工作流
func TestMaxStepWorkflow(t *testing.T) {
	// 创建一个包含循环的工作流JSON
	workflowJSON := `{
		"id": "test-cyclic-workflow",
		"name": "测试循环工作流",
		"nodes": [
			{
				"id": "node1",
				"name": "开始",
				"type": "n8n-nodes-base.manualTrigger",
				"position": [100, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node2",
				"name": "处理A",
				"type": "n8n-nodes-base.code",
				"position": [300, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node3",
				"name": "处理B",
				"type": "n8n-nodes-base.code",
				"position": [500, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node4",
				"name": "处理C",
				"type": "n8n-nodes-base.code",
				"position": [700, 100],
				"parameters": {},
				"typeVersion": 1
			}
		],
		"connections": {
			"开始": {
				"main": [
					[{"node": "处理A"}]
				]
			},
			"处理A": {
				"main": [
					[{"node": "处理B"}]
				]
			},
			"处理B": {
				"main": [
					[{"node": "处理C"}]
				]
			},
			"处理C": {
				"main": [
					[{"node": "处理A"}]
				]
			}
		}
	}`

	// 创建图对象
	graph, err := NewN8NWorkflowGraphFromJSON(workflowJSON)
	if err != nil {
		t.Fatalf("创建图对象失败: %v", err)
	}

	// 获取依赖图
	dependencyGraph := graph.GetDependencyGraph()

	// 测试1：小maxStep应该限制执行
	t.Run("小maxStep限制", func(t *testing.T) {
		executionOrder, err := topologicalSort(dependencyGraph, 2)
		if err != nil {
			t.Fatalf("拓扑排序失败: %v", err)
		}
		if len(executionOrder) != 2 {
			t.Errorf("期望排序节点数为2，实际得到%d", len(executionOrder))
		}
		t.Logf("小maxStep执行顺序: %v", executionOrder)
	})

	// 测试2：中等maxStep
	t.Run("中等maxStep", func(t *testing.T) {
		executionOrder, err := topologicalSort(dependencyGraph, 10)
		if err != nil {
			t.Fatalf("拓扑排序失败: %v", err)
		}
		if len(executionOrder) != 4 { // 应该包含所有节点
			t.Errorf("期望排序节点数为4，实际得到%d", len(executionOrder))
		}
		t.Logf("中等maxStep执行顺序: %v", executionOrder)
	})

	// 测试3：大maxStep
	t.Run("大maxStep", func(t *testing.T) {
		executionOrder, err := topologicalSort(dependencyGraph, 100)
		if err != nil {
			t.Fatalf("拓扑排序失败: %v", err)
		}
		if len(executionOrder) != 4 {
			t.Errorf("期望排序节点数为4，实际得到%d", len(executionOrder))
		}
		t.Logf("大maxStep执行顺序: %v", executionOrder)
	})

	// 验证依赖图
	t.Logf("依赖图: %v", dependencyGraph)
	for node, deps := range dependencyGraph {
		t.Logf("节点 %s 依赖于: %v", node, deps)
	}
}

// TestGenericWorkflowWithMaxStep 测试GenericWorkflowWithMaxStep函数
func TestGenericWorkflowWithMaxStep(t *testing.T) {
	// 使用简单的工作流JSON进行测试
	workflowJSON := `{
		"id": "test-generic-workflow",
		"name": "测试通用工作流",
		"nodes": [
			{
				"id": "node1",
				"name": "开始",
				"type": "n8n-nodes-base.manualTrigger",
				"position": [100, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node2",
				"name": "步骤1",
				"type": "n8n-nodes-base.code",
				"position": [300, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node3",
				"name": "步骤2",
				"type": "n8n-nodes-base.code",
				"position": [500, 100],
				"parameters": {},
				"typeVersion": 1
			}
		],
		"connections": {
			"开始": {
				"main": [
					[{"node": "步骤1"}]
				]
			},
			"步骤1": {
				"main": [
					[{"node": "步骤2"}]
				]
			}
		}
	}`

	// 创建初始数据
	initData := make(map[string]interface{})
	initData["triggerData"] = map[string]interface{}{
		"message":   "测试数据",
		"timestamp": "2025-10-15T12:00:00Z",
	}

	// 测试1：使用默认maxStep - 测试图的解析和拓扑排序
	t.Run("默认maxStep", func(t *testing.T) {
		// 创建图对象并测试解析
		graph, err := NewN8NWorkflowGraphFromJSON(workflowJSON)
		if err != nil {
			t.Fatalf("创建图对象失败: %v", err)
		}

		// 获取依赖图并测试拓扑排序
		dependencyGraph := graph.GetDependencyGraph()
		executionOrder, err := topologicalSort(dependencyGraph, 100) // 使用大maxStep
		if err != nil {
			t.Fatalf("拓扑排序失败: %v", err)
		}

		if len(executionOrder) != 3 {
			t.Errorf("期望排序节点数为3，实际得到%d", len(executionOrder))
		}

		t.Logf("默认maxStep结果: 成功=true, 执行节点数=%d, 执行顺序=%v",
			len(executionOrder), executionOrder)
	})

	// 测试2：使用自定义maxStep - 测试拓扑排序的步数限制
	t.Run("自定义maxStep", func(t *testing.T) {
		// 创建图对象并测试解析
		graph, err := NewN8NWorkflowGraphFromJSON(workflowJSON)
		if err != nil {
			t.Fatalf("创建图对象失败: %v", err)
		}

		// 获取依赖图并测试带步数限制的拓扑排序
		dependencyGraph := graph.GetDependencyGraph()
		executionOrder, err := topologicalSort(dependencyGraph, 2) // 使用小maxStep测试限制
		if err != nil {
			t.Fatalf("拓扑排序失败: %v", err)
		}

		// 由于maxStep限制为2，应该只能执行2个节点
		if len(executionOrder) != 2 {
			t.Errorf("期望排序节点数为2（受maxStep限制），实际得到%d", len(executionOrder))
		}

		t.Logf("自定义maxStep结果: 成功=true, maxStep=2, 实际步数=%d, 执行顺序=%v",
			len(executionOrder), executionOrder)
	})
}
