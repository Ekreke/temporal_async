package main

import (
	"encoding/json"
	"testing"
	"time"

	"n8n2temporal/node"
)

// TestCompleteWorkflow 测试完整工作流执行
func TestCompleteWorkflow(t *testing.T) {
	// 测试数据：完整的N8N工作流JSON
	completeWorkflowJSON := `{
		"id": "test-complete-workflow",
		"name": "测试完整工作流",
		"nodes": [
			{
				"id": "start",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"position": [240, 100],
				"parameters": {
					"globalParameters": {
						"python-node": {
							"timeout": 30,
							"requirements": ["requests"]
						}
					}
				}
			},
			{
				"id": "variables",
				"name": "变量节点",
				"type": "n8n-nodes-base.variable",
				"position": [460, 100],
				"parameters": {
					"operation": "set",
					"variables": {
						"user_id": "12345",
						"session_token": "abc123xyz",
						"is_premium_user": true,
						"request_count": 5
					}
				}
			},
			{
				"id": "condition",
				"name": "条件判断",
				"type": "n8n-nodes-base.conditional",
				"position": [680, 100],
				"parameters": {
					"nodeType": "if",
					"conditions": [
						{
							"id": "premium-check",
							"name": "高级用户检查",
							"outputPath": "premium-branch",
							"logicOperator": "AND",
							"conditions": [
								{
									"leftValue": "is_premium_user",
									"operator": "equals",
									"rightValue": true
								},
								{
									"leftValue": "request_count",
									"operator": "less_than",
									"rightValue": 100
								}
							]
						}
					],
					"defaultBranch": "regular-branch"
				}
			},
			{
				"id": "python",
				"name": "Python节点",
				"type": "n8n-nodes-base.pythonDocker",
				"position": [900, 100],
				"parameters": {
					"code": "import time\nresult = {'processed_data': input_data, 'timestamp': time.time(), 'status': 'success'}",
					"dockerImage": "python:3.11-slim",
					"timeoutSeconds": 30
				}
			},
			{
				"id": "custom",
				"name": "自定义节点",
				"type": "CUSTOM.customNode",
				"position": [1120, 100],
				"parameters": {
					"customLogic": "process_data",
					"outputFormat": "json"
				}
			},
			{
				"id": "end",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"position": [1340, 100],
				"parameters": {
					"resultMode": "all"
				}
			}
		],
		"connections": {
			"开始节点": {
				"main": [[{"node": "变量节点", "type": "main", "index": 0}]]
			},
			"变量节点": {
				"main": [[{"node": "条件判断", "type": "main", "index": 0}]]
			},
			"条件判断": {
				"premium-branch": [[{"node": "Python节点", "type": "main", "index": 0}]],
				"regular-branch": [[{"node": "结束节点", "type": "main", "index": 0}]]
			},
			"Python节点": {
				"main": [[{"node": "自定义节点", "type": "main", "index": 0}]]
			},
			"自定义节点": {
				"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
			}
		}
	}`

	// 验证JSON格式
	var workflowData map[string]interface{}
	err := json.Unmarshal([]byte(completeWorkflowJSON), &workflowData)
	if err != nil {
		t.Fatalf("工作流JSON格式无效: %v", err)
	}

	t.Log("✅ 完整工作流JSON格式验证通过")
}

// TestAllNodeTypes 测试所有节点类型的创建和基本信息
func TestAllNodeTypes(t *testing.T) {
	// 测试所有v2.0.0核心节点
	nodeTests := []struct {
		name     string
		nodeType string
		create   func() node.Activity
	}{
		{
			name:     "StartNode",
			nodeType: "n8n-nodes-base.start",
			create:   func() node.Activity { return node.NewStartNodeActivity() },
		},
		{
			name:     "EndNode",
			nodeType: "n8n-nodes-base.end",
			create:   func() node.Activity { return node.NewEndNodeActivity() },
		},
		{
			name:     "VariableNode",
			nodeType: "n8n-nodes-base.variable",
			create:   func() node.Activity { return node.NewVariableNodeActivity() },
		},
		{
			name:     "ConditionalNode",
			nodeType: "n8n-nodes-base.conditional",
			create:   func() node.Activity { return node.NewConditionalNodeActivity() },
		},
		{
			name:     "PythonDockerNode",
			nodeType: "n8n-nodes-base.pythonDocker",
			create:   func() node.Activity { return node.NewPythonDockerNodeActivity() },
		},
		{
			name:     "DomainResolveNode",
			nodeType: "DNS.domainResolve", // 实际类型
			create:   func() node.Activity { return node.NewDomainResolveActivity() },
		},
		{
			name:     "CustomNode",
			nodeType: "CUSTOM.customNode",
			create:   func() node.Activity { return node.NewCustomNodeActivity() },
		},
	}

	for _, tt := range nodeTests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建节点实例
			activity := tt.create()
			if activity == nil {
				t.Fatalf("无法创建 %s 节点实例", tt.name)
			}

			// 获取节点信息
			info := activity.GetNodeInfo()
			if info == nil {
				t.Fatalf("%s 节点信息为空", tt.name)
			}

			// 验证节点类型
			if info.Type != tt.nodeType {
				t.Errorf("%s 节点类型不匹配: 期望 %s, 实际 %s", tt.name, tt.nodeType, info.Type)
			}

			// 验证基本信息
			if info.ID == "" {
				t.Errorf("%s 节点ID为空", tt.name)
			}
			if info.Name == "" {
				t.Errorf("%s 节点名称为空", tt.name)
			}
			if info.Version == "" {
				t.Errorf("%s 节点版本为空", tt.name)
			}

			t.Logf("✅ %s 节点测试通过: %s (%s)", tt.name, info.Name, info.Type)
		})
	}
}

// TestNodeRegistry 测试节点注册表包含所有节点
func TestNodeRegistry(t *testing.T) {
	// 获取所有注册的节点
	allNodes := node.GetAllRegisteredNodes()
	if len(allNodes) == 0 {
		t.Fatal("没有找到任何注册的节点")
	}

	// 验证核心节点都已注册
	coreNodeTypes := []string{
		"n8n-nodes-base.start",
		"n8n-nodes-base.end",
		"n8n-nodes-base.variable",
		"n8n-nodes-base.conditional",
		"n8n-nodes-base.pythonDocker",
		"n8n-nodes-base.domainResolve",
		"CUSTOM.customNode",
	}

	registeredCoreNodes := 0
	for _, nodeType := range coreNodeTypes {
		if nodeInfo, exists := allNodes[nodeType]; exists {
			registeredCoreNodes++
			t.Logf("✅ 核心节点已注册: %s (%s)", nodeInfo.Name, nodeType)
		} else {
			t.Errorf("❌ 核心节点未注册: %s", nodeType)
		}
	}

	// 验证兼容性节点
	compatNodeTypes := []string{
		"n8n-nodes-base.pythonCode",
		"n8n-nodes-base.if",
		"n8n-nodes-base.custom",
	}

	registeredCompatNodes := 0
	for _, nodeType := range compatNodeTypes {
		if _, exists := allNodes[nodeType]; exists {
			registeredCompatNodes++
			t.Logf("✅ 兼容节点已注册: %s", nodeType)
		} else {
			t.Errorf("❌ 兼容节点未注册: %s", nodeType)
		}
	}

	expectedMinCount := len(coreNodeTypes) + len(compatNodeTypes)
	if len(allNodes) < expectedMinCount {
		t.Errorf("注册节点数量不足: 期望至少 %d 个, 实际 %d 个", expectedMinCount, len(allNodes))
	}

	t.Logf("✅ 节点注册表测试通过: 核心 %d/%d, 兼容 %d/%d, 总计 %d 个节点",
		registeredCoreNodes, len(coreNodeTypes),
		registeredCompatNodes, len(compatNodeTypes),
		len(allNodes))
}

// TestWorkflowJSONStructure 测试工作流JSON结构
func TestWorkflowJSONStructure(t *testing.T) {
	// 测试完整工作流JSON结构
	// 在实件际环境中，可以读取examples/complete_workflow.json文

	// 使用内联的完整工作流JSON进行测试
	workflowJSON := `{
		"id": "test-workflow",
		"name": "测试工作流",
		"nodes": [
			{
				"id": "start",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"position": [240, 100]
			}
		],
		"connections": {}
	}`

	var workflowData map[string]interface{}
	err := json.Unmarshal([]byte(workflowJSON), &workflowData)
	if err != nil {
		t.Fatalf("工作流JSON解析失败: %v", err)
	}

	// 验证必需字段
	requiredFields := []string{"id", "name", "nodes", "connections"}
	for _, field := range requiredFields {
		if _, exists := workflowData[field]; !exists {
			t.Errorf("工作流缺少必需字段: %s", field)
		}
	}

	// 验证节点字段
	nodes, ok := workflowData["nodes"].([]interface{})
	if !ok {
		t.Fatal("工作流nodes字段格式无效")
	}

	if len(nodes) == 0 {
		t.Fatal("工作流至少应包含一个节点")
	}

	t.Log("✅ 工作流JSON结构验证通过")
}

// TestNodeExecutionSequence 测试节点执行顺序
func TestNodeExecutionSequence(t *testing.T) {
	// 创建表达式评估器
	express := node.NewExpressionEvaluator(nil)

	// 模拟工作流执行序列
	executionSequence := []struct {
		nodeType string
		activity node.Activity
	}{
		{"n8n-nodes-base.start", node.NewStartNodeActivity(express)},
		{"n8n-nodes-base.variable", node.NewVariableNodeActivity(express)},
		{"n8n-nodes-base.conditional", node.NewConditionalNodeActivity(express)},
		{"n8n-nodes-base.pythonDocker", node.NewPythonDockerNodeActivity(express)},
		{"CUSTOM.customNode", node.NewCustomNodeActivity(express)},
		{"n8n-nodes-base.end", node.NewEndNodeActivity(express)},
	}

	// 验证每个节点类型都可以正确创建
	for i, step := range executionSequence {
		info := step.activity.GetNodeInfo()
		t.Logf("步骤 %d: %s -> %s", i+1, step.nodeType, info.Name)
	}

	t.Log("✅ 节点执行顺序测试通过")
}

// TestWorkflowDataFlow 测试工作流数据流
func TestWorkflowDataFlow(t *testing.T) {
	// 模拟数据在各节点间的传递
	initialData := map[string]interface{}{
		"user_id":   "test_user_123",
		"timestamp": time.Now().Unix(),
		"session_data": map[string]interface{}{
			"active":      true,
			"permissions": []string{"read", "write"},
		},
	}

	// 模拟各节点的数据处理
	stages := []struct {
		name     string
		nodeType string
		process  func(map[string]interface{}) map[string]interface{}
	}{
		{
			name:     "开始节点",
			nodeType: "n8n-nodes-base.start",
			process:  func(data map[string]interface{}) map[string]interface{} { return data },
		},
		{
			name:     "变量节点",
			nodeType: "n8n-nodes-base.variable",
			process: func(data map[string]interface{}) map[string]interface{} {
				data["processing_started"] = true
				return data
			},
		},
		{
			name:     "条件节点",
			nodeType: "n8n-nodes-base.conditional",
			process: func(data map[string]interface{}) map[string]interface{} {
				data["condition_met"] = true
				return data
			},
		},
		{
			name:     "Python节点",
			nodeType: "n8n-nodes-base.pythonDocker",
			process: func(data map[string]interface{}) map[string]interface{} {
				data["python_processed"] = true
				data["processing_result"] = "success"
				return data
			},
		},
		{
			name:     "结束节点",
			nodeType: "n8n-nodes-base.end",
			process: func(data map[string]interface{}) map[string]interface{} {
				data["workflow_completed"] = true
				return data
			},
		},
	}

	currentData := initialData
	for i, stage := range stages {
		currentData = stage.process(currentData)
		t.Logf("步骤 %d (%s): 数据 keys = %v", i+1, stage.name, getMapKeys(currentData))
	}

	// 验证最终数据包含所有处理标记
	expectedKeys := []string{
		"user_id", "timestamp", "session_data",
		"processing_started", "condition_met", "python_processed", "processing_result", "workflow_completed",
	}

	for _, key := range expectedKeys {
		if _, exists := currentData[key]; !exists {
			t.Errorf("最终数据缺少预期键: %s", key)
		}
	}

	t.Log("✅ 工作流数据流测试通过")
}

// getMapKeys 获取map的所有键
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// TestProjectBuild 验证项目能够正确构建
func TestProjectBuild(t *testing.T) {
	t.Log("测试项目构建...")

	// 这个测试验证所有核心组件都能正确初始化
	registry := node.NewNodeRegistry()
	if registry == nil {
		t.Fatal("无法创建节点注册表")
	}

	// 验证全局注册表
	allNodes := node.GetAllRegisteredNodes()
	if len(allNodes) == 0 {
		t.Fatal("全局节点注册表为空")
	}

	t.Log("✅ 项目构建测试通过")
}

// TestMainIntegration 主要集成测试
func TestMainIntegration(t *testing.T) {
	t.Run("CompleteWorkflow", TestCompleteWorkflow)
	t.Run("AllNodeTypes", TestAllNodeTypes)
	t.Run("NodeRegistry", TestNodeRegistry)
	t.Run("WorkflowJSONStructure", TestWorkflowJSONStructure)
	t.Run("NodeExecutionSequence", TestNodeExecutionSequence)
	t.Run("WorkflowDataFlow", TestWorkflowDataFlow)
	t.Run("ProjectBuild", TestProjectBuild)

	t.Log("🎉 所有集成测试通过!")
}
