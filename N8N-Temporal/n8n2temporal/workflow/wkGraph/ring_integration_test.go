package wkGraph

import (
	"fmt"
	"testing"
	"time"
)

// TestRingDetectionIntegration 集成测试：测试完整的环检测流程
func TestRingDetectionIntegration(t *testing.T) {
	// 准备所有测试工作流
	testWorkflows := GetAllTestWorkflows()

	testCases := []struct {
		name              string
		expectedRings     int
		expectedValid     bool
		expectedCondRings int
		description       string
	}{
		{
			name:              "linear",
			expectedRings:     0,
			expectedValid:     true,
			expectedCondRings: 0,
			description:       "线性工作流，无环",
		},
		{
			name:              "simple_ring",
			expectedRings:     1,
			expectedValid:     false,
			expectedCondRings: 0,
			description:       "简单环，无条件节点",
		},
		{
			name:              "conditional_ring",
			expectedRings:     1,
			expectedValid:     true,
			expectedCondRings: 1,
			description:       "条件环，符合条件的环",
		},
		{
			name:              "multi_ring",
			expectedRings:     2,
			expectedValid:     false,
			expectedCondRings: 0,
			description:       "多环工作流，无条件节点",
		},
		{
			name:              "nested_ring",
			expectedRings:     2,
			expectedValid:     true,
			expectedCondRings: 2,
			description:       "嵌套环，内外环都有条件节点",
		},
		{
			name:              "invalid_conditional",
			expectedRings:     1,
			expectedValid:     false,
			expectedCondRings: 0,
			description:       "无效条件环，有条件节点但无法脱离",
		},
		{
			name:              "complex",
			expectedRings:     0,
			expectedValid:     true,
			expectedCondRings: 0,
			description:       "复杂工作流，无环",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("测试用例: %s - %s", tc.name, tc.description)

			workflowJson := testWorkflows[tc.name]
			if workflowJson == "" {
				t.Fatalf("找不到工作流数据: %s", tc.name)
			}

			// 创建工作流图
			graph, err := NewWkFlowGraph(workflowJson)
			if err != nil {
				t.Fatalf("创建工作流图失败: %v", err)
			}

			// 检查工作流图的基本信息
			allNodes := graph.GetAllNodes()
			t.Logf("工作流 '%s' 包含 %d 个节点", tc.name, len(allNodes))

			if len(allNodes) == 0 {
				t.Error("工作流应该包含节点")
			}

			// 执行环检测
			startTime := time.Now()
			result := graph.DetectRings()
			duration := time.Since(startTime)

			t.Logf("环检测耗时: %v", duration)

			// 验证基本结果
			if result.HasRings && len(result.Rings) == 0 {
				t.Error("HasRings为true但Rings为空")
			}

			if !result.HasRings && len(result.Rings) > 0 {
				t.Error("HasRings为false但Rings不为空")
			}

			// 验证环数量
			if len(result.Rings) != tc.expectedRings {
				t.Errorf("环数量不符合预期: 期望 %d, 实际 %d", tc.expectedRings, len(result.Rings))
			}

			// 验证环验证结果
			if result.ValidateRings() != tc.expectedValid {
				t.Errorf("环验证结果不符合预期: 期望 %v, 实际 %v", tc.expectedValid, result.ValidateRings())
			}

			// 验证条件环数量
			if len(result.ConditionalRings) != tc.expectedCondRings {
				t.Errorf("条件环数量不符合预期: 期望 %d, 实际 %d", tc.expectedCondRings, len(result.ConditionalRings))
			}

			// 详细验证环信息
			for i, ring := range result.Rings {
				t.Logf("环 %d: 入口节点='%s', 路径='%v', 包含条件节点=%v, 可脱离环=%v",
					i+1, ring.EntryNode, ring.Path, ring.HasConditional, ring.CanExit)

				// 验证环路径的有效性
				if len(ring.Path) == 0 {
					t.Errorf("环 %d 的路径不能为空", i+1)
				}

				// 验证入口节点在路径中
				entryNodeInPath := false
				for _, node := range ring.Path {
					if node == ring.EntryNode {
						entryNodeInPath = true
						break
					}
				}
				if !entryNodeInPath {
					t.Errorf("环 %d 的入口节点 '%s' 不在路径中", i+1, ring.EntryNode)
				}

				// 验证条件节点的一致性
				if ring.HasConditional {
					if ring.ConditionalNode == "" {
						t.Errorf("环 %d 标记为包含条件节点，但条件节点名称为空", i+1)
					}

					// 验证条件节点在路径中
					conditionalNodeInPath := false
					for _, node := range ring.Path {
						if node == ring.ConditionalNode {
							conditionalNodeInPath = true
							break
						}
					}
					if !conditionalNodeInPath {
						t.Errorf("环 %d 的条件节点 '%s' 不在路径中", i+1, ring.ConditionalNode)
					}
				}
			}

			// 打印详细的环检测结果
			output := result.PrintRingDetectionResult()
			t.Logf("环检测结果输出:\n%s", output)

			// 验证输出包含关键信息
			if result.HasRings && !contains(output, "环详细信息") {
				t.Error("有环时输出应该包含'环详细信息'")
			}

			if len(result.ConditionalRings) > 0 && !contains(output, "符合条件的环") {
				t.Error("有条件环时输出应该包含'符合条件的环'")
			}

			t.Logf("测试用例 '%s' 通过", tc.name)
		})
	}
}

// TestRingDetectionPerformance 性能测试：测试不同规模工作流的环检测性能
func TestRingDetectionPerformance(t *testing.T) {
	testWorkflows := GetAllTestWorkflows()

	performanceTests := []struct {
		name        string
		maxDuration time.Duration
		description string
	}{
		{
			name:        "linear",
			maxDuration: 10 * time.Millisecond,
			description: "线性工作流性能测试",
		},
		{
			name:        "simple_ring",
			maxDuration: 10 * time.Millisecond,
			description: "简单环性能测试",
		},
		{
			name:        "conditional_ring",
			maxDuration: 15 * time.Millisecond,
			description: "条件环性能测试",
		},
		{
			name:        "multi_ring",
			maxDuration: 20 * time.Millisecond,
			description: "多环性能测试",
		},
		{
			name:        "nested_ring",
			maxDuration: 25 * time.Millisecond,
			description: "嵌套环性能测试",
		},
		{
			name:        "complex",
			maxDuration: 30 * time.Millisecond,
			description: "复杂工作流性能测试",
		},
	}

	for _, pt := range performanceTests {
		t.Run(pt.name, func(t *testing.T) {
			t.Logf("性能测试: %s - %s", pt.name, pt.description)

			workflowJson := testWorkflows[pt.name]
			graph, err := NewWkFlowGraph(workflowJson)
			if err != nil {
				t.Fatalf("创建工作流图失败: %v", err)
			}

			// 多次运行取平均值
			iterations := 100
			var totalDuration time.Duration

			for i := 0; i < iterations; i++ {
				start := time.Now()
				result := graph.DetectRings()
				duration := time.Since(start)
				totalDuration += duration

				// 验证结果的一致性
				if i == 0 {
					// 第一次运行时验证结果的正确性
					if result == nil {
						t.Error("环检测结果不能为空")
					}
				}
			}

			avgDuration := totalDuration / time.Duration(iterations)
			t.Logf("平均耗时: %v (运行 %d 次)", avgDuration, iterations)

			if avgDuration > pt.maxDuration {
				t.Errorf("性能不符合预期: 平均耗时 %v 超过最大允许时间 %v", avgDuration, pt.maxDuration)
			}
		})
	}
}

// TestRingDetectionEdgeCases 边界情况测试
func TestRingDetectionEdgeCases(t *testing.T) {
	t.Run("empty_workflow", func(t *testing.T) {
		// 测试空工作流
		emptyWorkflow := `{
			"id": "empty-workflow",
			"name": "空工作流",
			"active": false,
			"nodes": [],
			"connections": {}
		}`

		graph, err := NewWkFlowGraph(emptyWorkflow)
		if err != nil {
			t.Fatalf("创建工作流图失败: %v", err)
		}

		result := graph.DetectRings()

		if result.HasRings {
			t.Error("空工作流不应该检测到环")
		}

		if len(result.Rings) != 0 {
			t.Errorf("空工作流应该有 0 个环，但有 %d 个", len(result.Rings))
		}

		if !result.ValidateRings() {
			t.Error("空工作流应该通过环验证")
		}
	})

	t.Run("single_node_workflow", func(t *testing.T) {
		// 测试单节点工作流
		singleNodeWorkflow := `{
			"id": "single-node-workflow",
			"name": "单节点工作流",
			"active": false,
			"nodes": [
				{
					"id": "start-node",
					"name": "开始节点",
					"type": "n8n-nodes-base.start",
					"type_version": 1,
					"version": 1,
					"position": [240, 100],
					"parameters": {}
				}
			],
			"connections": {}
		}`

		graph, err := NewWkFlowGraph(singleNodeWorkflow)
		if err != nil {
			t.Fatalf("创建工作流图失败: %v", err)
		}

		result := graph.DetectRings()

		if result.HasRings {
			t.Error("单节点工作流不应该检测到环")
		}

		if len(result.Rings) != 0 {
			t.Errorf("单节点工作流应该有 0 个环，但有 %d 个", len(result.Rings))
		}

		if !result.ValidateRings() {
			t.Error("单节点工作流应该通过环验证")
		}
	})

	t.Run("self_loop", func(t *testing.T) {
		// 测试自环（节点连接到自己）
		selfLoopWorkflow := `{
			"id": "self-loop-workflow",
			"name": "自环工作流",
			"active": false,
			"nodes": [
				{
					"id": "loop-node",
					"name": "环节点",
					"type": "n8n-nodes-base.conditional",
					"type_version": 1,
					"version": 1,
					"position": [240, 100],
					"parameters": {
						"conditions": [
							{
								"id": "self-condition",
								"name": "自环条件",
								"outputPath": "self-branch",
								"enabled": true,
								"conditions": [
									{
										"leftValue": "$var.continue_loop",
										"operator": "equals",
										"rightValue": true
									}
								]
							}
						],
						"defaultBranch": "exit-branch"
					}
				},
				{
					"id": "exit-node",
					"name": "出口节点",
					"type": "n8n-nodes-base.end",
					"type_version": 1,
					"version": 1,
					"position": [460, 100],
					"parameters": {}
				}
			],
			"connections": {
				"环节点": {
					"self-branch": [[{"node": "环节点", "type": "main", "index": 0}]],
					"exit-branch": [[{"node": "出口节点", "type": "main", "index": 0}]]
				}
			}
		}`

		graph, err := NewWkFlowGraph(selfLoopWorkflow)
		if err != nil {
			t.Fatalf("创建工作流图失败: %v", err)
		}

		result := graph.DetectRings()

		if !result.HasRings {
			t.Error("自环工作流应该检测到环")
		}

		if len(result.Rings) == 0 {
			t.Error("应该检测到至少一个环")
		}

		// 自环应该能通过验证（有条件节点且能脱离）
		if !result.ValidateRings() {
			t.Error("自环工作流应该通过环验证")
		}

		// 检查环路径的长度
		for _, ring := range result.Rings {
			if len(ring.Path) < 1 {
				t.Error("自环路径至少应该包含一个节点")
			}
		}
	})
}

// TestRingDetectionRealWorldScenarios 真实场景测试
func TestRingDetectionRealWorldScenarios(t *testing.T) {
	t.Run("retry_mechanism", func(t *testing.T) {
		// 测试重试机制场景（常见于实际工作流中）
		retryWorkflow := `{
			"id": "retry-workflow",
			"name": "重试机制工作流",
			"active": false,
			"nodes": [
				{
					"id": "start-node",
					"name": "开始节点",
					"type": "n8n-nodes-base.start",
					"type_version": 1,
					"version": 1,
					"position": [240, 100],
					"parameters": {}
				},
				{
					"id": "api-call",
					"name": "API调用",
					"type": "n8n-nodes-base.httpRequest",
					"type_version": 1,
					"version": 1,
					"position": [460, 100],
					"parameters": {"url": "https://api.example.com"}
				},
				{
					"id": "check-result",
					"name": "检查结果",
					"type": "n8n-nodes-base.conditional",
					"type_version": 1,
					"version": 1,
					"position": [680, 100],
					"parameters": {
						"conditions": [
							{
								"id": "success-check",
								"name": "成功检查",
								"outputPath": "success-branch",
								"enabled": true,
								"conditions": [
									{
										"leftValue": "$json.status",
										"operator": "equals",
										"rightValue": "success"
									}
								]
							},
							{
								"id": "retry-check",
								"name": "重试检查",
								"outputPath": "retry-branch",
								"enabled": true,
								"conditions": [
									{
										"leftValue": "$json.retry_count",
										"operator": "less_than",
										"rightValue": 3
									}
								]
							}
						],
						"defaultBranch": "fail-branch"
					}
				},
				{
					"id": "increment-retry",
					"name": "增加重试次数",
					"type": "n8n-nodes-base.variable",
					"type_version": 1,
					"version": 1,
					"position": [900, 50],
					"parameters": {
						"operation": "set",
						"variables": {"retry_count": "{{$json.retry_count + 1}}"}
					}
				},
				{
					"id": "success-handler",
					"name": "成功处理",
					"type": "n8n-nodes-base.pythonDocker",
					"type_version": 1,
					"version": 1,
					"position": [900, 100],
					"parameters": {"code": "print('success')"}
				},
				{
					"id": "fail-handler",
					"name": "失败处理",
					"type": "n8n-nodes-base.pythonDocker",
					"type_version": 1,
					"version": 1,
					"position": [900, 150],
					"parameters": {"code": "print('failed')"}
				},
				{
					"id": "end-node",
					"name": "结束节点",
					"type": "n8n-nodes-base.end",
					"type_version": 1,
					"version": 1,
					"position": [1120, 100],
					"parameters": {}
				}
			],
			"connections": {
				"开始节点": {
					"main": [[{"node": "API调用", "type": "main", "index": 0}]]
				},
				"API调用": {
					"main": [[{"node": "检查结果", "type": "main", "index": 0}]]
				},
				"检查结果": {
					"success-branch": [[{"node": "成功处理", "type": "main", "index": 0}]],
					"retry-branch": [[{"node": "增加重试次数", "type": "main", "index": 0}]],
					"fail-branch": [[{"node": "失败处理", "type": "main", "index": 0}]]
				},
				"增加重试次数": {
					"main": [[{"node": "API调用", "type": "main", "index": 0}]]
				},
				"成功处理": {
					"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
				},
				"失败处理": {
					"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
				}
			}
		}`

		graph, err := NewWkFlowGraph(retryWorkflow)
		if err != nil {
			t.Fatalf("创建工作流图失败: %v", err)
		}

		result := graph.DetectRings()

		if !result.HasRings {
			t.Error("重试工作流应该检测到环")
		}

		if len(result.Rings) == 0 {
			t.Error("应该检测到至少一个环")
		}

		// 重试环应该能通过验证（有条件节点且能脱离）
		if !result.ValidateRings() {
			t.Error("重试工作流应该通过环验证")
		}

		// 验证条件节点
		foundRetryConditional := false
		for _, ring := range result.Rings {
			if ring.ConditionalNode == "检查结果" && ring.HasConditional && ring.CanExit {
				foundRetryConditional = true
				break
			}
		}

		if !foundRetryConditional {
			t.Error("应该找到包含条件节点且能脱离环的重试环")
		}
	})
}

// TestRingDetectionConsistency 一致性测试：确保多次运行结果一致
func TestRingDetectionConsistency(t *testing.T) {
	testWorkflows := GetAllTestWorkflows()

	for workflowName, workflowJson := range testWorkflows {
		t.Run(fmt.Sprintf("consistency_%s", workflowName), func(t *testing.T) {
			graph, err := NewWkFlowGraph(workflowJson)
			if err != nil {
				t.Fatalf("创建工作流图失败: %v", err)
			}

			// 多次运行环检测
			iterations := 10
			var firstResult *RingDetectionResult
			allResultsConsistent := true

			for i := 0; i < iterations; i++ {
				result := graph.DetectRings()

				if i == 0 {
					firstResult = result
				} else {
					// 比较结果是否一致
					if !compareRingDetectionResults(firstResult, result) {
						allResultsConsistent = false
						t.Errorf("第 %d 次运行结果与第一次不一致", i+1)
					}
				}
			}

			if !allResultsConsistent {
				t.Error("多次运行结果不一致")
			} else {
				t.Logf("工作流 '%s' 的 %d 次运行结果一致", workflowName, iterations)
			}
		})
	}
}

// compareRingDetectionResults 比较两个环检测结果是否一致
func compareRingDetectionResults(r1, r2 *RingDetectionResult) bool {
	if r1.HasRings != r2.HasRings {
		return false
	}

	if len(r1.Rings) != len(r2.Rings) {
		return false
	}

	if len(r1.ConditionalRings) != len(r2.ConditionalRings) {
		return false
	}

	if r1.ValidateRings() != r2.ValidateRings() {
		return false
	}

	// 简单比较环的数量，详细的环路径比较比较复杂，这里先不实现
	return true
}
