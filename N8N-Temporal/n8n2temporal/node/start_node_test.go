package node

import (
	"context"
	"testing"
)

func TestStartNode_Execute(t *testing.T) {
	tests := []struct {
		name        string
		input       *ActivityInput
		expectError bool
		checkFunc   func(*testing.T, *ActivityOutput)
	}{
		{
			name: "成功执行开始节点 - 带全局参数",
			input: &ActivityInput{
				NodeID:   "start-1",
				NodeName: "Start Node",
				NodeType: "n8n-nodes-base.start",
				Parameters: map[string]interface{}{
					"globalParameters": map[string]interface{}{
						"node1": map[string]interface{}{
							"param1": "value1",
							"param2": 42,
						},
						"python-node": map[string]interface{}{
							"code":    "print('hello')",
							"timeout": 30,
						},
					},
				},
				WorkflowID:  "workflow-1",
				ExecutionID: "exec-1",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				// 检查返回数据
				if output.Data["success"] != true {
					t.Error("期望返回success=true")
				}
				if output.Data["configuredNodes"] != 2 {
					t.Errorf("期望配置了2个节点，实际: %v", output.Data["configuredNodes"])
				}

				// 检查节点参数
				nodeParams, ok := output.Data["nodeParameters"].(map[string]map[string]interface{})
				if !ok {
					t.Error("node参数格式错误")
				}
				if len(nodeParams) != 2 {
					t.Errorf("期望2个节点参数，实际: %d", len(nodeParams))
				}
			},
		},
		{
			name: "成功执行开始节点 - 无参数",
			input: &ActivityInput{
				NodeID:      "start-2",
				NodeName:    "Start Node Empty",
				NodeType:    "n8n-nodes-base.start",
				Parameters:  map[string]interface{}{},
				WorkflowID:  "workflow-2",
				ExecutionID: "exec-2",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}
				if output.Data["configuredNodes"] != 0 {
					t.Errorf("期望配置了0个节点，实际: %v", output.Data["configuredNodes"])
				}
			},
		},
		{
			name: "参数为空",
			input: &ActivityInput{
				NodeID:      "start-3",
				NodeName:    "Start Node No Params",
				NodeType:    "n8n-nodes-base.start",
				Parameters:  nil,
				WorkflowID:  "workflow-3",
				ExecutionID: "exec-3",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "无效的节点类型",
			input: &ActivityInput{
				NodeID:      "start-4",
				NodeName:    "Invalid Node Type",
				NodeType:    "invalid-type",
				Parameters:  map[string]interface{}{},
				WorkflowID:  "workflow-4",
				ExecutionID: "exec-4",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "无效的全局参数格式",
			input: &ActivityInput{
				NodeID:   "start-5",
				NodeName: "Invalid Global Params",
				NodeType: "n8n-nodes-base.start",
				Parameters: map[string]interface{}{
					"globalParameters": "invalid-format",
				},
				WorkflowID:  "workflow-5",
				ExecutionID: "exec-5",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "节点参数格式无效",
			input: &ActivityInput{
				NodeID:   "start-6",
				NodeName: "Invalid Node Params",
				NodeType: "n8n-nodes-base.start",
				Parameters: map[string]interface{}{
					"globalParameters": map[string]interface{}{
						"node1": "invalid-node-params",
					},
				},
				WorkflowID:  "workflow-6",
				ExecutionID: "exec-6",
			},
			expectError: true,
			checkFunc:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// 创建开始节点实例
			startNode := NewStartNodeActivity()
			startNodeInstance := startNode.(*StartNode)

			// 初始化工作流上下文
			workflowContext := NewWorkflowContext()
			startNodeInstance.InitExpressionEvaluator(workflowContext)

			// 验证输入
			err := startNode.ValidateInput(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateInput() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if tt.expectError {
				return
			}

			// 执行节点
			output, err := startNode.Execute(ctx, tt.input)
			if err != nil {
				t.Errorf("Execute() 返回错误: %v", err)
				return
			}

			if !output.Success {
				t.Errorf("节点执行失败: %s", output.Error)
				return
			}

			// 检查输出
			if tt.checkFunc != nil {
				tt.checkFunc(t, output)
			}

			// 验证工作流上下文中存储了全局参数
			if tt.input.Parameters != nil && tt.input.Parameters["globalParameters"] != nil {
				globalData, exists := workflowContext.GetNodeData("__global__")
				if !exists {
					t.Error("工作流上下文中未找到全局参数")
					return
				}

				globalContext := globalData
				if globalContext == nil {
					t.Error("全局参数格式错误")
					return
				}

				if _, hasGlobalParams := globalContext["globalParameters"]; !hasGlobalParams {
					t.Error("缺少globalParameters字段")
				}
			}
		})
	}
}

func TestStartNode_GetNodeParametersForNode(t *testing.T) {
	// 创建开始节点实例
	startNode := NewStartNodeActivity()
	startNodeInstance := startNode.(*StartNode)

	// 初始化工作流上下文
	workflowContext := NewWorkflowContext()
	startNodeInstance.InitExpressionEvaluator(workflowContext)

	// 模拟存储全局参数
	globalContextData := map[string]interface{}{
		"globalParameters": map[string]map[string]interface{}{
			"test-node": map[string]interface{}{
				"param1": "value1",
				"param2": 42,
			},
			"another-node": map[string]interface{}{
				"paramA": "valueA",
			},
		},
		"nodeParameters": make(map[string]interface{}),
	}
	workflowContext.SetNodeData("__global__", globalContextData)

	tests := []struct {
		name           string
		nodeName       string
		expectedParams map[string]interface{}
		expectFound    bool
	}{
		{
			name:     "获取存在的节点参数",
			nodeName: "test-node",
			expectedParams: map[string]interface{}{
				"param1": "value1",
				"param2": 42,
			},
			expectFound: true,
		},
		{
			name:     "获取另一个存在的节点参数",
			nodeName: "another-node",
			expectedParams: map[string]interface{}{
				"paramA": "valueA",
			},
			expectFound: true,
		},
		{
			name:           "获取不存在的节点参数",
			nodeName:       "non-existent-node",
			expectedParams: nil,
			expectFound:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := startNodeInstance.GetNodeParametersForNode(tt.nodeName)

			if tt.expectFound {
				if params == nil {
					t.Error("期望找到参数但返回nil")
					return
				}

				// 检查参数内容
				if len(params) != len(tt.expectedParams) {
					t.Errorf("参数数量不匹配，期望: %d, 实际: %d", len(tt.expectedParams), len(params))
					return
				}

				for key, expectedValue := range tt.expectedParams {
					if actualValue, exists := params[key]; !exists || actualValue != expectedValue {
						t.Errorf("参数 %s 不匹配，期望: %v, 实际: %v", key, expectedValue, actualValue)
					}
				}
			} else {
				if params != nil {
					t.Error("期望不找到参数但返回了参数")
				}
			}
		})
	}
}

func TestStartNode_GetAllGlobalParameters(t *testing.T) {
	// 创建开始节点实例
	startNode := NewStartNodeActivity()
	startNodeInstance := startNode.(*StartNode)

	// 初始化工作流上下文
	workflowContext := NewWorkflowContext()
	startNodeInstance.InitExpressionEvaluator(workflowContext)

	// 测试无全局参数情况
	params := startNodeInstance.GetAllGlobalParameters()
	if params != nil {
		t.Error("无全局参数时应返回nil")
	}

	// 模拟存储全局参数
	globalContextData := map[string]interface{}{
		"globalParameters": map[string]map[string]interface{}{
			"node1": map[string]interface{}{
				"param1": "value1",
			},
			"node2": map[string]interface{}{
				"paramA": "valueA",
			},
		},
		"nodeParameters": make(map[string]interface{}),
	}
	workflowContext.SetNodeData("__global__", globalContextData)

	params = startNodeInstance.GetAllGlobalParameters()
	if params == nil {
		t.Error("有全局参数时应返回参数")
		return
	}

	if len(params) != 2 {
		t.Errorf("期望2个节点的参数，实际: %d", len(params))
	}

	// 检查具体参数
	if node1Params, exists := params["node1"]; !exists || node1Params["param1"] != "value1" {
		t.Error("node1参数不正确")
	}

	if node2Params, exists := params["node2"]; !exists || node2Params["paramA"] != "valueA" {
		t.Error("node2参数不正确")
	}
}

func TestStartNode_GetNodeInfo(t *testing.T) {
	startNode := NewStartNodeActivity()
	info := startNode.GetNodeInfo()

	if info.ID != "start-node" {
		t.Errorf("期望节点ID为 'start-node'，实际: %s", info.ID)
	}

	if info.Name != "Start Node" {
		t.Errorf("期望节点名称为 'Start Node'，实际: %s", info.Name)
	}

	if info.Type != "n8n-nodes-base.start" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.start'，实际: %s", info.Type)
	}

	if info.Category != "core" {
		t.Errorf("期望节点分类为 'core'，实际: %s", info.Category)
	}

	if info.Version != "1.0.0" {
		t.Errorf("期望节点版本为 '1.0.0'，实际: %s", info.Version)
	}
}

func TestStartNode_ParseParameters(t *testing.T) {
	startNode := NewStartNodeActivity()
	startNodeInstance := startNode.(*StartNode)

	tests := []struct {
		name        string
		parameters  map[string]interface{}
		expectError bool
		expected    StartNodeParameters
	}{
		{
			name: "正确解析globalParameters格式",
			parameters: map[string]interface{}{
				"globalParameters": map[string]interface{}{
					"node1": map[string]interface{}{
						"param1": "value1",
					},
					"node2": map[string]interface{}{
						"param2": 42,
					},
				},
			},
			expectError: false,
			expected: StartNodeParameters{
				GlobalParameters: map[string]map[string]interface{}{
					"node1": map[string]interface{}{
						"param1": "value1",
					},
					"node2": map[string]interface{}{
						"param2": 42,
					},
				},
			},
		},
		{
			name: "直接解析节点参数格式",
			parameters: map[string]interface{}{
				"node1": map[string]interface{}{
					"param1": "value1",
				},
				"node2": map[string]interface{}{
					"param2": 42,
				},
				"other-field": "ignore-me",
			},
			expectError: false,
			expected: StartNodeParameters{
				GlobalParameters: map[string]map[string]interface{}{
					"node1": map[string]interface{}{
						"param1": "value1",
					},
					"node2": map[string]interface{}{
						"param2": 42,
					},
				},
			},
		},
		{
			name: "无效的globalParameters格式",
			parameters: map[string]interface{}{
				"globalParameters": "invalid",
			},
			expectError: true,
		},
		{
			name: "节点参数格式无效",
			parameters: map[string]interface{}{
				"globalParameters": map[string]interface{}{
					"node1": "invalid-format",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params StartNodeParameters
			err := startNodeInstance.parseParameters(tt.parameters, &params)

			if (err != nil) != tt.expectError {
				t.Errorf("parseParameters() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError {
				// 比较解析结果
				if len(params.GlobalParameters) != len(tt.expected.GlobalParameters) {
					t.Errorf("全局参数数量不匹配，期望: %d, 实际: %d",
						len(tt.expected.GlobalParameters), len(params.GlobalParameters))
					return
				}

				for nodeName, expectedNodeParams := range tt.expected.GlobalParameters {
					actualNodeParams, exists := params.GlobalParameters[nodeName]
					if !exists {
						t.Errorf("缺少节点 %s 的参数", nodeName)
						continue
					}

					// 简单比较参数长度
					if len(actualNodeParams) != len(expectedNodeParams) {
						t.Errorf("节点 %s 参数数量不匹配", nodeName)
					}
				}
			}
		})
	}
}
