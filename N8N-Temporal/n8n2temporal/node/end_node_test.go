package node

import (
	"context"
	"testing"
)

func TestEndNode_Execute(t *testing.T) {
	tests := []struct {
		name        string
		input       *ActivityInput
		expectError bool
		checkFunc   func(*testing.T, *ActivityOutput)
	}{
		{
			name: "成功执行结束节点 - 默认模式",
			input: &ActivityInput{
				NodeID:      "end-1",
				NodeName:    "End Node",
				NodeType:    "n8n-nodes-base.end",
				Parameters:  map[string]interface{}{},
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
				if output.Data["resultMode"] != "all" {
					t.Errorf("期望默认resultMode为all，实际: %v", output.Data["resultMode"])
				}
				if output.Data["nodeCount"] != 0 {
					t.Errorf("期望nodeCount为0，实际: %v", output.Data["nodeCount"])
				}
			},
		},
		{
			name: "成功执行结束节点 - last模式",
			input: &ActivityInput{
				NodeID:   "end-2",
				NodeName: "End Node Last",
				NodeType: "n8n-nodes-base.end",
				Parameters: map[string]interface{}{
					"resultMode": "last",
				},
				WorkflowID:  "workflow-2",
				ExecutionID: "exec-2",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["resultMode"] != "last" {
					t.Errorf("期望resultMode为last，实际: %v", output.Data["resultMode"])
				}
			},
		},
		{
			name: "成功执行结束节点 - all模式",
			input: &ActivityInput{
				NodeID:   "end-3",
				NodeName: "End Node All",
				NodeType: "n8n-nodes-base.end",
				Parameters: map[string]interface{}{
					"resultMode": "all",
				},
				WorkflowID:  "workflow-3",
				ExecutionID: "exec-3",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["resultMode"] != "all" {
					t.Errorf("期望resultMode为all，实际: %v", output.Data["resultMode"])
				}
			},
		},
		{
			name: "无效的节点类型",
			input: &ActivityInput{
				NodeID:      "end-4",
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
			name: "无效的resultMode值",
			input: &ActivityInput{
				NodeID:   "end-5",
				NodeName: "Invalid Result Mode",
				NodeType: "n8n-nodes-base.end",
				Parameters: map[string]interface{}{
					"resultMode": "invalid",
				},
				WorkflowID:  "workflow-5",
				ExecutionID: "exec-5",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "resultMode不是字符串类型",
			input: &ActivityInput{
				NodeID:   "end-6",
				NodeName: "Invalid Result Mode Type",
				NodeType: "n8n-nodes-base.end",
				Parameters: map[string]interface{}{
					"resultMode": 123,
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

			// 创建结束节点实例
			endNode := NewEndNodeActivity()

			// 验证输入
			err := endNode.ValidateInput(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateInput() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if tt.expectError {
				return
			}

			// 执行节点
			output, err := endNode.Execute(ctx, tt.input)
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
		})
	}
}

func TestEndNode_generateLastNodeResult(t *testing.T) {
	endNode := NewEndNodeActivity()
	endNodeInstance := endNode.(*EndNode)

	tests := []struct {
		name         string
		allNodeData  map[string]interface{}
		expectedType string
		checkFunc    func(*testing.T, interface{})
	}{
		{
			name:         "空节点数据",
			allNodeData:  map[string]interface{}{},
			expectedType: "map[string]interface {}",
			checkFunc: func(t *testing.T, result interface{}) {
				resultMap, ok := result.(map[string]interface{})
				if !ok {
					t.Error("期望返回map[string]interface{}")
					return
				}
				if resultMap["message"] != "工作流中没有节点数据" {
					t.Error("期望返回空数据消息")
				}
			},
		},
		{
			name: "只有系统节点数据",
			allNodeData: map[string]interface{}{
				"__global__": map[string]interface{}{
					"globalParameters": map[string]interface{}{},
				},
				"__variables__": map[string]interface{}{
					"var1": "value1",
				},
			},
			expectedType: "map[string]interface {}",
			checkFunc: func(t *testing.T, result interface{}) {
				resultMap, ok := result.(map[string]interface{})
				if !ok {
					t.Error("期望返回map[string]interface{}")
					return
				}
				if resultMap["message"] != "工作流中没有用户节点数据" {
					t.Error("期望返回无用户节点消息")
				}
			},
		},
		{
			name: "有用户节点数据",
			allNodeData: map[string]interface{}{
				"__global__": map[string]interface{}{
					"globalParameters": map[string]interface{}{},
				},
				"node1": map[string]interface{}{
					"data": "node1-data",
				},
				"node2": map[string]interface{}{
					"data": "node2-data",
				},
				"node3": map[string]interface{}{
					"data": "node3-data",
				},
			},
			expectedType: "map[string]interface {}",
			checkFunc: func(t *testing.T, result interface{}) {
				resultMap, ok := result.(map[string]interface{})
				if !ok {
					t.Error("期望返回map[string]interface{}")
					return
				}
				// 最后的节点应该是node3（按字母排序）
				if resultMap["lastNodeName"] != "node3" {
					t.Errorf("期望最后节点为node3，实际: %v", resultMap["lastNodeName"])
				}
				if resultMap["lastNodeData"] == nil {
					t.Error("期望有最后节点数据")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := endNodeInstance.generateLastNodeResult(tt.allNodeData)

			if tt.checkFunc != nil {
				tt.checkFunc(t, result)
			}
		})
	}
}

func TestEndNode_generateAllNodesResult(t *testing.T) {
	endNode := NewEndNodeActivity()
	endNodeInstance := endNode.(*EndNode)

	tests := []struct {
		name        string
		allNodeData map[string]interface{}
		checkFunc   func(*testing.T, map[string]interface{})
	}{
		{
			name:        "空节点数据",
			allNodeData: map[string]interface{}{},
			checkFunc: func(t *testing.T, result map[string]interface{}) {
				if len(result) != 1 || result["statistics"] == nil {
					t.Error("期望只有统计信息")
				}
			},
		},
		{
			name: "混合节点数据",
			allNodeData: map[string]interface{}{
				"__global__": map[string]interface{}{
					"globalParameters": map[string]interface{}{
						"node1": map[string]interface{}{"param1": "value1"},
					},
				},
				"__variables__": map[string]interface{}{
					"var1": "value1",
					"var2": "value2",
				},
				"node1": map[string]interface{}{
					"data":    "node1-result",
					"success": true,
				},
				"node2": map[string]interface{}{
					"data":    "node2-result",
					"success": false,
				},
			},
			checkFunc: func(t *testing.T, result map[string]interface{}) {
				// 检查用户节点
				if result["node1"] == nil || result["node2"] == nil {
					t.Error("期望包含用户节点数据")
				}

				// 检查系统节点
				systemData, exists := result["system"]
				if !exists {
					t.Error("期望包含系统节点数据")
					return
				}

				systemMap, ok := systemData.(map[string]interface{})
				if !ok {
					t.Error("系统数据应为map[string]interface{}")
					return
				}

				if systemMap["__global__"] == nil || systemMap["__variables__"] == nil {
					t.Error("期望包含全局和变量数据")
				}

				// 检查统计信息
				stats, exists := result["statistics"]
				if !exists {
					t.Error("期望包含统计信息")
					return
				}

				statsMap, ok := stats.(map[string]interface{})
				if !ok {
					t.Error("统计信息应为map[string]interface{}")
					return
				}

				if statsMap["totalNodes"] != 4 {
					t.Errorf("期望总节点数为4，实际: %v", statsMap["totalNodes"])
				}
				if statsMap["userNodes"] != 2 {
					t.Errorf("期望用户节点数为2，实际: %v", statsMap["userNodes"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := endNodeInstance.generateAllNodesResult(tt.allNodeData)

			if tt.checkFunc != nil {
				tt.checkFunc(t, result)
			}
		})
	}
}

func TestEndNode_parseParameters(t *testing.T) {
	endNode := NewEndNodeActivity()
	endNodeInstance := endNode.(*EndNode)

	tests := []struct {
		name        string
		parameters  map[string]interface{}
		expectError bool
		expected    EndNodeParameters
	}{
		{
			name:        "空参数使用默认值",
			parameters:  map[string]interface{}{},
			expectError: false,
			expected: EndNodeParameters{
				ResultMode: "all",
			},
		},
		{
			name: "设置last模式",
			parameters: map[string]interface{}{
				"resultMode": "last",
			},
			expectError: false,
			expected: EndNodeParameters{
				ResultMode: "last",
			},
		},
		{
			name: "设置all模式",
			parameters: map[string]interface{}{
				"resultMode": "all",
			},
			expectError: false,
			expected: EndNodeParameters{
				ResultMode: "all",
			},
		},
		{
			name: "无效的resultMode值",
			parameters: map[string]interface{}{
				"resultMode": "invalid",
			},
			expectError: true,
		},
		{
			name: "resultMode不是字符串",
			parameters: map[string]interface{}{
				"resultMode": 123,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params EndNodeParameters
			err := endNodeInstance.parseParameters(tt.parameters, &params)

			if (err != nil) != tt.expectError {
				t.Errorf("parseParameters() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError && params.ResultMode != tt.expected.ResultMode {
				t.Errorf("期望resultMode为%s，实际: %s", tt.expected.ResultMode, params.ResultMode)
			}
		})
	}
}

func TestEndNode_GetNodeInfo(t *testing.T) {
	endNode := NewEndNodeActivity()
	info := endNode.GetNodeInfo()

	if info.ID != "end-node" {
		t.Errorf("期望节点ID为 'end-node'，实际: %s", info.ID)
	}

	if info.Name != "End Node" {
		t.Errorf("期望节点名称为 'End Node'，实际: %s", info.Name)
	}

	if info.Type != "n8n-nodes-base.end" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.end'，实际: %s", info.Type)
	}

	if info.Category != "core" {
		t.Errorf("期望节点分类为 'core'，实际: %s", info.Category)
	}

	if info.Version != "1.0.0" {
		t.Errorf("期望节点版本为 '1.0.0'，实际: %s", info.Version)
	}
}

func TestEndNode_IsEndNode(t *testing.T) {
	endNode := NewEndNodeActivity()
	endNodeInstance := endNode.(*EndNode)

	if !endNodeInstance.IsEndNode() {
		t.Error("IsEndNode应返回true")
	}
}

func TestEndNode_GetResultMode(t *testing.T) {
	endNode := NewEndNodeActivity()
	endNodeInstance := endNode.(*EndNode)

	resultMode := endNodeInstance.GetResultMode()
	if resultMode != "all" {
		t.Errorf("期望默认resultMode为'all'，实际: %s", resultMode)
	}
}
