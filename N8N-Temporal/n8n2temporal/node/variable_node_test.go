package node

import (
	"context"
	"testing"
)

func TestVariableNode_Execute(t *testing.T) {
	// 在每个测试前清空全局变量
	GlobalVariables.ClearVariables()

	tests := []struct {
		name        string
		input       *ActivityInput
		expectError bool
		checkFunc   func(*testing.T, *ActivityOutput)
	}{
		{
			name: "成功执行变量节点 - set操作",
			input: &ActivityInput{
				NodeID:   "var-1",
				NodeName: "Variable Node Set",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"operation": "set",
					"variables": map[string]interface{}{
						"var1": "value1",
						"var2": 42,
						"var3": true,
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

				if output.Data["success"] != true {
					t.Error("期望返回success=true")
				}
				if output.Data["operation"] != "set" {
					t.Errorf("期望operation为set，实际: %v", output.Data["operation"])
				}
				if output.Data["setCount"] != 3 {
					t.Errorf("期望setCount为3，实际: %v", output.Data["setCount"])
				}

				// 验证变量确实被设置了
				if val, exists := GlobalVariables.GetVariable("var1"); !exists || val != "value1" {
					t.Error("变量var1设置失败")
				}
			},
		},
		{
			name: "成功执行变量节点 - get操作获取所有变量",
			input: &ActivityInput{
				NodeID:   "var-2",
				NodeName: "Variable Node Get All",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"operation": "get",
				},
				WorkflowID:  "workflow-2",
				ExecutionID: "exec-2",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["operation"] != "get" {
					t.Errorf("期望operation为get，实际: %v", output.Data["operation"])
				}

				variables, ok := output.Data["variables"].(map[string]interface{})
				if !ok {
					t.Error("期望返回变量map")
					return
				}

				if len(variables) != 3 {
					t.Errorf("期望3个变量，实际: %d", len(variables))
				}
			},
		},
		{
			name: "成功执行变量节点 - get操作获取指定变量",
			input: &ActivityInput{
				NodeID:   "var-3",
				NodeName: "Variable Node Get Specific",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"operation": "get",
					"variables": map[string]interface{}{
						"var1": "",
						"var3": "",
					},
				},
				WorkflowID:  "workflow-3",
				ExecutionID: "exec-3",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["foundCount"] != 2 {
					t.Errorf("期望foundCount为2，实际: %v", output.Data["foundCount"])
				}

				variables, ok := output.Data["variables"].(map[string]interface{})
				if !ok {
					t.Error("期望返回变量map")
					return
				}

				if len(variables) != 2 {
					t.Errorf("期望2个变量，实际: %d", len(variables))
				}
			},
		},
		{
			name: "成功执行变量节点 - delete操作",
			input: &ActivityInput{
				NodeID:   "var-4",
				NodeName: "Variable Node Delete",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"operation": "delete",
					"variables": map[string]interface{}{
						"var1": "",
						"var2": "",
					},
				},
				WorkflowID:  "workflow-4",
				ExecutionID: "exec-4",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["deletedCount"] != 2 {
					t.Errorf("期望deletedCount为2，实际: %v", output.Data["deletedCount"])
				}

				// 验证变量确实被删除了
				if _, exists := GlobalVariables.GetVariable("var1"); exists {
					t.Error("变量var1删除失败")
				}
			},
		},
		{
			name: "成功执行变量节点 - clear操作",
			input: &ActivityInput{
				NodeID:   "var-5",
				NodeName: "Variable Node Clear",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"operation": "clear",
				},
				WorkflowID:  "workflow-5",
				ExecutionID: "exec-5",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["clearedCount"] != 1 {
					t.Errorf("期望clearedCount为1，实际: %v", output.Data["clearedCount"])
				}

				// 验证所有变量都被清空了
				allVars := GlobalVariables.GetAllVariables()
				if len(allVars) != 0 {
					t.Errorf("期望0个变量，实际: %d", len(allVars))
				}
			},
		},
		{
			name: "无效的节点类型",
			input: &ActivityInput{
				NodeID:      "var-6",
				NodeName:    "Invalid Node Type",
				NodeType:    "invalid-type",
				Parameters:  map[string]interface{}{},
				WorkflowID:  "workflow-6",
				ExecutionID: "exec-6",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "无效的operation值",
			input: &ActivityInput{
				NodeID:   "var-7",
				NodeName: "Invalid Operation",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"operation": "invalid",
				},
				WorkflowID:  "workflow-7",
				ExecutionID: "exec-7",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "variables格式无效",
			input: &ActivityInput{
				NodeID:   "var-8",
				NodeName: "Invalid Variables Format",
				NodeType: "n8n-nodes-base.variable",
				Parameters: map[string]interface{}{
					"variables": "invalid-format",
				},
				WorkflowID:  "workflow-8",
				ExecutionID: "exec-8",
			},
			expectError: true,
			checkFunc:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 在每个测试开始前清空变量
			GlobalVariables.ClearVariables()

			// 为get和delete测试准备一些变量
			if tt.name == "成功执行变量节点 - get操作获取所有变量" ||
				tt.name == "成功执行变量节点 - get操作获取指定变量" ||
				tt.name == "成功执行变量节点 - delete操作" {
				GlobalVariables.SetVariable("var1", "value1")
				GlobalVariables.SetVariable("var2", 42)
				GlobalVariables.SetVariable("var3", true)
			}

			if tt.name == "成功执行变量节点 - clear操作" {
				GlobalVariables.SetVariable("var3", true)
			}

			ctx := context.Background()

			// 创建变量节点实例
			varNode := NewVariableNodeActivity()

			// 验证输入
			err := varNode.ValidateInput(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateInput() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if tt.expectError {
				return
			}

			// 执行节点
			output, err := varNode.Execute(ctx, tt.input)
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

func TestVariableStorage(t *testing.T) {
	// 在每个测试前清空存储
	GlobalVariables.ClearVariables()

	t.Run("测试SetVariable和GetVariable", func(t *testing.T) {
		GlobalVariables.SetVariable("test1", "value1")
		GlobalVariables.SetVariable("test2", 42)
		GlobalVariables.SetVariable("test3", true)

		// 测试获取存在的变量
		if val, exists := GlobalVariables.GetVariable("test1"); !exists || val != "value1" {
			t.Error("SetVariable或GetVariable失败")
		}

		// 测试获取不存在的变量
		if _, exists := GlobalVariables.GetVariable("nonexistent"); exists {
			t.Error("不存在的变量不应该存在")
		}
	})

	t.Run("测试GetAllVariables", func(t *testing.T) {
		GlobalVariables.SetVariable("key1", "val1")
		GlobalVariables.SetVariable("key2", "val2")

		allVars := GlobalVariables.GetAllVariables()
		if len(allVars) != 2 {
			t.Errorf("期望2个变量，实际: %d", len(allVars))
		}

		// 测试返回的是副本
		allVars["newKey"] = "newValue"
		if _, exists := GlobalVariables.GetVariable("newKey"); exists {
			t.Error("GetAllVariables应该返回副本，而不是原始map的引用")
		}
	})

	t.Run("测试DeleteVariable", func(t *testing.T) {
		GlobalVariables.SetVariable("delete1", "value1")
		GlobalVariables.SetVariable("delete2", "value2")

		// 测试删除存在的变量
		if !GlobalVariables.DeleteVariable("delete1") {
			t.Error("删除存在的变量应该返回true")
		}

		if _, exists := GlobalVariables.GetVariable("delete1"); exists {
			t.Error("变量应该被删除")
		}

		// 测试删除不存在的变量
		if GlobalVariables.DeleteVariable("nonexistent") {
			t.Error("删除不存在的变量应该返回false")
		}
	})

	t.Run("测试ClearVariables", func(t *testing.T) {
		GlobalVariables.SetVariable("clear1", "value1")
		GlobalVariables.SetVariable("clear2", "value2")

		GlobalVariables.ClearVariables()

		allVars := GlobalVariables.GetAllVariables()
		if len(allVars) != 0 {
			t.Errorf("清空后期望0个变量，实际: %d", len(allVars))
		}
	})

	t.Run("测试SetMultipleVariables", func(t *testing.T) {
		vars := map[string]interface{}{
			"multi1": "val1",
			"multi2": 100,
			"multi3": false,
		}

		GlobalVariables.SetMultipleVariables(vars)

		if val, exists := GlobalVariables.GetVariable("multi1"); !exists || val != "val1" {
			t.Error("SetMultipleVariables失败")
		}

		if val, exists := GlobalVariables.GetVariable("multi3"); !exists || val != false {
			t.Error("SetMultipleVariables失败")
		}

		allVars := GlobalVariables.GetAllVariables()
		if len(allVars) != 3 {
			t.Errorf("期望3个变量，实际: %d", len(allVars))
		}
	})
}

func TestVariableNode_parseParameters(t *testing.T) {
	varNode := NewVariableNodeActivity()
	varNodeInstance := varNode.(*VariableNode)

	tests := []struct {
		name        string
		parameters  map[string]interface{}
		expectError bool
		expected    VariableNodeParameters
	}{
		{
			name:        "空参数使用默认值",
			parameters:  map[string]interface{}{},
			expectError: false,
			expected: VariableNodeParameters{
				Operation:      "set",
				OverwriteMode:  "overwrite",
				VariablesScope: "global",
				Variables:      map[string]interface{}{},
			},
		},
		{
			name: "完整参数设置",
			parameters: map[string]interface{}{
				"operation":      "get",
				"overwriteMode":  "merge",
				"variablesScope": "local",
				"variables": map[string]interface{}{
					"var1": "value1",
					"var2": 42,
				},
			},
			expectError: false,
			expected: VariableNodeParameters{
				Operation:      "get",
				OverwriteMode:  "merge",
				VariablesScope: "local",
				Variables: map[string]interface{}{
					"var1": "value1",
					"var2": 42,
				},
			},
		},
		{
			name: "无效的operation值",
			parameters: map[string]interface{}{
				"operation": "invalid",
			},
			expectError: true,
		},
		{
			name: "无效的overwriteMode值",
			parameters: map[string]interface{}{
				"overwriteMode": "invalid",
			},
			expectError: true,
		},
		{
			name: "无效的variablesScope值",
			parameters: map[string]interface{}{
				"variablesScope": "invalid",
			},
			expectError: true,
		},
		{
			name: "variables格式无效",
			parameters: map[string]interface{}{
				"variables": "invalid",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params VariableNodeParameters
			err := varNodeInstance.parseParameters(tt.parameters, &params)

			if (err != nil) != tt.expectError {
				t.Errorf("parseParameters() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError {
				if params.Operation != tt.expected.Operation {
					t.Errorf("期望operation为%s，实际: %s", tt.expected.Operation, params.Operation)
				}
				if params.OverwriteMode != tt.expected.OverwriteMode {
					t.Errorf("期望overwriteMode为%s，实际: %s", tt.expected.OverwriteMode, params.OverwriteMode)
				}
				if params.VariablesScope != tt.expected.VariablesScope {
					t.Errorf("期望variablesScope为%s，实际: %s", tt.expected.VariablesScope, params.VariablesScope)
				}
				if len(params.Variables) != len(tt.expected.Variables) {
					t.Errorf("期望variables数量为%d，实际: %d", len(tt.expected.Variables), len(params.Variables))
				}
			}
		})
	}
}

func TestVariableNode_ExecuteSetOperation(t *testing.T) {
	varNode := NewVariableNodeActivity()
	varNodeInstance := varNode.(*VariableNode)

	t.Run("测试overwrite模式", func(t *testing.T) {
		// 在每个测试前清空变量
		GlobalVariables.ClearVariables()
		GlobalVariables.SetVariable("existing", "old_value")

		input := &ActivityInput{
			NodeID:   "test",
			NodeName: "test",
			NodeType: "n8n-nodes-base.variable",
			Parameters: map[string]interface{}{
				"operation":     "set",
				"overwriteMode": "overwrite",
				"variables": map[string]interface{}{
					"existing": "new_value",
					"new":      "new_value",
				},
			},
		}

		params := VariableNodeParameters{
			Operation:     "set",
			OverwriteMode: "overwrite",
			Variables: map[string]interface{}{
				"existing": "new_value",
				"new":      "new_value",
			},
		}

		result, err := varNodeInstance.executeSetOperation(input, params)
		if err != nil {
			t.Errorf("executeSetOperation失败: %v", err)
			return
		}

		if result["setCount"] != 2 {
			t.Errorf("期望setCount为2，实际: %v", result["setCount"])
		}

		// 验证变量值
		if val, _ := GlobalVariables.GetVariable("existing"); val != "new_value" {
			t.Error("变量应该被覆盖")
		}
	})

	t.Run("测试skip模式", func(t *testing.T) {
		// 在每个测试前清空变量
		GlobalVariables.ClearVariables()
		GlobalVariables.SetVariable("existing", "old_value")

		input := &ActivityInput{
			NodeID:   "test",
			NodeName: "test",
			NodeType: "n8n-nodes-base.variable",
		}

		params := VariableNodeParameters{
			Operation:     "set",
			OverwriteMode: "skip",
			Variables: map[string]interface{}{
				"existing": "new_value",
				"new":      "new_value",
			},
		}

		result, err := varNodeInstance.executeSetOperation(input, params)
		if err != nil {
			t.Errorf("executeSetOperation失败: %v", err)
			return
		}

		if result["setCount"] != 1 {
			t.Errorf("期望setCount为1，实际: %v", result["setCount"])
		}
		if result["skippedCount"] != 1 {
			t.Errorf("期望skippedCount为1，实际: %v", result["skippedCount"])
		}

		// 验证现有变量没有被覆盖
		if val, _ := GlobalVariables.GetVariable("existing"); val != "old_value" {
			t.Error("现有变量不应该被覆盖")
		}
	})
}

func TestVariableNode_GetNodeInfo(t *testing.T) {
	varNode := NewVariableNodeActivity()
	info := varNode.GetNodeInfo()

	if info.ID != "variable-node" {
		t.Errorf("期望节点ID为 'variable-node'，实际: %s", info.ID)
	}

	if info.Name != "Variable Node" {
		t.Errorf("期望节点名称为 'Variable Node'，实际: %s", info.Name)
	}

	if info.Type != "n8n-nodes-base.variable" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.variable'，实际: %s", info.Type)
	}

	if info.Category != "core" {
		t.Errorf("期望节点分类为 'core'，实际: %s", info.Category)
	}

	if info.Version != "1.0.0" {
		t.Errorf("期望节点版本为 '1.0.0'，实际: %s", info.Version)
	}
}

func TestGetAllStoredVariables(t *testing.T) {
	GlobalVariables.ClearVariables()

	GlobalVariables.SetVariable("test1", "value1")
	GlobalVariables.SetVariable("test2", 42)

	vars := GetAllStoredVariables()
	if len(vars) != 2 {
		t.Errorf("期望2个变量，实际: %d", len(vars))
	}
}

func TestClearAllStoredVariables(t *testing.T) {
	GlobalVariables.SetVariable("test1", "value1")
	GlobalVariables.SetVariable("test2", 42)

	ClearAllStoredVariables()

	vars := GetAllStoredVariables()
	if len(vars) != 0 {
		t.Errorf("清空后期望0个变量，实际: %d", len(vars))
	}
}
