package node

import (
	"context"
	"testing"
)

func TestConditionalNode_Execute(t *testing.T) {
	tests := []struct {
		name        string
		input       *ActivityInput
		expectError bool
		checkFunc   func(*testing.T, *ActivityOutput)
	}{
		{
			name: "成功执行条件节点 - if类型匹配条件",
			input: &ActivityInput{
				NodeID:   "cond-1",
				NodeName: "Conditional Node If Match",
				NodeType: "n8n-nodes-base.conditional",
				InputData: map[string]interface{}{
					"status": "active",
					"age":    25,
				},
				Parameters: map[string]interface{}{
					"nodeType": "if",
					"conditions": []interface{}{
						map[string]interface{}{
							"id":            "rule1",
							"name":          "检查状态",
							"outputPath":    "branch1",
							"enabled":       true,
							"logicOperator": "AND",
							"conditions": []interface{}{
								map[string]interface{}{
									"leftValue":     "status",
									"operator":      "equals",
									"rightValue":    "active",
									"caseSensitive": true,
								},
							},
						},
					},
					"defaultBranch": "default",
				},
				WorkflowID:  "workflow-1",
				ExecutionID: "exec-1",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["nodeType"] != "if" {
					t.Errorf("期望nodeType为if，实际: %v", output.Data["nodeType"])
				}

				if output.Data["hasMatch"] != true {
					t.Error("期望有匹配的条件")
				}

				matchedConditions := output.Data["matchedConditions"]
				if matchedConditions == nil {
					t.Error("期望有匹配的条件")
				}

				outputPaths := output.Data["outputPaths"]
				if outputPaths == nil {
					t.Error("期望有输出路径")
				}
			},
		},
		{
			name: "成功执行条件节点 - if类型无匹配使用默认",
			input: &ActivityInput{
				NodeID:   "cond-2",
				NodeName: "Conditional Node If Default",
				NodeType: "n8n-nodes-base.conditional",
				InputData: map[string]interface{}{
					"status": "inactive",
				},
				Parameters: map[string]interface{}{
					"nodeType": "if",
					"conditions": []interface{}{
						map[string]interface{}{
							"id":         "rule1",
							"name":       "检查状态",
							"outputPath": "branch1",
							"enabled":    true,
							"conditions": []interface{}{
								map[string]interface{}{
									"leftValue":     "status",
									"operator":      "equals",
									"rightValue":    "active",
									"caseSensitive": true,
								},
							},
						},
					},
					"defaultBranch": "default",
				},
				WorkflowID:  "workflow-2",
				ExecutionID: "exec-2",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["hasMatch"] != false {
					t.Error("期望没有匹配的条件")
				}

				outputPaths, ok := output.Data["outputPaths"].([]interface{})
				if !ok || len(outputPaths) != 1 || outputPaths[0] != "default" {
					t.Error("期望输出路径为default")
				}
			},
		},
		{
			name: "成功执行条件节点 - switch类型多条件匹配",
			input: &ActivityInput{
				NodeID:   "cond-3",
				NodeName: "Conditional Node Switch",
				NodeType: "n8n-nodes-base.conditional",
				InputData: map[string]interface{}{
					"category": "premium",
					"score":    85,
				},
				Parameters: map[string]interface{}{
					"nodeType": "switch",
					"conditions": []interface{}{
						map[string]interface{}{
							"id":         "rule1",
							"name":       "高级用户",
							"outputPath": "premium",
							"enabled":    true,
							"conditions": []interface{}{
								map[string]interface{}{
									"leftValue":  "category",
									"operator":   "equals",
									"rightValue": "premium",
								},
							},
						},
						map[string]interface{}{
							"id":         "rule2",
							"name":       "高分用户",
							"outputPath": "high-score",
							"enabled":    true,
							"conditions": []interface{}{
								map[string]interface{}{
									"leftValue":  "score",
									"operator":   "greater_than",
									"rightValue": 80,
								},
							},
						},
					},
					"defaultBranch": "else",
				},
				WorkflowID:  "workflow-3",
				ExecutionID: "exec-3",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["nodeType"] != "switch" {
					t.Errorf("期望nodeType为switch，实际: %v", output.Data["nodeType"])
				}

				if output.Data["hasMatch"] != true {
					t.Error("期望有匹配的条件")
				}

				matchedConditions, ok := output.Data["matchedConditions"].([]interface{})
				if !ok || len(matchedConditions) != 2 {
					t.Errorf("期望2个匹配的条件，实际: %d", len(matchedConditions))
				}

				outputPaths, ok := output.Data["outputPaths"].([]interface{})
				if !ok || len(outputPaths) != 2 {
					t.Errorf("期望2个输出路径，实际: %d", len(outputPaths))
				}
			},
		},
		{
			name: "成功执行条件节点 - 复杂条件AND逻辑",
			input: &ActivityInput{
				NodeID:   "cond-4",
				NodeName: "Conditional Node Complex AND",
				NodeType: "n8n-nodes-base.conditional",
				InputData: map[string]interface{}{
					"status":   "active",
					"age":      25,
					"category": "premium",
				},
				Parameters: map[string]interface{}{
					"nodeType": "if",
					"conditions": []interface{}{
						map[string]interface{}{
							"id":            "rule1",
							"name":          "复杂条件",
							"outputPath":    "complex",
							"enabled":       true,
							"logicOperator": "AND",
							"conditions": []interface{}{
								map[string]interface{}{
									"leftValue":  "status",
									"operator":   "equals",
									"rightValue": "active",
								},
								map[string]interface{}{
									"leftValue":  "age",
									"operator":   "greater_than",
									"rightValue": 18,
								},
								map[string]interface{}{
									"leftValue":  "category",
									"operator":   "equals",
									"rightValue": "premium",
								},
							},
						},
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

				if output.Data["hasMatch"] != true {
					t.Error("期望有匹配的条件")
				}

				outputPaths, ok := output.Data["outputPaths"].([]interface{})
				if !ok || len(outputPaths) != 1 || outputPaths[0] != "complex" {
					t.Error("期望输出路径为complex")
				}
			},
		},
		{
			name: "成功执行条件节点 - 复杂条件OR逻辑",
			input: &ActivityInput{
				NodeID:   "cond-5",
				NodeName: "Conditional Node Complex OR",
				NodeType: "n8n-nodes-base.conditional",
				InputData: map[string]interface{}{
					"status":   "inactive",
					"priority": "high",
				},
				Parameters: map[string]interface{}{
					"nodeType": "if",
					"conditions": []interface{}{
						map[string]interface{}{
							"id":            "rule1",
							"name":          "OR条件",
							"outputPath":    "or-branch",
							"enabled":       true,
							"logicOperator": "OR",
							"conditions": []interface{}{
								map[string]interface{}{
									"leftValue":  "status",
									"operator":   "equals",
									"rightValue": "active",
								},
								map[string]interface{}{
									"leftValue":  "priority",
									"operator":   "equals",
									"rightValue": "high",
								},
							},
						},
					},
				},
				WorkflowID:  "workflow-5",
				ExecutionID: "exec-5",
			},
			expectError: false,
			checkFunc: func(t *testing.T, output *ActivityOutput) {
				if !output.Success {
					t.Errorf("期望执行成功，但返回失败: %s", output.Error)
				}

				if output.Data["hasMatch"] != true {
					t.Error("期望有匹配的条件")
				}

				outputPaths, ok := output.Data["outputPaths"].([]interface{})
				if !ok || len(outputPaths) != 1 || outputPaths[0] != "or-branch" {
					t.Error("期望输出路径为or-branch")
				}
			},
		},
		{
			name: "无效的节点类型",
			input: &ActivityInput{
				NodeID:      "cond-6",
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
			name: "无效的nodeType参数",
			input: &ActivityInput{
				NodeID:   "cond-7",
				NodeName: "Invalid Node Type Param",
				NodeType: "n8n-nodes-base.conditional",
				Parameters: map[string]interface{}{
					"nodeType": "invalid",
				},
				WorkflowID:  "workflow-7",
				ExecutionID: "exec-7",
			},
			expectError: true,
			checkFunc:   nil,
		},
		{
			name: "conditions格式无效",
			input: &ActivityInput{
				NodeID:   "cond-8",
				NodeName: "Invalid Conditions Format",
				NodeType: "n8n-nodes-base.conditional",
				Parameters: map[string]interface{}{
					"conditions": "invalid",
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
			ctx := context.Background()

			// 创建条件节点实例
			condNode := NewConditionalNodeActivity()
			condNodeInstance := condNode.(*ConditionalNode)

			// 初始化表达式评估器
			workflowContext := NewWorkflowContext()
			condNodeInstance.InitExpressionEvaluator(workflowContext)

			// 验证输入
			err := condNode.ValidateInput(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateInput() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if tt.expectError {
				return
			}

			// 执行节点
			output, err := condNode.Execute(ctx, tt.input)
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

func TestConditionalNode_parseParameters(t *testing.T) {
	condNode := NewConditionalNodeActivity()
	condNodeInstance := condNode.(*ConditionalNode)

	tests := []struct {
		name        string
		parameters  map[string]interface{}
		expectError bool
		expected    ConditionalNodeParameters
	}{
		{
			name:        "空参数使用默认值",
			parameters:  map[string]interface{}{},
			expectError: false,
			expected: ConditionalNodeParameters{
				NodeType:      "if",
				Conditions:    []ConditionRule{},
				DefaultBranch: "default",
				EvaluateMode:  "first",
				InputData:     map[string]interface{}{},
			},
		},
		{
			name: "完整的if参数",
			parameters: map[string]interface{}{
				"nodeType":      "if",
				"defaultBranch": "else-branch",
				"evaluateMode":  "first",
				"inputData": map[string]interface{}{
					"extra": "data",
				},
				"conditions": []interface{}{
					map[string]interface{}{
						"id":            "rule1",
						"name":          "测试规则",
						"outputPath":    "output1",
						"enabled":       true,
						"logicOperator": "AND",
						"conditions": []interface{}{
							map[string]interface{}{
								"leftValue":     "field1",
								"operator":      "equals",
								"rightValue":    "value1",
								"caseSensitive": false,
							},
						},
					},
				},
			},
			expectError: false,
			expected: ConditionalNodeParameters{
				NodeType:      "if",
				DefaultBranch: "else-branch",
				EvaluateMode:  "first",
				InputData: map[string]interface{}{
					"extra": "data",
				},
				Conditions: []ConditionRule{
					{
						ID:            "rule1",
						Name:          "测试规则",
						OutputPath:    "output1",
						Enabled:       true,
						LogicOperator: "AND",
						Conditions: []ConditionExpression{
							{
								LeftValue:     "field1",
								Operator:      "equals",
								RightValue:    "value1",
								CaseSensitive: false,
							},
						},
					},
				},
			},
		},
		{
			name: "无效的nodeType值",
			parameters: map[string]interface{}{
				"nodeType": "invalid",
			},
			expectError: true,
		},
		{
			name: "无效的evaluateMode值",
			parameters: map[string]interface{}{
				"evaluateMode": "invalid",
			},
			expectError: true,
		},
		{
			name: "conditions格式无效",
			parameters: map[string]interface{}{
				"conditions": "invalid",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params ConditionalNodeParameters
			err := condNodeInstance.parseParameters(tt.parameters, &params)

			if (err != nil) != tt.expectError {
				t.Errorf("parseParameters() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError {
				if params.NodeType != tt.expected.NodeType {
					t.Errorf("期望nodeType为%s，实际: %s", tt.expected.NodeType, params.NodeType)
				}
				if params.DefaultBranch != tt.expected.DefaultBranch {
					t.Errorf("期望defaultBranch为%s，实际: %s", tt.expected.DefaultBranch, params.DefaultBranch)
				}
				if params.EvaluateMode != tt.expected.EvaluateMode {
					t.Errorf("期望evaluateMode为%s，实际: %s", tt.expected.EvaluateMode, params.EvaluateMode)
				}
				if len(params.Conditions) != len(tt.expected.Conditions) {
					t.Errorf("期望conditions数量为%d，实际: %d", len(tt.expected.Conditions), len(params.Conditions))
				}
			}
		})
	}
}

func TestConditionalNode_compareValues(t *testing.T) {
	condNode := NewConditionalNodeActivity()
	condNodeInstance := condNode.(*ConditionalNode)

	tests := []struct {
		name           string
		left           interface{}
		right          interface{}
		operator       string
		caseSensitive  bool
		expectedResult bool
		expectError    bool
	}{
		{
			name:           "字符串相等",
			left:           "hello",
			right:          "hello",
			operator:       "equals",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "字符串不等",
			left:           "hello",
			right:          "world",
			operator:       "equals",
			caseSensitive:  true,
			expectedResult: false,
			expectError:    false,
		},
		{
			name:           "字符串包含",
			left:           "hello world",
			right:          "world",
			operator:       "contains",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "大小写不敏感相等",
			left:           "Hello",
			right:          "hello",
			operator:       "equals",
			caseSensitive:  false,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "数字大于",
			left:           25,
			right:          20,
			operator:       "greater_than",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "数字小于等于",
			left:           15,
			right:          20,
			operator:       "less_than_or_equal",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "空字符串检查",
			left:           "",
			right:          "",
			operator:       "is_empty",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "非空字符串检查",
			left:           "hello",
			right:          "",
			operator:       "is_not_empty",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "正则表达式匹配",
			left:           "test123",
			right:          "^test\\d+$",
			operator:       "regex",
			caseSensitive:  true,
			expectedResult: true,
			expectError:    false,
		},
		{
			name:           "无效操作符",
			left:           "hello",
			right:          "world",
			operator:       "invalid_op",
			caseSensitive:  true,
			expectedResult: false,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := condNodeInstance.compareValues(tt.left, tt.right, tt.operator, tt.caseSensitive)

			if (err != nil) != tt.expectError {
				t.Errorf("compareValues() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError && result != tt.expectedResult {
				t.Errorf("期望结果为%v，实际: %v", tt.expectedResult, result)
			}
		})
	}
}

func TestConditionalNode_GetNodeInfo(t *testing.T) {
	condNode := NewConditionalNodeActivity()
	info := condNode.GetNodeInfo()

	if info.ID != "conditional-node" {
		t.Errorf("期望节点ID为 'conditional-node'，实际: %s", info.ID)
	}

	if info.Name != "Conditional Node" {
		t.Errorf("期望节点名称为 'Conditional Node'，实际: %s", info.Name)
	}

	if info.Type != "n8n-nodes-base.conditional" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.conditional'，实际: %s", info.Type)
	}

	if info.Category != "logic" {
		t.Errorf("期望节点分类为 'logic'，实际: %s", info.Category)
	}

	if info.Version != "1.0.0" {
		t.Errorf("期望节点版本为 '1.0.0'，实际: %s", info.Version)
	}
}

func TestConditionalNode_evaluateConditionRule(t *testing.T) {
	condNode := NewConditionalNodeActivity()
	condNodeInstance := condNode.(*ConditionalNode)

	data := map[string]interface{}{
		"status":   "active",
		"score":    85,
		"category": "premium",
	}

	t.Run("测试AND逻辑全部匹配", func(t *testing.T) {
		rule := ConditionRule{
			ID:            "rule1",
			LogicOperator: "AND",
			Conditions: []ConditionExpression{
				{
					LeftValue:  "status",
					Operator:   "equals",
					RightValue: "active",
				},
				{
					LeftValue:  "score",
					Operator:   "greater_than",
					RightValue: 80,
				},
			},
		}

		result, err := condNodeInstance.evaluateConditionRule(rule, data)
		if err != nil {
			t.Errorf("evaluateConditionRule失败: %v", err)
			return
		}

		if !result {
			t.Error("期望条件规则匹配")
		}
	})

	t.Run("测试AND逻辑部分不匹配", func(t *testing.T) {
		rule := ConditionRule{
			ID:            "rule2",
			LogicOperator: "AND",
			Conditions: []ConditionExpression{
				{
					LeftValue:  "status",
					Operator:   "equals",
					RightValue: "inactive",
				},
				{
					LeftValue:  "score",
					Operator:   "greater_than",
					RightValue: 80,
				},
			},
		}

		result, err := condNodeInstance.evaluateConditionRule(rule, data)
		if err != nil {
			t.Errorf("evaluateConditionRule失败: %v", err)
			return
		}

		if result {
			t.Error("期望条件规则不匹配")
		}
	})

	t.Run("测试OR逻辑部分匹配", func(t *testing.T) {
		rule := ConditionRule{
			ID:            "rule3",
			LogicOperator: "OR",
			Conditions: []ConditionExpression{
				{
					LeftValue:  "status",
					Operator:   "equals",
					RightValue: "inactive",
				},
				{
					LeftValue:  "category",
					Operator:   "equals",
					RightValue: "premium",
				},
			},
		}

		result, err := condNodeInstance.evaluateConditionRule(rule, data)
		if err != nil {
			t.Errorf("evaluateConditionRule失败: %v", err)
			return
		}

		if !result {
			t.Error("期望条件规则匹配（OR逻辑）")
		}
	})
}
