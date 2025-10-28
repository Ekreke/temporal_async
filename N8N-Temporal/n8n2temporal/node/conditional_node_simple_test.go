package node

import (
	"context"
	"testing"
)

func TestConditionalNode_Basic(t *testing.T) {
	express := NewExpressionEvaluator(nil)
	condNode := NewConditionalNodeActivity(express)

	// 测试节点信息
	info := condNode.GetNodeInfo()
	if info.Type != "n8n-nodes-base.conditional" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.conditional'，实际: %s", info.Type)
	}

	// 测试基本输入验证
	validInput := &ActivityInput{
		NodeID:      "test-1",
		NodeName:    "Test Conditional Node",
		NodeType:    "n8n-nodes-base.conditional",
		Parameters:  map[string]interface{}{},
		WorkflowID:  "workflow-1",
		ExecutionID: "exec-1",
	}

	err := condNode.ValidateInput(validInput)
	if err != nil {
		t.Errorf("有效输入验证失败: %v", err)
	}

	// 测试无效节点类型
	invalidInput := &ActivityInput{
		NodeID:      "test-2",
		NodeName:    "Invalid Node Type",
		NodeType:    "invalid-type",
		Parameters:  map[string]interface{}{},
		WorkflowID:  "workflow-2",
		ExecutionID: "exec-2",
	}

	err = condNode.ValidateInput(invalidInput)
	if err == nil {
		t.Error("无效节点类型应该验证失败")
	}

	// 测试执行基本条件
	ctx := context.Background()
	// condNode 已经有了 express，不需要初始化

	simpleInput := &ActivityInput{
		NodeID:   "test-3",
		NodeName: "Simple Test",
		NodeType: "n8n-nodes-base.conditional",
		InputData: map[string]interface{}{
			"status": "active",
		},
		Parameters: map[string]interface{}{
			"nodeType": "if",
			"conditions": []interface{}{
				map[string]interface{}{
					"id":         "rule1",
					"name":       "检查状态",
					"outputPath": "active-branch",
					"enabled":    true,
					"conditions": []interface{}{
						map[string]interface{}{
							"leftValue":  "status",
							"operator":   "equals",
							"rightValue": "active",
						},
					},
				},
			},
		},
		WorkflowID:  "workflow-3",
		ExecutionID: "exec-3",
	}

	output, err := condNode.Execute(ctx, simpleInput)
	if err != nil {
		t.Errorf("执行失败: %v", err)
		return
	}

	if !output.Success {
		t.Errorf("节点执行失败: %s", output.Error)
		return
	}

	// 检查基本输出字段
	if output.Data["success"] != true {
		t.Error("期望success=true")
	}

	if output.Data["nodeType"] != "if" {
		t.Errorf("期望nodeType为if，实际: %v", output.Data["nodeType"])
	}

	if output.Data["hasMatch"] != true {
		t.Error("期望hasMatch=true")
	}

	if output.Data["conditionsCount"] != 1 {
		t.Errorf("期望conditionsCount为1，实际: %v", output.Data["conditionsCount"])
	}

	t.Log("基本条件节点测试通过")
}
