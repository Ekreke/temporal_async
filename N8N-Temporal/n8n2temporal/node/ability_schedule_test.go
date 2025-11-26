package node

import (
	"testing"
)

// TestAbilitySchedule_ValidateInput
// 目的：验证能力调度节点在参数完整且有效时通过校验，并正确初始化内部转换器
// 边界：pb_file、req_message、rsp_message均提供非空值
// 断言：ValidateInput返回nil，parameters与conv被设置
func TestAbilitySchedule_ValidateInput(t *testing.T) {
	a := &AbilityScheduleActivity{BaseActivity: &BaseActivity{}}
	node := &WkFLowNode{ID: "ability", Name: "调度", Type: "*.abilitySchedule", Parameters: map[string]interface{}{"pb_file": "x.proto", "req_message": "Req", "rsp_message": "Rsp", "task_kind": "test"}}
	in := &ActivityInput{Node: node}
	err := a.ValidateInput(in)
	if err != nil || a.parameters == nil || a.conv == nil {
		t.Fatalf("validate err: %v", err)
	}
}

// TestAbilitySchedule_ValidateInput_Missing
// 目的：验证缺少必填参数时的校验错误分支
// 边界：pb_file、req_message、rsp_message为空字符串
// 断言：ValidateInput返回错误
func TestAbilitySchedule_ValidateInput_Missing(t *testing.T) {
	a := &AbilityScheduleActivity{BaseActivity: &BaseActivity{}}
	node := &WkFLowNode{ID: "ability", Name: "调度", Type: "*.abilitySchedule", Parameters: map[string]interface{}{"pb_file": "", "req_message": "", "rsp_message": ""}}
	in := &ActivityInput{Node: node}
	err := a.ValidateInput(in)
	if err == nil {
		t.Fatalf("expected error")
	}
}
