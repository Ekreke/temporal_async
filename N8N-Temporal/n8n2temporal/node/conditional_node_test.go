package node

import (
	"testing"
)

// TestConditional_executeIfLogic_Match
// 目的：验证IF逻辑在满足条件时命中首个规则并输出其分支
// 边界：单规则、AND逻辑、表达式左值为节点引用$('域名解析').domain
// 断言：OutputPaths为ok、HasMatch为true
func TestConditional_executeIfLogic_Match(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{expressionEvaluator: newEvaluatorWithContext(map[string]map[string]interface{}{"域名解析": {"domain": "ex.com"}})}}
	params := ConditionalNodeParameters{DefaultBranch: "default"}
	params.Conditions = []ConditionRule{{ID: "r1", Name: "n", OutputPath: "ok", Enabled: true, LogicOperator: "AND", Conditions: []ConditionExpression{{LeftValue: "$('域名解析').domain", Operator: "equals", RightValue: "ex.com", CaseSensitive: true}}}}
	res, err := c.executeIfLogic(params, map[string]interface{}{})
	if err != nil || res.OutputPaths != "ok" || !res.HasMatch {
		t.Fatalf("match failed: %#v %v", res, err)
	}
}

// TestConditional_executeIfLogic_Default
// 目的：验证IF逻辑在不满足任何规则时输出默认分支
// 边界：默认分支为default；规则条件与实际数据不匹配
// 断言：OutputPaths为default、HasMatch为false
func TestConditional_executeIfLogic_Default(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{expressionEvaluator: newEvaluatorWithContext(map[string]map[string]interface{}{"域名解析": {"domain": "ex2.com"}})}}
	params := ConditionalNodeParameters{DefaultBranch: "default"}
	params.Conditions = []ConditionRule{{ID: "r1", Name: "n", OutputPath: "ok", Enabled: true, LogicOperator: "AND", Conditions: []ConditionExpression{{LeftValue: "$('域名解析').domain", Operator: "equals", RightValue: "ex.com", CaseSensitive: true}}}}
	res, err := c.executeIfLogic(params, map[string]interface{}{})
	if err != nil || res.OutputPaths != "default" || res.HasMatch {
		t.Fatalf("default failed: %#v %v", res, err)
	}
}

// TestConditional_CompareOperators_StringAndNumeric
// 目的：覆盖字符串与数值比较操作符的主要分支
// 边界：equals/not_equals/contains/starts_with/ends_with/is_empty/is_not_empty
//
//	以及数值比较 >, >=, <, <=，并包含数值转换错误
//
// 断言：对应操作符的真/假情况与错误分支符合预期
func TestConditional_CompareOperators_StringAndNumeric(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{}}
	// string ops
	if ok, err := c.compareValues("a", "a", "equals", true); !ok || err != nil {
		t.Fatalf("equals fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("a", "b", "equals", true); ok || err != nil {
		t.Fatalf("equals mismatch fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("a", "b", "not_equals", true); !ok || err != nil {
		t.Fatalf("not_equals fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("abc", "b", "contains", true); !ok || err != nil {
		t.Fatalf("contains fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("abc", "a", "starts_with", true); !ok || err != nil {
		t.Fatalf("starts_with fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("abc", "c", "ends_with", true); !ok || err != nil {
		t.Fatalf("ends_with fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("", "", "is_empty", true); !ok || err != nil {
		t.Fatalf("is_empty fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("x", "", "is_not_empty", true); !ok || err != nil {
		t.Fatalf("is_not_empty fail: %v %v", ok, err)
	}
	// numeric ops
	if ok, err := c.compareValues(2, 1, ">", true); !ok || err != nil {
		t.Fatalf("> fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues(2, 2, ">=", true); !ok || err != nil {
		t.Fatalf(">= fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues(1, 2, "<", true); !ok || err != nil {
		t.Fatalf("< fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues(2, 2, "<=", true); !ok || err != nil {
		t.Fatalf("<= fail: %v %v", ok, err)
	}
	// numeric errors
	if _, err := c.compareValues("a", "b", ">", true); err == nil {
		t.Fatalf("expected numeric error")
	}
}

// TestConditional_executeSwitchLogic
// 目的：验证switch逻辑评估所有启用规则并累积输出路径
// 边界：两个规则均命中；默认分支不触发
// 断言：outputPaths包含两个分支
func TestConditional_executeSwitchLogic(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{expressionEvaluator: newEvaluatorWithContext(map[string]map[string]interface{}{"域名解析": {"domain": "ex.com"}})}}
	params := ConditionalNodeParameters{DefaultBranch: "default"}
	params.Conditions = []ConditionRule{
		{ID: "r1", Name: "n1", OutputPath: "ok1", Enabled: true, LogicOperator: "AND", Conditions: []ConditionExpression{{LeftValue: "$('域名解析').domain", Operator: "equals", RightValue: "ex.com"}}},
		{ID: "r2", Name: "n2", OutputPath: "ok2", Enabled: true, LogicOperator: "AND", Conditions: []ConditionExpression{{LeftValue: "$('域名解析').domain", Operator: "equals", RightValue: "ex.com"}}},
	}
	res, err := c.executeSwitchLogic(params, map[string]interface{}{})
	if err != nil {
		t.Fatalf("switch err: %v", err)
	}
	outs := res["outputPaths"].([]string)
	if len(outs) != 2 {
		t.Fatalf("switch outputs len invalid: %v", outs)
	}
}

// TestConditional_ValidateInput_InvalidConditions
// 目的：验证输入参数校验在conditions类型错误时失败
// 边界：conditions为字符串而非数组
// 断言：ValidateInput返回错误
func TestConditional_ValidateInput_InvalidConditions(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{}}
	in := &ActivityInput{Node: &WkFLowNode{Type: "*.conditional", Parameters: map[string]interface{}{"conditions": "bad"}}}
	if err := c.ValidateInput(in); err == nil {
		t.Fatalf("expected conditions invalid error")
	}
}

// TestConditional_execCondition_ParseAndDefault
// 目的：验证参数解析后在规则禁用时走默认分支
// 边界：enabled=false；默认分支为default
// 断言：executeIfLogic输出default
func TestConditional_execCondition_ParseAndDefault(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{expressionEvaluator: newEvaluatorWithContext(map[string]map[string]interface{}{})}}
	node := &WkFLowNode{Type: "*.conditional", Parameters: map[string]interface{}{
		"defaultBranch": "default",
		"conditions":    []interface{}{map[string]interface{}{"id": "r1", "name": "n1", "outputPath": "ok", "enabled": false, "logicOperator": "AND", "conditions": []interface{}{map[string]interface{}{"leftValue": "json.user", "operator": "equals", "rightValue": "u1", "caseSensitive": true}}}},
	}}
	var params ConditionalNodeParameters
	if err := c.parseParameters(node.Parameters, &params); err != nil {
		t.Fatalf("parse err: %v", err)
	}
	out, err := c.executeIfLogic(params, map[string]interface{}{"json": map[string]interface{}{"user": "u1"}})
	if err != nil {
		t.Fatalf("executeIfLogic err: %v", err)
	}
	if out.OutputPaths != "default" {
		t.Fatalf("expected default, got %v", out.OutputPaths)
	}
}

// TestConditional_compareValues_Regex
// 目的：验证正则匹配与非匹配操作符
// 边界：regex与not_regex
// 断言：regex为true、not_regex为false
func TestConditional_compareValues_Regex(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{}}
	if ok, err := c.compareValues("abc123", "^abc\\d+", "regex", true); !ok || err != nil {
		t.Fatalf("regex fail: %v %v", ok, err)
	}
	if ok, err := c.compareValues("abc123", "^abc\\d+", "not_regex", true); ok || err != nil {
		t.Fatalf("not_regex fail: %v %v", ok, err)
	}
}

// TestConditional_compareValues_NotContains
// 目的：验证not_contains的否定包含逻辑
// 边界：右值不在左值中
// 断言：返回true
func TestConditional_compareValues_NotContains(t *testing.T) {
	c := &ConditionalNode{BaseActivity: &BaseActivity{}}
	if ok, err := c.compareValues("abc", "z", "not_contains", true); !ok || err != nil {
		t.Fatalf("not_contains fail: %v %v", ok, err)
	}
}
