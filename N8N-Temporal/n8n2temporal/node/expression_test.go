package node

import (
	"testing"
	"time"
)

// TestEvaluateExpression_VarGlobal
// 目的：验证$var与$global前缀表达式解析到上下文中对应值
// 边界：嵌套对象读取、布尔类型读取
// 断言：user.id为1、cfg.flag为true
func TestEvaluateExpression_VarGlobal(t *testing.T) {
	eval := NewExpressionEvaluator(NewWorkflowContext())
	eval.GetWorkflowContext().SetNodeData(ExpressVariablesNodeName, map[string]interface{}{"user": map[string]interface{}{"id": 1}})
	eval.GetWorkflowContext().SetNodeData(ExpressGlobalNodeName, map[string]interface{}{"cfg": map[string]interface{}{"flag": true}})
	v1, err := eval.EvaluateExpression("$var.user.id", map[string]interface{}{})
	if err != nil || v1.(int) != 1 {
		t.Fatalf("$var failed: %v %v", v1, err)
	}
	v2, err := eval.EvaluateExpression("$global.cfg.flag", map[string]interface{}{})
	if err != nil || v2.(bool) != true {
		t.Fatalf("$global failed: %v %v", v2, err)
	}
}

// TestEvaluateExpression_NodeReference
// 目的：验证$('节点').path的节点数据引用解析
// 边界：节点包含普通字段与json对象
// 断言：解析到domain为ex.com
func TestEvaluateExpression_NodeReference(t *testing.T) {
	eval := NewExpressionEvaluator(NewWorkflowContext())
	eval.GetWorkflowContext().SetNodeData("域名解析", map[string]interface{}{"domain": "ex.com", "json": map[string]interface{}{"a": 1}})
	v, err := eval.EvaluateExpression("$('域名解析').domain", map[string]interface{}{})
	if err != nil || v.(string) != "ex.com" {
		t.Fatalf("node ref failed: %v %v", v, err)
	}
}

// TestEvaluateExpression_InputDollarAndPath
// 目的：验证$返回整条输入与$json.path返回路径字段
// 边界：输入包含json与数组
// 断言：$包含user字段；$json.user解析为u1
func TestEvaluateExpression_InputDollarAndPath(t *testing.T) {
	eval := NewExpressionEvaluator(NewWorkflowContext())
	input := map[string]interface{}{"json": map[string]interface{}{"user": "u1"}, "arr": []interface{}{map[string]interface{}{"x": 1}}}
	vAll, err := eval.EvaluateExpression("$", input)
	if err != nil {
		t.Fatalf("$ failed: %v", err)
	}
	if vm := vAll.(map[string]interface{}); vm["json"].(map[string]interface{})["user"].(string) != "u1" {
		t.Fatalf("$ value invalid: %v", vm)
	}
	vPath, err := eval.EvaluateExpression("$json.user", input)
	if err != nil || vPath.(string) != "u1" {
		t.Fatalf("$json.user failed: %v %v", vPath, err)
	}
}

// TestExtractFieldValue_ArrayIndexAndErrors
// 目的：验证数组索引读取与越界/类型错误分支
// 边界：arr[0].x正常；arr[1]越界；arr非数组时报错
// 断言：分别命中对应分支
func TestExtractFieldValue_ArrayIndexAndErrors(t *testing.T) {
	eval := NewExpressionEvaluator(NewWorkflowContext())
	input := map[string]interface{}{"arr": []interface{}{map[string]interface{}{"x": 1}}}
	v, err := eval.extractFieldValue("arr[0].x", input)
	if err != nil || v.(int) != 1 {
		t.Fatalf("arr index failed: %v %v", v, err)
	}
	if _, err := eval.extractFieldValue("arr[1]", input); err == nil {
		t.Fatalf("expected out of range error")
	}
	if _, err := eval.extractFieldValue("missing.field", input); err == nil {
		t.Fatalf("expected missing field error")
	}
	bad := map[string]interface{}{"arr": "not array"}
	if _, err := eval.extractFieldValue("arr[0]", bad); err == nil {
		t.Fatalf("expected not array error")
	}
}

// TestWorkflowInfo_TimeAndId
// 目的：验证工作流信息与时间内置表达式的输出格式
// 边界：id/name非空；now字符串长度合理；today格式YYYY-MM-DD；timestamp接近当前时间
// 断言：各项不为空且格式合理
func TestWorkflowInfo_TimeAndId(t *testing.T) {
	eval := NewExpressionEvaluator(NewWorkflowContext())
	if v, _ := eval.EvaluateExpression("$workflow.id", nil); v == "" {
		t.Fatalf("workflow.id empty")
	}
	if v, _ := eval.EvaluateExpression("$workflow.name", nil); v == "" {
		t.Fatalf("workflow.name empty")
	}
	if v, _ := eval.EvaluateExpression("$now", nil); len(v.(string)) < 10 {
		t.Fatalf("now invalid: %v", v)
	}
	if v, _ := eval.EvaluateExpression("$today", nil); len(v.(string)) != len("2006-01-02") {
		t.Fatalf("today invalid: %v", v)
	}
	if v, _ := eval.EvaluateExpression("$timestamp", nil); v.(int64) <= time.Now().Unix()-10000 {
		t.Fatalf("timestamp invalid: %v", v)
	}
}

// TestEvaluateExpression_LiteralsAndFallback
// 目的：验证字面量解析与节点缺失时的fallback行为
// 边界：双引号/单引号字面量；缺失节点表达式原样返回；普通字符串返回自身
// 断言：解析结果与原值一致或按预期fallback
func TestEvaluateExpression_LiteralsAndFallback(t *testing.T) {
	eval := NewExpressionEvaluator(NewWorkflowContext())
	v, err := eval.EvaluateExpression("\"hello\"", nil)
	if err != nil || v.(string) != "hello" {
		t.Fatalf("double literal failed: %v %v", v, err)
	}
	v, err = eval.EvaluateExpression("'world'", nil)
	if err != nil || v.(string) != "world" {
		t.Fatalf("single literal failed: %v %v", v, err)
	}
	v, err = eval.EvaluateExpression("$('missing').field", nil)
	if err != nil || v.(string) != "$('missing').field" {
		t.Fatalf("fallback failed: %v %v", v, err)
	}
	v, err = eval.EvaluateExpression("plain", nil)
	if err != nil || v.(string) != "plain" {
		t.Fatalf("plain failed: %v %v", v, err)
	}
}
