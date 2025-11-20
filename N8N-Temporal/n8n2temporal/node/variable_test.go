package node

import (
	"context"
	"reflect"
	"testing"
)

func newEvaluatorWithContext(ctxData map[string]map[string]interface{}) *ExpressionEvaluator {
	wc := NewWorkflowContext()
	for k, v := range ctxData {
		wc.SetNodeData(k, v)
	}
	return NewExpressionEvaluator(wc)
}

// TestNormalizeValue
// 目的：验证变量节点对值的递归解析（字符串、map、数组）
// 边界：常量字符串；$var/$global；节点引用$('域名解析').item.json.domain；嵌套map动态键与数组元素
// 断言：每类解析结果符合预期
func TestNormalizeValue(t *testing.T) {
	eval := newEvaluatorWithContext(map[string]map[string]interface{}{
		ExpressVariablesNodeName: {"user_id": "123", "k_user": "user.profile.id"},
		ExpressGlobalNodeName:    {"params": map[string]interface{}{"retry": 3}},
		"域名解析":                   {"item": map[string]interface{}{"json": map[string]interface{}{"domain": "ex.com"}}},
	})
	vn := &VariableNode{BaseActivity: &BaseActivity{expressionEvaluator: eval}}
	input := map[string]interface{}{"json": map[string]interface{}{"a": 1}}

	got1, err := vn.normalizeValue("abc", input)
	if err != nil || got1.(string) != "abc" {
		t.Fatalf("const string failed: %v %v", got1, err)
	}

	got2, err := vn.normalizeValue("$var.user_id", input)
	if err != nil || got2.(string) != "123" {
		t.Fatalf("$var failed: %v %v", got2, err)
	}

	got3, err := vn.normalizeValue("$global.params.retry", input)
	if err != nil || got3.(int) != 3 {
		t.Fatalf("$global failed: %v %v", got3, err)
	}

	got4, err := vn.normalizeValue("$('域名解析').item.json.domain", input)
	if err != nil || got4.(string) != "ex.com" {
		t.Fatalf("node ref failed: %v %v", got4, err)
	}

	m := map[string]interface{}{
		"$var.k_user": "$('域名解析').item.json.domain",
	}
	got5, err := vn.normalizeValue(m, input)
	if err != nil {
		t.Fatalf("map normalize error: %v", err)
	}
	gm := got5.(map[string]interface{})
	if gm["user.profile.id"].(string) != "ex.com" {
		t.Fatalf("map normalize failed: %v", gm)
	}

	arr := []interface{}{"$var.user_id", "$global.params.retry"}
	got6, err := vn.normalizeValue(arr, input)
	if err != nil {
		t.Fatalf("array normalize error: %v", err)
	}
	ga := got6.([]interface{})
	if ga[0].(string) != "123" || ga[1].(int) != 3 {
		t.Fatalf("array normalize failed: %v", ga)
	}
}

// TestExecuteVariableNode_SetOverwrite
// 目的：验证set+overwrite模式按解析后的键与值写入变量上下文
// 边界：动态键来自$var.k_user；值来自节点引用
// 断言：写入后的变量键为解析后的字符串且值正确
func TestExecuteVariableNode_SetOverwrite(t *testing.T) {
	eval := newEvaluatorWithContext(map[string]map[string]interface{}{
		ExpressVariablesNodeName: {"k_user": "user.profile.id"},
		"域名解析":                   {"item": map[string]interface{}{"json": map[string]interface{}{"domain": "ex.com"}}},
	})
	vn := &VariableNode{BaseActivity: &BaseActivity{expressionEvaluator: eval}}
	node := &WkFLowNode{ID: "v1", Name: "变量", Type: "n8n-nodes-base.variable", Parameters: map[string]interface{}{
		"operation":     "set",
		"overwriteMode": "overwrite",
		"variables": map[string]interface{}{
			"$var.k_user": "$('域名解析').item.json.domain",
		},
	}}
	in := &ActivityInput{Node: node, Express: eval}
	res, err := vn.executeVariableNode(context.Background(), in, map[string]interface{}{"json": map[string]interface{}{"x": 1}})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	got, ok := eval.GetWorkflowContext().GetNodeDataKV(ExpressVariablesNodeName, "user.profile.id")
	if !ok || got.(string) != "ex.com" {
		t.Fatalf("set overwrite failed: %v %v", res, got)
	}
}

// TestExecuteVariableNode_Skip
// 目的：验证skip模式在键已存在时保持旧值
// 边界：初始变量存在a=old；新值为a=new
// 断言：最终变量仍为old
func TestExecuteVariableNode_Skip(t *testing.T) {
	eval := newEvaluatorWithContext(map[string]map[string]interface{}{
		ExpressVariablesNodeName: {"a": "old"},
	})
	vn := &VariableNode{BaseActivity: &BaseActivity{expressionEvaluator: eval}}
	node := &WkFLowNode{ID: "v2", Name: "变量", Type: "n8n-nodes-base.variable", Parameters: map[string]interface{}{
		"operation":     "set",
		"overwriteMode": "skip",
		"variables":     map[string]interface{}{"a": "new"},
	}}
	in := &ActivityInput{Node: node, Express: eval}
	res, err := vn.executeVariableNode(context.Background(), in, map[string]interface{}{})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	if res["a"].(string) != "old" {
		t.Fatalf("skip failed: %v", res)
	}
}

// TestExecuteVariableNode_Merge
// 目的：验证merge模式对map值进行浅合并
// 边界：旧值obj包含a；新值obj包含b
// 断言：合并后包含a与b
func TestExecuteVariableNode_Merge(t *testing.T) {
	eval := newEvaluatorWithContext(map[string]map[string]interface{}{
		ExpressVariablesNodeName: {"obj": map[string]interface{}{"a": 1}},
	})
	vn := &VariableNode{BaseActivity: &BaseActivity{expressionEvaluator: eval}}
	node := &WkFLowNode{ID: "v3", Name: "变量", Type: "n8n-nodes-base.variable", Parameters: map[string]interface{}{
		"operation":     "set",
		"overwriteMode": "merge",
		"variables":     map[string]interface{}{"obj": map[string]interface{}{"b": 2}},
	}}
	in := &ActivityInput{Node: node, Express: eval}
	res, err := vn.executeVariableNode(context.Background(), in, map[string]interface{}{})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	want := map[string]interface{}{"a": 1, "b": 2}
	if !reflect.DeepEqual(res["obj"], want) {
		t.Fatalf("merge failed: %v", res["obj"])
	}
}

// TestVariable_InvalidOperation
// 目的：验证不支持的operation分支返回错误
// 边界：operation=get
// 断言：executeVariableNode返回非nil错误
func TestVariable_InvalidOperation(t *testing.T) {
	eval := newEvaluatorWithContext(map[string]map[string]interface{}{})
	vn := &VariableNode{BaseActivity: &BaseActivity{expressionEvaluator: eval}}
	node := &WkFLowNode{ID: "vX", Name: "变量", Type: "*.variable", Parameters: map[string]interface{}{
		"operation":     "get",
		"overwriteMode": "overwrite",
		"variables":     map[string]interface{}{"a": "b"},
	}}
	in := &ActivityInput{Node: node, Express: eval}
	_, err := vn.executeVariableNode(context.Background(), in, map[string]interface{}{})
	if err == nil {
		t.Fatalf("expected invalid operation error")
	}
}

// TestVariable_Integration
// 目的：集成验证：连续执行两次写入，覆盖$global与节点引用解析
// 边界：profile.domain与cfg.flag按解析写入
// 断言：最终上下文中的两个键值正确
func TestVariable_Integration(t *testing.T) {
	eval := newEvaluatorWithContext(map[string]map[string]interface{}{
		ExpressVariablesNodeName: {},
		ExpressGlobalNodeName:    {"cfg": map[string]interface{}{"flag": true}},
		"域名解析":                   {"item": map[string]interface{}{"json": map[string]interface{}{"domain": "ex.com"}}},
	})
	vn := &VariableNode{BaseActivity: &BaseActivity{expressionEvaluator: eval}}
	node := &WkFLowNode{ID: "v4", Name: "变量", Type: "n8n-nodes-base.variable", Parameters: map[string]interface{}{
		"operation":     "set",
		"overwriteMode": "overwrite",
		"variables": map[string]interface{}{
			"profile.domain": "$('域名解析').item.json.domain",
			"cfg.flag":       "$global.cfg.flag",
		},
	}}
	in := &ActivityInput{Node: node, Express: eval}
	_, err := vn.executeVariableNode(context.Background(), in, map[string]interface{}{"json": map[string]interface{}{"a": 1}})
	if err != nil {
		t.Fatalf("execute1 error: %v", err)
	}
	_, err = vn.executeVariableNode(context.Background(), in, map[string]interface{}{"json": map[string]interface{}{"b": 2}})
	if err != nil {
		t.Fatalf("execute2 error: %v", err)
	}
	v1, ok1 := eval.GetWorkflowContext().GetNodeDataKV(ExpressVariablesNodeName, "profile.domain")
	v2, ok2 := eval.GetWorkflowContext().GetNodeDataKV(ExpressVariablesNodeName, "cfg.flag")
	if !ok1 || !ok2 || v1.(string) != "ex.com" || v2.(bool) != true {
		t.Fatalf("integration set failed: %v %v", v1, v2)
	}
}
