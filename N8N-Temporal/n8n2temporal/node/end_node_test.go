package node

import (
	"context"
	"testing"
)

// TestEndNode_executeEndNode
// 目的：验证结束节点执行逻辑返回空数据集
// 边界：无输入、无参数
// 断言：ExecNodeFuncResult.Data为nil
func TestEndNode_executeEndNode(t *testing.T) {
	e := &EndNode{BaseActivity: &BaseActivity{}}
	node := &WkFLowNode{ID: "end", Name: "结束", Type: "*.end"}
	in := &ActivityInput{Node: node}
	res, err := e.executeEndNode(context.Background(), in)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Data != nil {
		t.Fatalf("expected nil, got: %v", res.Data)
	}
}

// TestEndNode_ValidateInput
// 目的：验证结束节点的基础输入校验通过
// 边界：仅校验节点类型字段
// 断言：ValidateInput返回nil
func TestEndNode_ValidateInput(t *testing.T) {
	e := &EndNode{BaseActivity: &BaseActivity{}}
	in := &ActivityInput{Node: &WkFLowNode{Type: "*.end"}}
	if err := e.ValidateInput(in); err != nil {
		t.Fatalf("validate err: %v", err)
	}
}
