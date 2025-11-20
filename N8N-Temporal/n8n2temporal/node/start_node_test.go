package node

import (
	"context"
	"testing"
)

// TestStartNode_executeStartNode_MergePriority
// 目的：验证开始节点合并策略，输入数据覆盖parameters同名键
// 边界：parameters与inputData[0]包含重名键b
// 断言：b取输入值3，保留parameters的a与新增c
func TestStartNode_executeStartNode_MergePriority(t *testing.T) {
	s := &StartNode{BaseActivity: &BaseActivity{}}
	node := &WkFLowNode{ID: "start", Name: "开始", Type: "*.start", Parameters: map[string]interface{}{"a": 1, "b": 2}}
	in := &ActivityInput{Node: node, InputData: []map[string]interface{}{{"b": 3, "c": 4}}}
	res, err := s.executeStartNode(context.Background(), in)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(res.Data) != 1 {
		t.Fatalf("len: %d", len(res.Data))
	}
	m := res.Data[0]
	if m["a"].(int) != 1 || m["b"].(int) != 3 || m["c"].(int) != 4 {
		t.Fatalf("merge failed: %v", m)
	}
}

// TestStartNode_ValidateInput
// 目的：验证开始节点基础输入校验通过
// 边界：仅校验节点类型字段
// 断言：ValidateInput返回nil
func TestStartNode_ValidateInput(t *testing.T) {
	s := &StartNode{BaseActivity: &BaseActivity{}}
	in := &ActivityInput{Node: &WkFLowNode{Type: "*.start"}}
	if err := s.ValidateInput(in); err != nil {
		t.Fatalf("validate err: %v", err)
	}
}
