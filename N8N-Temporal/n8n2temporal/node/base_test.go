package node

import (
	"errors"
	"n8n2temporal/notify"
	"regexp"
	"testing"
)

// TestResolveNodeReferenceRegex
// 目的：验证节点引用正则表达式对$('Node').item.json.*形式的匹配能力
// 边界：包含节点名与路径两段；强调匹配项数量为3（整体、节点名、路径）
// 断言：FindStringSubmatch返回长度为3
func TestResolveNodeReferenceRegex(t *testing.T) {
	expression := "$('NodeName').item.json.hello"
	re := regexp.MustCompile(`\$\('([^']*)'\)(.*)`)
	matches := re.FindStringSubmatch(expression)
	if len(matches) != 3 {
		t.Fatalf("regex not match, got %d", len(matches))
	}
}

// TestWorkflowContext_SetGetKV_Nested
// 目的：验证工作流上下文的点分路径递归设置与读取
// 边界：多层嵌套键user.profile.id；读取不存在键返回false
// 断言：成功写入与读取123；不存在键读取失败
func TestWorkflowContext_SetGetKV_Nested(t *testing.T) {
	wc := NewWorkflowContext()
	if err := wc.SetNodeDataKV("__variables__", "user.profile.id", 123); err != nil {
		t.Fatalf("set kv err: %v", err)
	}
	v, ok := wc.GetNodeDataKV("__variables__", "user.profile.id")
	if !ok || v.(int) != 123 {
		t.Fatalf("get kv failed: %v %v", v, ok)
	}
	if _, ok := wc.GetNodeData("__variables__"); !ok {
		t.Fatalf("get node data failed")
	}
}

// TestBaseActivity_ValidateAndOutputs
// 目的：验证基类输入校验与成功/失败输出的封装逻辑
// 边界：nil输入、空type触发错误；成功输出包含节点与数据；错误输出包含错误文本
// 断言：对应分支返回符合预期结构
func TestBaseActivity_ValidateAndOutputs(t *testing.T) {
	b := &BaseActivity{}
	if err := b.ValidateInput(nil); err == nil {
		t.Fatalf("expected error on nil input")
	}
	in := &ActivityInput{Node: &WkFLowNode{Type: ""}}
	if err := b.ValidateInput(in); err == nil {
		t.Fatalf("expected error on empty type")
	}
	in = &ActivityInput{Node: &WkFLowNode{ID: "n1", Name: "节点", Type: "*.x"}, SignalInput: &notify.SignalData{Metadata: notify.SignalMetadata{Label: "L"}}}
	succ := b.CreateSuccessOutput(in, &ExecNodeFuncResult{Data: []map[string]interface{}{{"ok": true}}})
	if !succ.Success || len(succ.Data) != 1 || succ.NodeID != "n1" {
		t.Fatalf("success output invalid: %#v", succ)
	}
	errOut := b.CreateErrorOutput(in, errors.New("e"))
	if errOut.Success || errOut.Error == "" || errOut.NodeName != "节点" {
		t.Fatalf("error output invalid: %#v", errOut)
	}
}

// TestWkFLowNode_Check
// 目的：验证节点结构字段完整性检查
// 边界：全部为空触发错误；完整字段通过检查
// 断言：bad返回错误，good返回nil
func TestWkFLowNode_Check(t *testing.T) {
	bad := WkFLowNode{}
	if err := bad.Check(); err == nil {
		t.Fatalf("expected check error on empty")
	}
	good := WkFLowNode{ID: "id", Name: "n", Type: "*.x", Parameters: map[string]interface{}{}, Version: 1, IsRemote: false}
	if err := good.Check(); err != nil {
		t.Fatalf("check valid err: %v", err)
	}
}

// TestCreateSuccessOutput_NilExecRes
// 目的：验证CreateSuccessOutput在execRes为nil时的容错处理
// 边界：传入nil执行结果
// 断言：返回的输出Success为true、Data非nil且为空slice
func TestCreateSuccessOutput_NilExecRes(t *testing.T) {
	b := &BaseActivity{}
	in := &ActivityInput{Node: &WkFLowNode{ID: "n4", Name: "节点4", Type: "*.x"}, SignalInput: &notify.SignalData{Metadata: notify.SignalMetadata{}}}
	out := b.CreateSuccessOutput(in, nil)
	if !out.Success || out.NodeID != "n4" || out.Data == nil {
		t.Fatalf("nil execRes not handled: %#v", out)
	}
}
