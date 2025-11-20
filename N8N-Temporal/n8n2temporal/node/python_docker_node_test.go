package node

import (
	"testing"
)

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	return len(s) >= len(sub) && (s == sub || len(s) > len(sub) && (s[:len(sub)] == sub || s[len(s)-len(sub):] == sub || func() bool {
		for i := 1; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()))
}

// TestCodeNode_parseParameters_Valid
// 目的：验证完整参数解析路径与默认值填充
// 边界：timeoutSeconds为float64、maxRetries为int、env/requirements/inputData类型正确
// 断言：解析后的结构字段值符合预期
func TestCodeNode_parseParameters_Valid(t *testing.T) {
	p := &CodeNode{BaseActivity: &BaseActivity{}}
	params := make(map[string]interface{})
	params["code"] = "ret = {'ok': True}"
	params["dockerImage"] = "python:3.11-slim"
	params["timeoutSeconds"] = float64(30)
	params["maxRetries"] = int(2)
	params["environmentVars"] = map[string]interface{}{"ENV": "prod"}
	params["requirements"] = []interface{}{"requests"}
	params["inputData"] = map[string]interface{}{"x": 1}
	var dst CodeNodeParameters
	err := p.parseParameters(params, &dst)
	if err != nil {
		t.Fatalf("parse err: %v", err)
	}
	if dst.Code == "" || dst.TimeoutSeconds != 30 || dst.MaxRetries != 2 || dst.EnvironmentVars["ENV"] != "prod" {
		t.Fatalf("parsed invalid: %#v", dst)
	}
}

// TestCodeNode_generatePythonScript
// 目的：验证Python脚本生成包含模板与用户逻辑注入位置
// 边界：注入_input_data/_var_data、存在user_logic定义
// 断言：生成脚本非空且包含关键片段
func TestCodeNode_generatePythonScript(t *testing.T) {
	p := &CodeNode{BaseActivity: &BaseActivity{expressionEvaluator: NewExpressionEvaluator(NewWorkflowContext())}}
	c := CodeNodeParameters{Code: "return {'a': 1}", DockerImage: "python:3.11-slim", TimeoutSeconds: 10}
	script, err := p.generatePythonScript(c, map[string]interface{}{"json": map[string]interface{}{"x": 1}})
	if err != nil || len(script) == 0 {
		t.Fatalf("gen err: %v", err)
	}
	if !contains(script, "user_logic") || !contains(script, "_input_data") {
		t.Fatalf("script invalid: %s", script)
	}
}

// TestCodeNode_indentUserCode
// 目的：验证用户代码缩进格式化，保持空行与移除原有前导空格
// 边界：包含空行与非空行
// 断言：输出包含预期缩进
func TestCodeNode_indentUserCode(t *testing.T) {
	p := &CodeNode{}
	out := p.indentUserCode("a = 1\n\nprint(a)", "    ")
	if !contains(out, "    a = 1") || !contains(out, "print(a)") {
		t.Fatalf("indent invalid: %s", out)
	}
}

// TestCodeNode_parsePythonOutput
// 目的：验证从stdout解析JSON输出与原始文本回退
// 边界：存在JSON行与不存在JSON行
// 断言：成功标志与raw_output符合预期
func TestCodeNode_parsePythonOutput(t *testing.T) {
	p := &CodeNode{}
	s := "line\n{\"success\": true}"
	m, err := p.parsePythonOutput(s)
	if err != nil || m["success"].(bool) != true {
		t.Fatalf("parse invalid: %v %v", m, err)
	}
	s2 := "no json here"
	m2, err := p.parsePythonOutput(s2)
	if err != nil || m2["raw_output"].(string) != s2 {
		t.Fatalf("parse raw invalid: %v %v", m2, err)
	}
}

// TestCodeNode_ValidateInput_Errors
// 目的：覆盖参数校验的错误分支
// 边界：缺失code、code类型错误、timeoutSeconds类型错误、requirements类型错误、maxRetries类型错误
// 断言：每种错误均返回非nil
func TestCodeNode_ValidateInput_Errors(t *testing.T) {
	p := &CodeNode{BaseActivity: &BaseActivity{}}
	// missing code
	in := &ActivityInput{Node: &WkFLowNode{Type: "*.code", Parameters: map[string]interface{}{}}}
	if err := p.ValidateInput(in); err == nil {
		t.Fatalf("expected missing code error")
	}
	// code not string
	in = &ActivityInput{Node: &WkFLowNode{Type: "*.code", Parameters: map[string]interface{}{"code": 123}}}
	if err := p.ValidateInput(in); err == nil {
		t.Fatalf("expected code type error")
	}
	// timeout wrong type
	in = &ActivityInput{Node: &WkFLowNode{Type: "*.code", Parameters: map[string]interface{}{"code": "x", "timeoutSeconds": "abc"}}}
	if err := p.ValidateInput(in); err == nil {
		t.Fatalf("expected timeout type error")
	}
	// requirements wrong type
	in = &ActivityInput{Node: &WkFLowNode{Type: "*.code", Parameters: map[string]interface{}{"code": "x", "requirements": 123}}}
	if err := p.ValidateInput(in); err == nil {
		t.Fatalf("expected requirements type error")
	}
	// maxRetries wrong type
	in = &ActivityInput{Node: &WkFLowNode{Type: "*.code", Parameters: map[string]interface{}{"code": "x", "maxRetries": "a"}}}
	if err := p.ValidateInput(in); err == nil {
		t.Fatalf("expected maxRetries type error")
	}
}
