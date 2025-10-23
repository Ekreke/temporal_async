package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// PythonCodeActivity Python 代码执行 Activity
type PythonCodeActivity struct {
	BaseActivity
}

// NewPythonCodeActivity 创建新的Python代码执行节点
func NewPythonCodeActivity() *PythonCodeActivity {
	activity := &PythonCodeActivity{
		BaseActivity: BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "python_code",
				Name:        "Python Code Execution",
				Type:        "PYTHON.pythonCode",
				Description: "Execute Python code scripts",
				Version:     "1.0.0",
				Category:    "code",
				Icon:        "🐍",
			},
		},
	}
	// 初始化表达式评估器
	activity.InitExpressionEvaluator(nil)
	return activity
}

// Execute 执行节点逻辑（实现NodeActivity接口）
func (a *PythonCodeActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return a.ExecuteWithExecuteTiming(ctx, input, a.executePythonCode)
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *PythonCodeActivity) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}

	// 检查Python代码参数
	if input.Parameters == nil {
		return fmt.Errorf("缺少parameters参数")
	}

	if _, exists := input.Parameters["pythonCode"]; !exists {
		return fmt.Errorf("缺少pythonCode参数")
	}

	return nil
}

// executePythonCode 内部Python代码执行逻辑
func (a *PythonCodeActivity) executePythonCode(input *ActivityInput) (map[string]interface{}, error) {
	// 获取Python代码
	pythonCode := a.GetStringParameter(input.Parameters, "pythonCode")
	if pythonCode == "" {
		return nil, fmt.Errorf("pythonCode参数不能为空")
	}

	// 获取输入数据
	inputData := input.InputData
	if inputData == nil {
		inputData = make(map[string]interface{})
	}

	// 构建Python脚本
	pythonScript := a.buildPythonScript(pythonCode, inputData)

	// 执行Python代码
	output, err := a.executePythonScript(pythonScript)
	if err != nil {
		return nil, fmt.Errorf("Python执行失败: %v", err)
	}

	// 解析输出
	result, err := a.parsePythonOutput(output)
	if err != nil {
		return nil, fmt.Errorf("解析Python输出失败: %v", err)
	}

	resultData := map[string]interface{}{
		"executed":   true,
		"output":     result,
		"codeLength": len(pythonCode),
		"scriptHash": fmt.Sprintf("%x", len(pythonScript)), // 简化hash
	}

	return resultData, nil
}

// ExecutePythonCode 执行Python代码（向后兼容）
func (a *PythonCodeActivity) ExecutePythonCode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	// 转换旧格式到新格式
	nodeInput := &ActivityInput{
		NodeID:      getStringValue(input, "nodeId"),
		NodeName:    getStringValue(input, "nodeName"),
		NodeType:    "PYTHON.pythonCode",
		InputData:   getInputData(input),
		Parameters:  getParameters(input),
		WorkflowID:  getStringValue(input, "workflowId"),
		ExecutionID: getStringValue(input, "executionId"),
	}

	// 执行新接口
	output, err := a.Execute(ctx, nodeInput)
	if err != nil {
		return nil, err
	}

	// 转换输出格式
	if output.Success {
		return map[string]interface{}{
			"success": true,
			"data":    output.Data,
		}, nil
	} else {
		return map[string]interface{}{
			"success": false,
			"error":   output.Error,
		}, nil
	}
}

// buildPythonScript 构建Python执行脚本
func (a *PythonCodeActivity) buildPythonScript(code string, inputData map[string]interface{}) string {
	// 将输入数据转换为Python代码
	inputJSON, _ := json.Marshal(inputData)

	script := `import json
import sys
import traceback

# 设置输入数据
try:
    _input = json.loads('''` + string(inputJSON) + `''')
except:
    _input = {"all": lambda: []}

# 提供的Python代码
try:
` + strings.ReplaceAll(code, "\n", "\n    ") + `

    # 确保有返回值
    if 'result' not in locals() and not isinstance(_input, dict):
        result = _input
    elif 'result' not in locals():
        result = None

    # 输出结果
    print(json.dumps({"success": True, "result": result}, ensure_ascii=False))

except Exception as e:
    print(json.dumps({
        "success": False,
        "error": str(e),
        "traceback": traceback.format_exc()
    }, ensure_ascii=False))
    sys.exit(1)
`

	return script
}

// executePythonScript 执行Python脚本
func (a *PythonCodeActivity) executePythonScript(script string) (string, error) {
	// 检查系统中是否有Python解释器
	pythonCmd := "python3"
	if _, err := exec.LookPath(pythonCmd); err != nil {
		pythonCmd = "python"
		if _, err := exec.LookPath(pythonCmd); err != nil {
			return "", fmt.Errorf("未找到Python解释器")
		}
	}

	// 执行Python脚本
	cmd := exec.Command(pythonCmd, "-c", script)
	output, err := cmd.CombinedOutput()

	return string(output), err
}

// parsePythonOutput 解析Python输出
func (a *PythonCodeActivity) parsePythonOutput(output string) (interface{}, error) {
	// 查找JSON输出
	lines := strings.Split(output, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
			var result map[string]interface{}
			err := json.Unmarshal([]byte(line), &result)
			if err == nil {
				if success, ok := result["success"].(bool); ok && success {
					return result["result"], nil
				} else {
					return nil, fmt.Errorf("Python执行错误: %v", result["error"])
				}
			}
		}
	}

	// 如果没有找到JSON，返回原始输出
	return output, nil
}
