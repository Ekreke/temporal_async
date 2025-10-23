package node

import (
	"fmt"
	"strings"
	"testing"
)

func TestPythonDockerNode_ValidateInput(t *testing.T) {
	tests := []struct {
		name        string
		input       *ActivityInput
		expectError bool
	}{
		{
			name: "有效输入验证",
			input: &ActivityInput{
				NodeID:   "python-1",
				NodeName: "Python Docker Node",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"code": "print('Hello, World!')",
				},
				WorkflowID:  "workflow-1",
				ExecutionID: "exec-1",
			},
			expectError: false,
		},
		{
			name: "完整参数验证",
			input: &ActivityInput{
				NodeID:   "python-2",
				NodeName: "Python Docker Node Full",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"code":           "result = {'data': input_data['value'] * 2}",
					"dockerImage":    "python:3.10-slim",
					"timeoutSeconds": 30,
					"maxRetries":     2,
					"workingDir":     "/workspace",
					"environmentVars": map[string]interface{}{
						"ENV1": "value1",
						"ENV2": "value2",
					},
					"requirements": []interface{}{
						"requests",
						"numpy",
					},
					"inputData": map[string]interface{}{
						"value": 21,
					},
				},
				WorkflowID:  "workflow-2",
				ExecutionID: "exec-2",
			},
			expectError: false,
		},
		{
			name: "无效节点类型",
			input: &ActivityInput{
				NodeID:   "python-3",
				NodeName: "Invalid Node Type",
				NodeType: "invalid-type",
				Parameters: map[string]interface{}{
					"code": "print('test')",
				},
				WorkflowID:  "workflow-3",
				ExecutionID: "exec-3",
			},
			expectError: true,
		},
		{
			name: "缺少parameters",
			input: &ActivityInput{
				NodeID:      "python-4",
				NodeName:    "Missing Parameters",
				NodeType:    "n8n-nodes-base.pythonDocker",
				Parameters:  nil,
				WorkflowID:  "workflow-4",
				ExecutionID: "exec-4",
			},
			expectError: true,
		},
		{
			name: "缺少code参数",
			input: &ActivityInput{
				NodeID:   "python-5",
				NodeName: "Missing Code",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"dockerImage": "python:3.11-slim",
				},
				WorkflowID:  "workflow-5",
				ExecutionID: "exec-5",
			},
			expectError: true,
		},
		{
			name: "code参数类型错误",
			input: &ActivityInput{
				NodeID:   "python-6",
				NodeName: "Invalid Code Type",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"code": 123,
				},
				WorkflowID:  "workflow-6",
				ExecutionID: "exec-6",
			},
			expectError: true,
		},
		{
			name: "timeoutSeconds超出范围",
			input: &ActivityInput{
				NodeID:   "python-7",
				NodeName: "Invalid Timeout",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"code":           "print('test')",
					"timeoutSeconds": 400,
				},
				WorkflowID:  "workflow-7",
				ExecutionID: "exec-7",
			},
			expectError: true,
		},
		{
			name: "maxRetries超出范围",
			input: &ActivityInput{
				NodeID:   "python-8",
				NodeName: "Invalid Max Retries",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"code":       "print('test')",
					"maxRetries": 15,
				},
				WorkflowID:  "workflow-8",
				ExecutionID: "exec-8",
			},
			expectError: true,
		},
		{
			name: "requirements格式无效",
			input: &ActivityInput{
				NodeID:   "python-9",
				NodeName: "Invalid Requirements",
				NodeType: "n8n-nodes-base.pythonDocker",
				Parameters: map[string]interface{}{
					"code":         "print('test')",
					"requirements": "invalid",
				},
				WorkflowID:  "workflow-9",
				ExecutionID: "exec-9",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pythonNode := NewPythonDockerNodeActivity()

			err := pythonNode.ValidateInput(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateInput() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

func TestPythonDockerNode_parseParameters(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	pythonNodeInstance := pythonNode.(*PythonDockerNode)

	tests := []struct {
		name        string
		parameters  map[string]interface{}
		expectError bool
		expected    PythonDockerNodeParameters
	}{
		{
			name: "最小参数解析",
			parameters: map[string]interface{}{
				"code": "print('Hello')",
			},
			expectError: false,
			expected: PythonDockerNodeParameters{
				Code:            "print('Hello')",
				DockerImage:     "python:3.11-slim",
				TimeoutSeconds:  60,
				MaxRetries:      3,
				WorkingDir:      "/app",
				EnvironmentVars: map[string]string{},
				Requirements:    []string{"requests"},
				InputData:       map[string]interface{}{},
			},
		},
		{
			name: "完整参数解析",
			parameters: map[string]interface{}{
				"code":           "result = input_data['value'] * 2",
				"dockerImage":    "python:3.10-slim",
				"timeoutSeconds": 30,
				"maxRetries":     1,
				"workingDir":     "/workspace",
				"environmentVars": map[string]interface{}{
					"API_KEY": "secret123",
					"DEBUG":   "true",
				},
				"requirements": []interface{}{
					"requests",
					"pandas",
					"numpy",
				},
				"inputData": map[string]interface{}{
					"value": 50,
					"name":  "test",
				},
			},
			expectError: false,
			expected: PythonDockerNodeParameters{
				Code:           "result = input_data['value'] * 2",
				DockerImage:    "python:3.10-slim",
				TimeoutSeconds: 30,
				MaxRetries:     1,
				WorkingDir:     "/workspace",
				EnvironmentVars: map[string]string{
					"API_KEY": "secret123",
					"DEBUG":   "true",
				},
				Requirements: []string{"requests", "pandas", "numpy"},
				InputData: map[string]interface{}{
					"value": 50,
					"name":  "test",
				},
			},
		},
		{
			name: "缺少code参数",
			parameters: map[string]interface{}{
				"dockerImage": "python:3.11-slim",
			},
			expectError: true,
		},
		{
			name: "无效的timeoutSeconds",
			parameters: map[string]interface{}{
				"code":           "print('test')",
				"timeoutSeconds": -10,
			},
			expectError: true,
		},
		{
			name: "无效的maxRetries",
			parameters: map[string]interface{}{
				"code":       "print('test')",
				"maxRetries": -5,
			},
			expectError: true,
		},
		{
			name: "无效的environmentVars格式",
			parameters: map[string]interface{}{
				"code":            "print('test')",
				"environmentVars": "invalid",
			},
			expectError: true,
		},
		{
			name: "无效的requirements格式",
			parameters: map[string]interface{}{
				"code":         "print('test')",
				"requirements": "invalid",
			},
			expectError: true,
		},
		{
			name: "requirements包含非字符串项",
			parameters: map[string]interface{}{
				"code": "print('test')",
				"requirements": []interface{}{
					"requests",
					123,
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params PythonDockerNodeParameters
			err := pythonNodeInstance.parseParameters(tt.parameters, &params)

			if (err != nil) != tt.expectError {
				t.Errorf("parseParameters() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError {
				if params.Code != tt.expected.Code {
					t.Errorf("期望code为%s，实际: %s", tt.expected.Code, params.Code)
				}
				if params.DockerImage != tt.expected.DockerImage {
					t.Errorf("期望dockerImage为%s，实际: %s", tt.expected.DockerImage, params.DockerImage)
				}
				if params.TimeoutSeconds != tt.expected.TimeoutSeconds {
					t.Errorf("期望timeoutSeconds为%d，实际: %d", tt.expected.TimeoutSeconds, params.TimeoutSeconds)
				}
				if params.MaxRetries != tt.expected.MaxRetries {
					t.Errorf("期望maxRetries为%d，实际: %d", tt.expected.MaxRetries, params.MaxRetries)
				}
				if params.WorkingDir != tt.expected.WorkingDir {
					t.Errorf("期望workingDir为%s，实际: %s", tt.expected.WorkingDir, params.WorkingDir)
				}
				if len(params.Requirements) != len(tt.expected.Requirements) {
					t.Errorf("期望requirements数量为%d，实际: %d", len(tt.expected.Requirements), len(params.Requirements))
				}
				if len(params.InputData) != len(tt.expected.InputData) {
					t.Errorf("期望inputData数量为%d，实际: %d", len(tt.expected.InputData), len(params.InputData))
				}
			}
		})
	}
}

func TestPythonDockerNode_GetNodeInfo(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	info := pythonNode.GetNodeInfo()

	if info.ID != "python-docker-node" {
		t.Errorf("期望节点ID为 'python-docker-node'，实际: %s", info.ID)
	}

	if info.Name != "Python Docker Node" {
		t.Errorf("期望节点名称为 'Python Docker Node'，实际: %s", info.Name)
	}

	if info.Type != "n8n-nodes-base.pythonDocker" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.pythonDocker'，实际: %s", info.Type)
	}

	if info.Category != "execution" {
		t.Errorf("期望节点分类为 'execution'，实际: %s", info.Category)
	}

	if info.Version != "1.0.0" {
		t.Errorf("期望节点版本为 '1.0.0'，实际: %s", info.Version)
	}
}

func TestPythonDockerNode_GeneratePythonScript(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	pythonNodeInstance := pythonNode.(*PythonDockerNode)

	params := PythonDockerNodeParameters{
		Code: "result = input_data['value'] * 2\nprint('Calculation complete')",
	}

	inputData := map[string]interface{}{
		"value": 21,
		"name":  "test",
	}

	script := pythonNodeInstance.generatePythonScript(params, inputData)

	// 检查脚本是否包含关键元素
	if !strings.Contains(script, "input_data = ") {
		t.Error("脚本应包含input_data定义")
	}

	if !strings.Contains(script, params.Code) {
		t.Error("脚本应包含用户代码")
	}

	if !strings.Contains(script, "def main():") {
		t.Error("脚本应包含main函数")
	}

	if !strings.Contains(script, "print(json.dumps(result") {
		t.Error("脚本应包含结果输出")
	}

	t.Logf("生成的Python脚本:\n%s", script)
}

func TestPythonDockerNode_MapToPythonJSON(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	pythonNodeInstance := pythonNode.(*PythonDockerNode)

	tests := []struct {
		name     string
		data     map[string]interface{}
		expected string
	}{
		{
			name: "简单map",
			data: map[string]interface{}{
				"key1": "value1",
				"key2": 42,
			},
			expected: `{"key1": "value1", "key2": 42}`,
		},
		{
			name: "嵌套map",
			data: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "John",
					"age":  30,
				},
				"active": true,
			},
			expected: `{"user": {"name": "John", "age": 30}, "active": true}`,
		},
		{
			name: "数组",
			data: map[string]interface{}{
				"items": []interface{}{"a", "b", "c"},
				"count": 3,
			},
			expected: `{"items": ["a", "b", "c"], "count": 3}`,
		},
		{
			name:     "空map",
			data:     map[string]interface{}{},
			expected: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pythonNodeInstance.mapToPythonJSON(tt.data)

			// 由于map遍历顺序不确定，我们检查关键元素而不是精确匹配
			if !strings.Contains(result, "{") || !strings.Contains(result, "}") {
				t.Errorf("结果应该包含花括号: %s", result)
			}

			for k := range tt.data {
				if !strings.Contains(result, fmt.Sprintf(`"%s"`, k)) {
					t.Errorf("结果应该包含键 %s: %s", k, result)
				}
			}

			t.Logf("输入: %+v", tt.data)
			t.Logf("输出: %s", result)
		})
	}
}

func TestPythonDockerNode_ParsePythonOutput(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	pythonNodeInstance := pythonNode.(*PythonDockerNode)

	tests := []struct {
		name         string
		output       string
		expectError  bool
		checkSuccess bool
	}{
		{
			name: "JSON输出",
			output: `Some log messages
{
  "success": true,
  "message": "Execution completed",
  "data": {"result": 42}
}`,
			expectError:  false,
			checkSuccess: true,
		},
		{
			name:         "纯文本输出",
			output:       "Hello, World!\nExecution completed successfully.",
			expectError:  false,
			checkSuccess: true,
		},
		{
			name:        "空输出",
			output:      "",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pythonNodeInstance.parsePythonOutput(tt.output)

			if (err != nil) != tt.expectError {
				t.Errorf("parsePythonOutput() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError && tt.checkSuccess {
				if result["success"] != true {
					t.Errorf("期望success=true，实际: %v", result["success"])
				}

				if result["message"] == nil {
					t.Error("期望有message字段")
				}
			}

			t.Logf("输出: %s", tt.output)
			t.Logf("解析结果: %+v", result)
		})
	}
}

func TestPythonDockerNode_HelperMethods(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	pythonNodeInstance := pythonNode.(*PythonDockerNode)

	t.Run("测试GetDockerImage", func(t *testing.T) {
		// 测试默认值
		image := pythonNodeInstance.GetDockerImage(map[string]interface{}{})
		if image != "python:3.11-slim" {
			t.Errorf("期望默认镜像为python:3.11-slim，实际: %s", image)
		}

		// 测试自定义值
		image = pythonNodeInstance.GetDockerImage(map[string]interface{}{
			"dockerImage": "python:3.10-alpine",
		})
		if image != "python:3.10-alpine" {
			t.Errorf("期望自定义镜像为python:3.10-alpine，实际: %s", image)
		}
	})

	t.Run("测试GetTimeoutSeconds", func(t *testing.T) {
		// 测试默认值
		timeout := pythonNodeInstance.GetTimeoutSeconds(map[string]interface{}{})
		if timeout != 60 {
			t.Errorf("期望默认超时为60秒，实际: %d", timeout)
		}

		// 测试自定义值
		timeout = pythonNodeInstance.GetTimeoutSeconds(map[string]interface{}{
			"timeoutSeconds": 120,
		})
		if timeout != 120 {
			t.Errorf("期望自定义超时为120秒，实际: %d", timeout)
		}
	})

	t.Run("测试GetMaxRetries", func(t *testing.T) {
		// 测试默认值
		retries := pythonNodeInstance.GetMaxRetries(map[string]interface{}{})
		if retries != 3 {
			t.Errorf("期望默认重试次数为3，实际: %d", retries)
		}

		// 测试自定义值
		retries = pythonNodeInstance.GetMaxRetries(map[string]interface{}{
			"maxRetries": 5,
		})
		if retries != 5 {
			t.Errorf("期望自定义重试次数为5，实际: %d", retries)
		}
	})
}

func TestPythonDockerNode_IsDockerAvailable(t *testing.T) {
	pythonNode := NewPythonDockerNodeActivity()
	pythonNodeInstance := pythonNode.(*PythonDockerNode)

	// 这个测试的结果取决于系统是否安装了Docker
	available := pythonNodeInstance.isDockerAvailable()
	t.Logf("Docker可用性: %v", available)

	// 我们不强制要求Docker必须可用，只是检查函数能正常执行
	if available {
		t.Log("Docker可用，可以执行Python Docker节点")
	} else {
		t.Log("Docker不可用，Python Docker节点将无法执行")
	}
}
