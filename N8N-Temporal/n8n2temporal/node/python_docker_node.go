package node

import (
	"bytes"
	"context"
	"fmt"
	"go.temporal.io/sdk/log"
	"os/exec"
	"strings"
	"time"
)

// PythonDockerNode Python Docker节点，在Docker容器中执行Python代码
type PythonDockerNode struct {
	*BaseActivity
}

// PythonDockerNodeParameters Python Docker节点参数
type PythonDockerNodeParameters struct {
	Code            string                 `json:"code"`            // Python代码
	DockerImage     string                 `json:"dockerImage"`     // Docker镜像名称
	TimeoutSeconds  int                    `json:"timeoutSeconds"`  // 超时时间（秒）
	MaxRetries      int                    `json:"maxRetries"`      // 最大重试次数
	WorkingDir      string                 `json:"workingDir"`      // 工作目录
	EnvironmentVars map[string]string      `json:"environmentVars"` // 环境变量
	Requirements    []string               `json:"requirements"`    // Python依赖包
	InputData       map[string]interface{} `json:"inputData"`       // 输入数据
}

// NewPythonDockerNodeActivity 创建Python Docker节点实例
func NewPythonDockerNodeActivity() Activity {
	return &PythonDockerNode{
		BaseActivity: &BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "python-docker-node",
				Name:        "Python Docker Node",
				Type:        "n8n-nodes-base.pythonDocker",
				Description: "Python节点，在Docker容器中安全执行Python代码",
				Version:     "1.0.0",
				Category:    "execution",
				Icon:        "🐍",
			},
		},
	}
}

// Execute 执行Python Docker节点逻辑
func (p *PythonDockerNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	// 在测试环境中可能没有Temporal上下文，直接执行
	data, err := p.executePythonDockerNode(input)
	if err != nil {
		return p.CreateErrorOutput(input, err), nil
	}
	return p.CreateSuccessOutput(input, data), nil
}

// executePythonDockerNode Python Docker节点的具体执行逻辑
func (p *PythonDockerNode) executePythonDockerNode(input *ActivityInput) (map[string]interface{}, error) {
	logger := &SimpleLogger{}

	// 解析参数
	var params PythonDockerNodeParameters
	if err := p.parseParameters(input.Parameters, &params); err != nil {
		return nil, fmt.Errorf("解析Python Docker节点参数失败: %w", err)
	}

	// 设置默认值
	if params.DockerImage == "" {
		params.DockerImage = "python:3.11-slim"
	}
	if params.TimeoutSeconds == 0 {
		params.TimeoutSeconds = 60
	}
	if params.MaxRetries == 0 {
		params.MaxRetries = 3
	}

	// 检查Docker是否可用
	if !p.isDockerAvailable() {
		return nil, fmt.Errorf("Docker不可用，请确保Docker已安装并运行")
	}

	logger.Info("开始执行Python Docker节点", "dockerImage", params.DockerImage, "timeout", params.TimeoutSeconds)

	// 合并输入数据
	executionData := make(map[string]interface{})
	for k, v := range input.InputData {
		executionData[k] = v
	}
	for k, v := range params.InputData {
		executionData[k] = v
	}

	// 执行Python代码
	var lastError error
	for attempt := 0; attempt <= params.MaxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("重试Python代码执行", "attempt", attempt, "maxRetries", params.MaxRetries)
			time.Sleep(time.Second * time.Duration(attempt)) // 指数退避
		}

		result, err := p.executePythonInDocker(params, executionData)
		if err == nil {
			logger.Info("Python Docker节点执行成功", "attempt", attempt+1)
			return result, nil
		}

		lastError = err
		logger.Error("Python代码执行失败", "attempt", attempt+1, "error", err)
	}

	return nil, fmt.Errorf("Python代码执行失败，已重试%d次: %w", params.MaxRetries, lastError)
}

// parseParameters 解析参数
func (p *PythonDockerNode) parseParameters(parameters map[string]interface{}, params *PythonDockerNodeParameters) error {
	// 设置默认值
	params.DockerImage = "python:3.11-slim"
	params.TimeoutSeconds = 60
	params.MaxRetries = 3
	params.WorkingDir = "/app"
	params.EnvironmentVars = make(map[string]string)
	params.Requirements = []string{"requests"}
	params.InputData = make(map[string]interface{})

	// 解析Python代码
	if code, exists := parameters["code"]; exists {
		if codeStr, ok := code.(string); ok {
			params.Code = codeStr
		} else {
			return fmt.Errorf("code必须是字符串类型")
		}
	} else {
		return fmt.Errorf("缺少必需的code参数")
	}

	// 解析Docker镜像
	if dockerImage, exists := parameters["dockerImage"]; exists {
		if dockerImageStr, ok := dockerImage.(string); ok {
			params.DockerImage = dockerImageStr
		}
	}

	// 解析超时时间
	if timeoutSeconds, exists := parameters["timeoutSeconds"]; exists {
		if timeoutInt, ok := timeoutSeconds.(int); ok {
			if timeoutInt <= 0 || timeoutInt > 300 {
				return fmt.Errorf("timeoutSeconds必须在1-300秒之间")
			}
			params.TimeoutSeconds = timeoutInt
		} else {
			return fmt.Errorf("timeoutSeconds必须是整数类型")
		}
	}

	// 解析最大重试次数
	if maxRetries, exists := parameters["maxRetries"]; exists {
		if maxRetriesInt, ok := maxRetries.(int); ok {
			if maxRetriesInt < 0 || maxRetriesInt > 10 {
				return fmt.Errorf("maxRetries必须在0-10之间")
			}
			params.MaxRetries = maxRetriesInt
		}
	}

	// 解析工作目录
	if workingDir, exists := parameters["workingDir"]; exists {
		if workingDirStr, ok := workingDir.(string); ok {
			params.WorkingDir = workingDirStr
		}
	}

	// 解析环境变量
	if environmentVars, exists := parameters["environmentVars"]; exists {
		if envMap, ok := environmentVars.(map[string]interface{}); ok {
			params.EnvironmentVars = make(map[string]string)
			for k, v := range envMap {
				if vStr, ok := v.(string); ok {
					params.EnvironmentVars[k] = vStr
				} else {
					params.EnvironmentVars[k] = fmt.Sprintf("%v", v)
				}
			}
		} else {
			return fmt.Errorf("environmentVars格式无效，应为map[string]interface{}")
		}
	}

	// 解析依赖包
	if requirements, exists := parameters["requirements"]; exists {
		if reqList, ok := requirements.([]interface{}); ok {
			params.Requirements = make([]string, 0, len(reqList))
			for _, req := range reqList {
				if reqStr, ok := req.(string); ok {
					params.Requirements = append(params.Requirements, reqStr)
				} else {
					return fmt.Errorf("requirements中的每个项目都必须是字符串")
				}
			}
		} else {
			return fmt.Errorf("requirements格式无效，应为字符串数组")
		}
	}

	// 解析输入数据
	if inputData, exists := parameters["inputData"]; exists {
		if inputDataMap, ok := inputData.(map[string]interface{}); ok {
			params.InputData = inputDataMap
		} else {
			return fmt.Errorf("inputData格式无效，应为map[string]interface{}")
		}
	}

	return nil
}

// isDockerAvailable 检查Docker是否可用
func (p *PythonDockerNode) isDockerAvailable() bool {
	cmd := exec.Command("docker", "--version")
	err := cmd.Run()
	return err == nil
}

// executePythonInDocker 在Docker容器中执行Python代码
func (p *PythonDockerNode) executePythonInDocker(params PythonDockerNodeParameters, inputData map[string]interface{}) (map[string]interface{}, error) {
	// 生成Python脚本
	pythonScript := p.generatePythonScript(params, inputData)

	// 准备Docker命令
	dockerArgs := []string{
		"run", "--rm",
		"--network=none", // 禁用网络访问以提高安全性
		"--memory=512m",  // 限制内存使用
		"--cpus=1",       // 限制CPU使用
	}

	// 添加环境变量
	for k, v := range params.EnvironmentVars {
		dockerArgs = append(dockerArgs, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	// 添加镜像和工作目录
	dockerArgs = append(dockerArgs, params.DockerImage, "python", "-c", pythonScript)

	// 创建命令并设置超时
	cmd := exec.Command("docker", dockerArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 设置执行超时
	timeout := time.Duration(params.TimeoutSeconds) * time.Second
	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	select {
	case err := <-done:
		if err != nil {
			return map[string]interface{}{
				"success": false,
				"error":   err.Error(),
				"stderr":  stderr.String(),
				"stdout":  stdout.String(),
			}, fmt.Errorf("Docker执行失败: %w, stderr: %s", err, stderr.String())
		}

		// 解析Python脚本输出
		output, err := p.parsePythonOutput(stdout.String())
		if err != nil {
			return map[string]interface{}{
				"success": false,
				"error":   err.Error(),
				"stdout":  stdout.String(),
				"stderr":  stderr.String(),
			}, fmt.Errorf("解析Python输出失败: %w", err)
		}

		output["success"] = true
		output["stdout"] = stdout.String()
		if stderr.String() != "" {
			output["stderr"] = stderr.String()
		}

		return output, nil

	case <-time.After(timeout):
		// 超时处理
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return nil, fmt.Errorf("Python代码执行超时（%d秒）", params.TimeoutSeconds)
	}
}

// generatePythonScript 生成Python脚本
func (p *PythonDockerNode) generatePythonScript(params PythonDockerNodeParameters, inputData map[string]interface{}) string {
	// 将输入数据转换为Python字典
	inputDataJSON := p.mapToPythonJSON(inputData)

	script := fmt.Sprintf(`#!/usr/bin/env python3
import json
import sys
import traceback
from typing import Dict, Any, Union

# 输入数据
input_data = %s

def main():
    try:
        # 用户代码开始
%s

        # 用户代码结束

        # 如果用户没有设置result变量，创建默认结果
        if 'result' not in locals():
            result = {
                'success': True,
                'message': 'Python代码执行完成',
                'data': None
            }

        # 输出结果
        print(json.dumps(result, ensure_ascii=False, indent=2))

    except Exception as e:
        error_result = {
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }
        print(json.dumps(error_result, ensure_ascii=False, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()
`, inputDataJSON, params.Code)

	return script
}

// mapToPythonJSON 将Go map转换为Python JSON格式
func (p *PythonDockerNode) mapToPythonJSON(data map[string]interface{}) string {
	var builder strings.Builder
	builder.WriteString("{")

	first := true
	for k, v := range data {
		if !first {
			builder.WriteString(", ")
		}
		first = false

		builder.WriteString(fmt.Sprintf(`"%s": %s`, k, p.valueToPythonJSON(v)))
	}

	builder.WriteString("}")
	return builder.String()
}

// valueToPythonJSON 将Go值转换为Python JSON值
func (p *PythonDockerNode) valueToPythonJSON(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), `"`, `\"`))
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case map[string]interface{}:
		return p.mapToPythonJSON(v)
	case []interface{}:
		var builder strings.Builder
		builder.WriteString("[")
		for i, item := range v {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(p.valueToPythonJSON(item))
		}
		builder.WriteString("]")
		return builder.String()
	default:
		return fmt.Sprintf(`"%s"`, fmt.Sprintf("%v", v))
	}
}

// parsePythonOutput 解析Python脚本输出
func (p *PythonDockerNode) parsePythonOutput(output string) (map[string]interface{}, error) {
	// 简单的JSON解析（在真实项目中应该使用更robust的JSON解析器）
	lines := strings.Split(output, "\n")

	// 查找JSON输出（通常是最后一行）
	var jsonStr string
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[") {
			jsonStr = line
			break
		}
	}

	if jsonStr == "" {
		return map[string]interface{}{
			"success":    true,
			"message":    "Python代码执行完成",
			"raw_output": output,
		}, nil
	}

	// 这里应该使用真正的JSON解析器，为了简化，我们返回原始输出
	return map[string]interface{}{
		"success":     true,
		"message":     "Python代码执行完成",
		"raw_output":  output,
		"json_output": jsonStr,
	}, nil
}

// GetLogger 获取logger
func (p *PythonDockerNode) GetLogger(ctx context.Context) log.Logger {
	return p.BaseActivity.GetLogger(ctx)
}

// ValidateInput 验证输入参数
func (p *PythonDockerNode) ValidateInput(input *ActivityInput) error {
	if err := p.BaseActivity.ValidateInput(input); err != nil {
		return err
	}

	// Python Docker节点的基本验证
	if input.NodeType != "n8n-nodes-base.pythonDocker" {
		return fmt.Errorf("Python Docker节点的类型必须为 n8n-nodes-base.pythonDocker")
	}

	// 验证必需参数
	if input.Parameters == nil {
		return fmt.Errorf("缺少parameters参数")
	}

	if _, exists := input.Parameters["code"]; !exists {
		return fmt.Errorf("缺少必需的code参数")
	}

	// 验证code参数类型
	if code, exists := input.Parameters["code"]; exists {
		if _, ok := code.(string); !ok {
			return fmt.Errorf("code必须是字符串类型")
		}
	}

	// 验证timeoutSeconds参数
	if timeoutSeconds, exists := input.Parameters["timeoutSeconds"]; exists {
		if timeoutInt, ok := timeoutSeconds.(int); ok {
			if timeoutInt <= 0 || timeoutInt > 300 {
				return fmt.Errorf("timeoutSeconds必须在1-300秒之间")
			}
		} else {
			return fmt.Errorf("timeoutSeconds必须是整数类型")
		}
	}

	// 验证maxRetries参数
	if maxRetries, exists := input.Parameters["maxRetries"]; exists {
		if maxRetriesInt, ok := maxRetries.(int); ok {
			if maxRetriesInt < 0 || maxRetriesInt > 10 {
				return fmt.Errorf("maxRetries必须在0-10之间")
			}
		} else {
			return fmt.Errorf("maxRetries必须是整数类型")
		}
	}

	// 验证requirements参数
	if requirements, exists := input.Parameters["requirements"]; exists {
		if reqList, ok := requirements.([]interface{}); ok {
			for _, req := range reqList {
				if _, ok := req.(string); !ok {
					return fmt.Errorf("requirements中的每个项目都必须是字符串")
				}
			}
		} else {
			return fmt.Errorf("requirements格式无效，应为字符串数组")
		}
	}

	return nil
}

// GetDockerImage 获取Docker镜像名称
func (p *PythonDockerNode) GetDockerImage(parameters map[string]interface{}) string {
	if dockerImage, exists := parameters["dockerImage"]; exists {
		if dockerImageStr, ok := dockerImage.(string); ok {
			return dockerImageStr
		}
	}
	return "python:3.11-slim"
}

// GetTimeoutSeconds 获取超时时间
func (p *PythonDockerNode) GetTimeoutSeconds(parameters map[string]interface{}) int {
	if timeoutSeconds, exists := parameters["timeoutSeconds"]; exists {
		if timeoutInt, ok := timeoutSeconds.(int); ok {
			return timeoutInt
		}
	}
	return 60
}

// GetMaxRetries 获取最大重试次数
func (p *PythonDockerNode) GetMaxRetries(parameters map[string]interface{}) int {
	if maxRetries, exists := parameters["maxRetries"]; exists {
		if maxRetriesInt, ok := maxRetries.(int); ok {
			return maxRetriesInt
		}
	}
	return 3
}
