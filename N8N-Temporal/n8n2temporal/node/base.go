package node

import (
	"context"
	"fmt"
	activitySdk "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ActivityResult Activity执行结果
type ActivityResult struct {
	Success bool                   `json:"success"`
	Data    map[string]interface{} `json:"data"`
	Error   string                 `json:"error,omitempty"`
}

// Activity 统一的节点接口
type Activity interface {
	// GetNodeInfo 获取节点基本信息
	GetNodeInfo() *ActivityInfo
	// Execute 执行节点逻辑
	Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error)
	// ValidateInput 验证输入参数
	ValidateInput(input *ActivityInput) error
	// GetLogger 获取logger
	GetLogger(ctx context.Context) log.Logger
}

// ActivityInfo 节点基本信息
type ActivityInfo struct {
	ID          string `json:"id"`          // 节点ID
	Name        string `json:"name"`        // 节点名称
	Type        string `json:"type"`        // 节点类型
	Description string `json:"description"` // 节点描述
	Version     string `json:"version"`     // 节点版本
	Category    string `json:"category"`    // 节点分类
	Icon        string `json:"icon"`        // 节点图标
}

// ActivityInput 节点输入数据
type ActivityInput struct {
	NodeID      string                 `json:"nodeId"`      // 节点ID
	NodeName    string                 `json:"nodeName"`    // 节点名称
	NodeType    string                 `json:"nodeType"`    // 节点类型
	InputData   map[string]interface{} `json:"inputData"`   // 输入数据
	Parameters  map[string]interface{} `json:"parameters"`  // 节点参数
	WorkflowID  string                 `json:"workflowId"`  // 工作流ID
	ExecutionID string                 `json:"executionId"` // 执行ID
}

// ActivityOutput 节点输出数据
type ActivityOutput struct {
	NodeID      string                 `json:"nodeId"`      // 节点ID
	NodeName    string                 `json:"nodeName"`    // 节点名称
	NodeType    string                 `json:"nodeType"`    // 节点类型
	Success     bool                   `json:"success"`     // 执行是否成功
	Data        map[string]interface{} `json:"data"`        // 输出数据
	Error       string                 `json:"error"`       // 错误信息
	ProcessedAt time.Time              `json:"processedAt"` // 处理时间
	Metadata    map[string]interface{} `json:"metadata"`    // 元数据
}

// BaseActivity Activity基类，提供通用功能
type BaseActivity struct {
	NodeInfo            *ActivityInfo
	expressionEvaluator *ExpressionEvaluator // 表达式评估器
}

// GetNodeInfo 获取节点信息（BaseActivity实现）
func (a *BaseActivity) GetNodeInfo() *ActivityInfo {
	return a.NodeInfo
}

// InitExpressionEvaluator 初始化表达式评估器
func (a *BaseActivity) InitExpressionEvaluator(workflowContext *WorkflowContext) {
	if workflowContext == nil {
		workflowContext = NewWorkflowContext()
	}
	a.expressionEvaluator = NewExpressionEvaluator(workflowContext)
}

// GetExpressionEvaluator 获取表达式评估器
func (a *BaseActivity) GetExpressionEvaluator() *ExpressionEvaluator {
	return a.expressionEvaluator
}

// SetWorkflowContext 设置工作流上下文
func (a *BaseActivity) SetWorkflowContext(workflowContext *WorkflowContext) {
	a.expressionEvaluator = NewExpressionEvaluator(workflowContext)
}

// AddNodeDataToContext 将节点执行结果添加到工作流上下文
func (a *BaseActivity) AddNodeDataToContext(nodeName string, data map[string]interface{}) {
	if a.expressionEvaluator != nil {
		a.expressionEvaluator.workflowContext.SetNodeData(nodeName, data)
	}
}

// GetGlobalParametersForNode 为指定节点获取全局参数
func (a *BaseActivity) GetGlobalParametersForNode(nodeName string) map[string]interface{} {
	if a.expressionEvaluator == nil || a.expressionEvaluator.workflowContext == nil {
		return nil
	}

	globalData, exists := a.expressionEvaluator.workflowContext.GetNodeData("__global__")
	if !exists {
		return nil
	}

	globalContext := globalData
	if globalContext == nil {
		return nil
	}

	globalParamsInterface, exists := globalContext["globalParameters"]
	if !exists {
		return nil
	}

	globalParams, ok := globalParamsInterface.(map[string]map[string]interface{})
	if !ok {
		return nil
	}

	if nodeParams, exists := globalParams[nodeName]; exists {
		return nodeParams
	}

	return nil
}

// GetVariableData 获取变量节点存储的数据
func (a *BaseActivity) GetVariableData() map[string]interface{} {
	if a.expressionEvaluator == nil || a.expressionEvaluator.workflowContext == nil {
		return nil
	}

	varData, exists := a.expressionEvaluator.workflowContext.GetNodeData("__variables__")
	if !exists {
		return nil
	}

	return varData
}

// ValidateInput 基础输入验证
func (a *BaseActivity) ValidateInput(input *ActivityInput) error {
	if input == nil {
		return fmt.Errorf("输入不能为空")
	}
	if input.NodeType == "" {
		return fmt.Errorf("节点类型不能为空")
	}
	return nil
}

// CreateSuccessOutput 创建成功输出
func (a *BaseActivity) CreateSuccessOutput(input *ActivityInput, data map[string]interface{}) *ActivityOutput {
	if data == nil {
		data = make(map[string]interface{})
	}
	return &ActivityOutput{
		NodeID:      input.NodeID,
		NodeName:    input.NodeName,
		NodeType:    input.NodeType,
		Success:     true,
		Data:        data,
		ProcessedAt: time.Now(),
		Metadata: map[string]interface{}{
			"executionTime": time.Now().Unix(),
			"nodeVersion":   a.NodeInfo.Version,
		},
	}
}

// CreateErrorOutput 创建错误输出
func (a *BaseActivity) CreateErrorOutput(input *ActivityInput, err error) *ActivityOutput {
	return &ActivityOutput{
		NodeID:      input.NodeID,
		NodeName:    input.NodeName,
		NodeType:    input.NodeType,
		Success:     false,
		Error:       err.Error(),
		ProcessedAt: time.Now(),
		Metadata: map[string]interface{}{
			"executionTime": time.Now().Unix(),
			"nodeVersion":   a.NodeInfo.Version,
		},
	}
}

// 执行逻辑
type nodeExecFunc func(input *ActivityInput) (map[string]interface{}, error)

// ExecuteWithExecuteTiming 带时间监控的节点执行
func (a *BaseActivity) ExecuteWithExecuteTiming(ctx context.Context, input *ActivityInput, executeFunc nodeExecFunc) (*ActivityOutput, error) {
	logger := a.GetLogger(ctx)
	startTime := time.Now()
	logger.Info("开始执行节点", "nodeType", input.NodeType, "nodeId", input.NodeID, "nodeName", input.NodeName)

	// 执行具体逻辑
	data, err := executeFunc(input)
	duration := time.Since(startTime)

	if err != nil {
		logger.Error("节点执行失败", "nodeType", input.NodeType, "nodeId", input.NodeID, "error", err, "duration", duration.String())
		return a.CreateErrorOutput(input, err), nil
	}

	// 创建输出
	output := a.CreateSuccessOutput(input, data)
	output.Metadata["executionDuration"] = duration.String()

	// 将节点执行结果添加到工作流上下文（覆盖之前的同名节点数据）
	a.AddNodeDataToContext(input.NodeName, output.Data)

	logger.Info("节点执行成功", "nodeType", input.NodeType, "nodeId", input.NodeID, "duration", duration.String())
	return output, nil
}

// GetLogger 获取activity logger
func (a *BaseActivity) GetLogger(ctx context.Context) log.Logger {
	return activitySdk.GetLogger(ctx)
}

// GetStringParameter 安全获取字符串参数
func (a *BaseActivity) GetStringParameter(parameters map[string]interface{}, key string) string {
	if value, exists := parameters[key]; exists {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

// getStringValue 安全获取字符串值
func getStringValue(data map[string]interface{}, key string) string {
	if value, exists := data[key]; exists {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

// getInputData 获取输入数据
func getInputData(input map[string]interface{}) map[string]interface{} {
	if data, exists := input["inputData"]; exists {
		if inputData, ok := data.(map[string]interface{}); ok {
			return inputData
		}
	}
	return make(map[string]interface{})
}

// getParameters 获取参数
func getParameters(input map[string]interface{}) map[string]interface{} {
	if data, exists := input["parameters"]; exists {
		if params, ok := data.(map[string]interface{}); ok {
			return params
		}
	}
	return make(map[string]interface{})
}

// WorkflowContext 工作流上下文管理器
type WorkflowContext struct {
	context map[string]interface{} // 存储每个节点的最新执行结果
}

// NewWorkflowContext 创建新的工作流上下文
func NewWorkflowContext() *WorkflowContext {
	return &WorkflowContext{
		context: make(map[string]interface{}),
	}
}

// SetNodeData 设置节点的最新数据（覆盖之前的）
func (wc *WorkflowContext) SetNodeData(nodeName string, data map[string]interface{}) {
	wc.context[nodeName] = data
}

// GetNodeData 获取节点的最新数据
func (wc *WorkflowContext) GetNodeData(nodeName string) (map[string]interface{}, bool) {
	data, exists := wc.context[nodeName]
	if !exists {
		return nil, false
	}
	if nodeMap, ok := data.(map[string]interface{}); ok {
		return nodeMap, true
	}
	return nil, false
}

// GetAllContext 获取完整的上下文
func (wc *WorkflowContext) GetAllContext() map[string]interface{} {
	return wc.context
}

// ExpressionEvaluator n8n表达式评估器
type ExpressionEvaluator struct {
	workflowContext *WorkflowContext // 工作流上下文管理器
}

// NewExpressionEvaluator 创建新的表达式评估器
func NewExpressionEvaluator(workflowContext *WorkflowContext) *ExpressionEvaluator {
	if workflowContext == nil {
		workflowContext = NewWorkflowContext()
	}
	return &ExpressionEvaluator{
		workflowContext: workflowContext,
	}
}

// SetWorkflowContext 设置工作流上下文
func (e *ExpressionEvaluator) SetWorkflowContext(workflowContext *WorkflowContext) {
	e.workflowContext = workflowContext
}

// GetWorkflowContext 获取工作流上下文
func (e *ExpressionEvaluator) GetWorkflowContext() *WorkflowContext {
	return e.workflowContext
}

// EvaluateExpression 评估 n8n 表达式（支持完整的工作流上下文）
func (e *ExpressionEvaluator) EvaluateExpression(expression string, inputData map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)

	// 处理各种n8n表达式模式
	switch {
	case strings.HasPrefix(expression, "$") && strings.Contains(expression, "(") && strings.Contains(expression, ")"):
		// 处理函数调用，如 $now.format(), $json.length() 等
		return e.evaluateFunctionCall(expression, inputData)

	case strings.HasPrefix(expression, "$json."):
		// 处理 $json.field 格式 - 引用当前节点的JSON数据
		return e.extractJsonValue(expression, inputData)

	case strings.HasPrefix(expression, "$('") && strings.Contains(expression, "')"):
		// 处理节点引用格式 - $('NodeName').item.json.field 或 $('NodeName').item.field
		return e.resolveNodeReference(expression)

	case strings.HasPrefix(expression, "$binary."):
		// 处理二进制数据 $binary.data
		return e.extractBinaryData(expression, inputData)

	case strings.HasPrefix(expression, "$execution."):
		// 处理执行上下文 $execution.id, $execution.mode 等
		return e.extractExecutionContext(expression)

	case strings.HasPrefix(expression, "$workflow."):
		// 处理工作流信息 $workflow.id, $workflow.name 等
		return e.extractWorkflowInfo(expression)

	case expression == "$now":
		// 处理当前时间
		return time.Now(), nil

	case expression == "$today":
		// 处理当前日期
		return time.Now().Format("2006-01-02"), nil

	case expression == "$timestamp":
		// 处理当前时间戳
		return time.Now().Unix(), nil

	case strings.Contains(expression, "+") || strings.Contains(expression, "-") ||
		strings.Contains(expression, "*") || strings.Contains(expression, "/"):
		// 处理数学表达式
		return e.evaluateMathExpression(expression, inputData)

	case strings.HasPrefix(expression, "="):
		// 处理直接值（等号开头但不是表达式）
		directValue := strings.TrimPrefix(expression, "=")
		return strings.TrimSpace(directValue), nil

	case strings.HasPrefix(expression, "\"") && strings.HasSuffix(expression, "\""):
		// 处理字符串字面量
		return strings.TrimPrefix(strings.TrimSuffix(expression, "\""), "\""), nil

	case strings.HasPrefix(expression, "'") && strings.HasSuffix(expression, "'"):
		// 处理单引号字符串字面量
		return strings.TrimPrefix(strings.TrimSuffix(expression, "'"), "'"), nil

	default:
		// 尝试作为简单字段路径处理
		if value, exists := inputData[expression]; exists {
			return value, nil
		}
		// 尝试解析为数字
		if num, err := strconv.ParseFloat(expression, 64); err == nil {
			return num, nil
		}
		// 尝试解析为布尔值
		if strings.ToLower(expression) == "true" {
			return true, nil
		}
		if strings.ToLower(expression) == "false" {
			return false, nil
		}
		// 返回原始字符串作为fallback
		return expression, nil
	}
}

// extractJsonValue 提取JSON字段值
func (e *ExpressionEvaluator) extractJsonValue(expression string, inputData map[string]interface{}) (interface{}, error) {
	fieldPath := strings.TrimPrefix(expression, "$json.")

	// 如果fieldPath为空，返回整个inputData作为json对象
	if fieldPath == "" {
		return inputData, nil
	}

	return e.extractFieldValue(fieldPath, inputData)
}

// resolveNodeReference 解析节点引用
func (e *ExpressionEvaluator) resolveNodeReference(expression string) (interface{}, error) {
	// 使用正则表达式解析节点引用
	// 匹配模式: $('NodeName').item.json.field 或 $('NodeName').item.field
	// 支持带引号的字段名，如 $('NodeName').item.json["@type"]
	re := regexp.MustCompile(`\$\('([^']+)'\)\.item\.(json)?\.?(\w+|\["[^"]+"\])`)
	matches := re.FindStringSubmatch(expression)

	if len(matches) < 4 {
		return nil, fmt.Errorf("无效的节点引用表达式: %s", expression)
	}

	nodeName := matches[1]
	hasJsonPrefix := matches[2] != "" // 是否有 json 前缀
	fieldPath := matches[3]

	// 处理带引号的字段名，如 ["@type"] -> @type
	if strings.HasPrefix(fieldPath, "[\"") && strings.HasSuffix(fieldPath, "\"]") {
		fieldPath = strings.TrimPrefix(fieldPath, "[\"")
		fieldPath = strings.TrimSuffix(fieldPath, "\"]")
	}

	// 从工作流上下文中获取节点数据
	nodeData, exists := e.workflowContext.GetNodeData(nodeName)
	if !exists {
		// 如果找不到节点数据，返回表达式本身作为fallback
		return fmt.Sprintf("{{ %s }}", expression), nil
	}

	// 构建完整的字段路径
	var fullPath string
	if hasJsonPrefix {
		fullPath = fmt.Sprintf("json.%s", fieldPath)
	} else {
		fullPath = fieldPath
	}

	// 提取字段值
	return e.extractFieldValue(fullPath, nodeData)
}

// extractBinaryData 提取二进制数据
func (e *ExpressionEvaluator) extractBinaryData(expression string, inputData map[string]interface{}) (interface{}, error) {
	fieldPath := strings.TrimPrefix(expression, "$binary.")
	return e.extractFieldValue(fieldPath, inputData)
}

// extractExecutionContext 提取执行上下文信息
func (e *ExpressionEvaluator) extractExecutionContext(expression string) (interface{}, error) {
	// 简化实现，返回一些基本的执行信息
	switch expression {
	case "$execution.id":
		return "exec_" + fmt.Sprintf("%d", time.Now().Unix()), nil
	case "$execution.mode":
		return "manual", nil
	case "$execution.retryCount":
		return 0, nil
	default:
		return fmt.Sprintf("{{ %s }}", expression), nil
	}
}

// extractWorkflowInfo 提取工作流信息
func (e *ExpressionEvaluator) extractWorkflowInfo(expression string) (interface{}, error) {
	// 简化实现，返回一些基本的工作流信息
	switch expression {
	case "$workflow.id":
		return "workflow_" + fmt.Sprintf("%d", time.Now().Unix()), nil
	case "$workflow.name":
		return "n8n-workflow", nil
	default:
		return fmt.Sprintf("{{ %s }}", expression), nil
	}
}

// evaluateFunctionCall 评估函数调用
func (e *ExpressionEvaluator) evaluateFunctionCall(expression string, inputData map[string]interface{}) (interface{}, error) {
	// 简化的函数调用实现
	// 解析函数名和参数，支持无参数调用
	re := regexp.MustCompile(`^\$(\w+)\.(\w+)\((.*)\)$`)
	matches := re.FindStringSubmatch(expression)

	if len(matches) < 4 {
		return fmt.Sprintf("{{ %s }}", expression), nil
	}

	objectName := matches[1]
	functionName := matches[2]
	paramsStr := strings.TrimSpace(matches[2])

	var params []string
	if paramsStr != "" {
		params = strings.Split(paramsStr, ",")
		// 清理参数
		for i, param := range params {
			params[i] = strings.TrimSpace(param)
		}
	} else {
		params = []string{} // 无参数
	}

	switch objectName {
	case "now":
		return e.evaluateTimeFunction(functionName, params)
	case "json":
		return e.evaluateJsonFunction(functionName, params, inputData)
	default:
		return fmt.Sprintf("{{ %s }}", expression), nil
	}
}

// evaluateTimeFunction 评估时间相关函数
func (e *ExpressionEvaluator) evaluateTimeFunction(functionName string, params []string) (interface{}, error) {
	now := time.Now()

	switch functionName {
	case "format":
		if len(params) > 0 {
			return now.Format(params[0]), nil
		}
		return now.Format("2006-01-02T15:04:05Z07:00"), nil
	case "year":
		return now.Year(), nil
	case "month":
		return int(now.Month()), nil
	case "day":
		return now.Day(), nil
	default:
		return fmt.Sprintf("{{ $now.%s() }}", functionName), nil
	}
}

// evaluateJsonFunction 评估JSON相关函数
func (e *ExpressionEvaluator) evaluateJsonFunction(functionName string, params []string, inputData map[string]interface{}) (interface{}, error) {
	switch functionName {
	case "length":
		// 计算$json的长度 - $json指的是整个inputData对象
		return len(inputData), nil
	case "keys":
		// 返回$json的键列表
		keys := make([]string, 0, len(inputData))
		for key := range inputData {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return keys, nil
	default:
		return fmt.Sprintf("{{ $json.%s() }}", functionName), nil
	}
}

// evaluateMathExpression 评估数学表达式
func (e *ExpressionEvaluator) evaluateMathExpression(expression string, inputData map[string]interface{}) (interface{}, error) {
	// 这是一个简化的数学表达式评估器
	// 首先尝试解析表达式中的变量
	re := regexp.MustCompile(`\$json\.([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`)
	evaluatedExpr := re.ReplaceAllStringFunc(expression, func(match string) string {
		fieldPath := strings.TrimPrefix(match, "$json.")
		if value, err := e.extractFieldValue(fieldPath, inputData); err == nil {
			return fmt.Sprintf("%v", value)
		}
		return "0"
	})

	// 简单的数学运算解析（仅支持基本运算）
	// 如果无法解析，返回原始表达式
	return evaluatedExpr, nil
}

// extractFieldValue 从输入数据中提取字段值
func (e *ExpressionEvaluator) extractFieldValue(fieldPath string, inputData map[string]interface{}) (interface{}, error) {
	// 处理空字段路径
	if fieldPath == "" {
		return inputData, nil
	}

	// 简单的字段路径解析，支持 "json.field1.field2" 和 "json.array[0]" 格式
	parts := strings.Split(fieldPath, ".")
	current := inputData

	for i, part := range parts {
		// 跳过空的路径部分
		if part == "" {
			continue
		}

		// 如果是json前缀，需要进入json对象而不是跳过
		if i == 0 && part == "json" {
			if jsonValue, exists := current["json"]; exists {
				if jsonMap, ok := jsonValue.(map[string]interface{}); ok {
					current = jsonMap
					continue
				} else {
					return nil, fmt.Errorf("json字段不是对象类型")
				}
			} else {
				return nil, fmt.Errorf("字段路径 '%s' 中缺少 'json' 字段", fieldPath)
			}
		}

		// 检查是否包含数组索引 [数字]
		if strings.Contains(part, "[") && strings.Contains(part, "]") {
			// 分离字段名和索引部分
			fieldName := part[:strings.Index(part, "[")]
			indexStr := part[strings.Index(part, "[")+1 : strings.LastIndex(part, "]")]

			// 获取字段值
			if value, exists := current[fieldName]; exists {
				if array, ok := value.([]interface{}); ok {
					// 解析索引
					if index, err := strconv.Atoi(indexStr); err == nil {
						if index >= 0 && index < len(array) {
							if i == len(parts)-1 {
								return array[index], nil
							}
							// 如果还有后续路径，检查当前元素是否是map
							if nextMap, ok := array[index].(map[string]interface{}); ok {
								current = nextMap
								continue
							} else {
								return nil, fmt.Errorf("数组索引 '%s' 的元素不是对象", fieldPath)
							}
						} else {
							return nil, fmt.Errorf("数组索引 '%d' 超出范围 [0, %d]", index, len(array))
						}
					} else {
						return nil, fmt.Errorf("无效的数组索引 '%s'", indexStr)
					}
				} else {
					return nil, fmt.Errorf("字段 '%s' 不是数组类型", fieldName)
				}
			} else {
				return nil, fmt.Errorf("字段路径 '%s' 中缺少 '%s'", fieldPath, fieldName)
			}
		}

		// 普通字段访问
		if value, exists := current[part]; exists {
			if i == len(parts)-1 {
				return value, nil
			}

			if nextMap, ok := value.(map[string]interface{}); ok {
				current = nextMap
			} else {
				return nil, fmt.Errorf("字段路径 '%s' 在 '%s' 处不是对象", fieldPath, part)
			}
		} else {
			return nil, fmt.Errorf("字段路径 '%s' 中缺少 '%s'", fieldPath, part)
		}
	}
	return nil, fmt.Errorf("字段路径 '%s' 无效", fieldPath)
}
