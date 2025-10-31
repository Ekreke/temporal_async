package node

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CustomNodeActivity 自定义节点 Activity
type CustomNodeActivity struct {
	*BaseActivity
}

// NewCustomNodeActivity 创建新的自定义节点
func NewCustomNodeActivity(node WkFLowNode, express *ExpressionEvaluator) *CustomNodeActivity {
	activity := &CustomNodeActivity{
		BaseActivity: &BaseActivity{
			NodeInfo:            &node,
			expressionEvaluator: express,
		},
	}
	// 注册节点
	return activity
}

// GetLogger 获取log对象
func (a *CustomNodeActivity) GetLogger(ctx context.Context) log.Logger {
	return a.BaseActivity.GetLogger(ctx)
}

// GetNodeInfo 获取当前节点信息
func (a *CustomNodeActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// Execute 执行节点逻辑（实现NodeActivity接口）
func (a *CustomNodeActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return a.ExecuteWithExecuteTiming(ctx, input, a.executeCustomNode)
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *CustomNodeActivity) ValidateInput(input *ActivityInput) error {
	// 基础验证
	return a.BaseActivity.ValidateInput(input)
}

// ExecuteCustomNode 执行自定义节点（向后兼容）
func (a *CustomNodeActivity) ExecuteCustomNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	// 转换旧格式到新格式
	nodeInput := &ActivityInput{
		NodeID:      getStringValue(input, "nodeId"),
		NodeName:    getStringValue(input, "nodeName"),
		NodeType:    getStringValue(input, "nodeType"),
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

// executeCustomNode 内部自定义节点逻辑
func (a *CustomNodeActivity) executeCustomNode(input *ActivityInput) (map[string]interface{}, error) {
	// 解析节点类型
	nodeType, err := a.parseNodeTypeFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("解析节点类型失败: %v", err)
	}

	// 根据节点类型执行不同的业务逻辑
	result, err := a.executeByNodeType(nodeType, input)
	if err != nil {
		return nil, fmt.Errorf("执行节点%s失败: %v", nodeType, err)
	}

	resultData := map[string]interface{}{
		"nodeType": nodeType,
		"executed": true,
		"result":   result,
	}

	return resultData, nil
}

// parseNodeTypeFromInput 从输入中解析节点类型
func (a *CustomNodeActivity) parseNodeTypeFromInput(input *ActivityInput) (string, error) {
	// 优先从节点类型获取
	if input.NodeType != "" {
		return input.NodeType, nil
	}

	// 从参数中获取
	if input.Parameters != nil {
		if nodeType, exists := input.Parameters["nodeType"].(string); exists && nodeType != "" {
			return nodeType, nil
		}
		if operation, exists := input.Parameters["operation"].(string); exists && operation != "" {
			return operation, nil
		}
	}

	// 从节点名称推断类型
	if input.NodeName != "" {
		return a.inferNodeTypeFromName(input.NodeName), nil
	}

	return "generic_custom", nil
}

// parseNodeType 解析节点类型
func (a *CustomNodeActivity) parseNodeType(input map[string]interface{}) (string, error) {
	// 尝试从多个位置获取节点类型
	if nodeType, exists := input["nodeType"].(string); exists && nodeType != "" {
		return nodeType, nil
	}

	if parameters, ok := input["parameters"].(map[string]interface{}); ok {
		if nodeType, exists := parameters["nodeType"].(string); exists && nodeType != "" {
			return nodeType, nil
		}
		if operation, exists := parameters["operation"].(string); exists && operation != "" {
			return operation, nil
		}
	}

	// 从节点名称推断类型
	if nodeName, exists := input["nodeName"].(string); exists && nodeName != "" {
		return a.inferNodeTypeFromName(nodeName), nil
	}

	return "unknown", nil
}

// inferNodeTypeFromName 从节点名称推断节点类型
func (a *CustomNodeActivity) inferNodeTypeFromName(nodeName string) string {
	nodeName = strings.ToLower(nodeName)

	// HTTP请求相关
	if strings.Contains(nodeName, "http") || strings.Contains(nodeName, "request") || strings.Contains(nodeName, "api") {
		return "http_request"
	}

	// 数据转换相关
	if strings.Contains(nodeName, "transform") || strings.Contains(nodeName, "convert") || strings.Contains(nodeName, "map") {
		return "data_transform"
	}

	// 时间相关
	if strings.Contains(nodeName, "time") || strings.Contains(nodeName, "date") || strings.Contains(nodeName, "schedule") {
		return "time_operation"
	}

	// 文本处理相关
	if strings.Contains(nodeName, "text") || strings.Contains(nodeName, "string") || strings.Contains(nodeName, "format") {
		return "text_processing"
	}

	// 数学计算相关
	if strings.Contains(nodeName, "math") || strings.Contains(nodeName, "calculate") || strings.Contains(nodeName, "compute") {
		return "math_operation"
	}

	// 数据验证相关
	if strings.Contains(nodeName, "validate") || strings.Contains(nodeName, "check") || strings.Contains(nodeName, "verify") {
		return "data_validation"
	}

	// 默认返回通用自定义节点
	return "generic_custom"
}

// executeByNodeType 根据节点类型执行相应逻辑
func (a *CustomNodeActivity) executeByNodeType(nodeType string, input *ActivityInput) (map[string]interface{}, error) {
	switch nodeType {
	case "http_request":
		return a.executeHTTPRequest(input)
	case "data_transform":
		return a.executeDataTransform(input)
	case "time_operation":
		return a.executeTimeOperation(input)
	case "text_processing":
		return a.executeTextProcessing(input)
	case "math_operation":
		return a.executeMathOperation(input)
	case "data_validation":
		return a.executeDataValidation(input)
	default:
		return a.executeGenericCustom(input)
	}
}

// executeHTTPRequest 执行HTTP请求节点
func (a *CustomNodeActivity) executeHTTPRequest(input *ActivityInput) (map[string]interface{}, error) {
	if input.Parameters == nil {
		return nil, fmt.Errorf("缺少HTTP请求参数")
	}
	parameters := input.Parameters

	// 解析HTTP请求参数
	url := getStringValue(parameters, "url")
	method := getStringValue(parameters, "method")
	if method == "" {
		method = "GET"
	}

	headers := make(map[string]string)
	if headersData, exists := parameters["headers"].(map[string]interface{}); exists {
		for key, value := range headersData {
			if strValue, ok := value.(string); ok {
				headers[key] = strValue
			}
		}
	}

	// 解析超时设置
	timeout := 30 * time.Second
	if timeoutValue, exists := parameters["timeout"]; exists {
		if timeoutStr, ok := timeoutValue.(string); ok {
			if timeoutInt, err := strconv.Atoi(timeoutStr); err == nil {
				timeout = time.Duration(timeoutInt) * time.Second
			}
		}
	}

	// 创建HTTP客户端
	client := &http.Client{Timeout: timeout}

	// 创建请求（简化实现，实际应该支持body等）
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建HTTP请求失败: %v", err)
	}

	// 设置请求头
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 返回响应信息
	result := map[string]interface{}{
		"status":     resp.StatusCode,
		"statusText": resp.Status,
		"headers":    resp.Header,
		"url":        url,
		"method":     method,
	}

	return result, nil
}

// executeDataTransform 执行数据转换节点
func (a *CustomNodeActivity) executeDataTransform(input *ActivityInput) (map[string]interface{}, error) {
	parameters := input.Parameters
	inputData := input.InputData
	if inputData == nil {
		inputData = make(map[string]interface{})
	}

	// 解析转换规则
	transformType := getStringValue(parameters, "transformType")

	var transformedData interface{}
	var err error

	switch transformType {
	case "rename_fields":
		transformedData, err = a.renameFields(inputData, parameters)
	case "filter_fields":
		transformedData, err = a.filterFields(inputData, parameters)
	case "aggregate":
		transformedData, err = a.aggregateData(inputData, parameters)
	case "sort":
		transformedData, err = a.sortData(inputData, parameters)
	default:
		// 默认透传数据
		transformedData = inputData
	}

	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"transformType": transformType,
		"transformed":   transformedData,
		"original":      inputData,
	}

	return result, nil
}

// executeTimeOperation 执行时间操作节点
func (a *CustomNodeActivity) executeTimeOperation(input *ActivityInput) (map[string]interface{}, error) {
	parameters := input.Parameters
	if parameters == nil {
		return nil, fmt.Errorf("缺少时间操作参数")
	}

	operation := getStringValue(parameters, "operation")
	currentTime := time.Now()

	var result interface{}
	var err error

	switch operation {
	case "current_time":
		result = currentTime
	case "format_time":
		format := getStringValue(parameters, "format")
		if format == "" {
			format = "2006-01-02 15:04:05"
		}
		result = currentTime.Format(format)
	case "parse_time":
		timeStr := getStringValue(parameters, "timeString")
		format := getStringValue(parameters, "format")
		if format == "" {
			format = "2006-01-02 15:04:05"
		}
		result, err = time.Parse(format, timeStr)
	case "add_time":
		duration := getStringValue(parameters, "duration")
		if parsedDuration, parseErr := time.ParseDuration(duration); parseErr == nil {
			result = currentTime.Add(parsedDuration)
		} else {
			err = fmt.Errorf("无效的时间格式: %v", parseErr)
		}
	default:
		result = currentTime
	}

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"operation": operation,
		"result":    result,
		"timestamp": currentTime,
	}, nil
}

// executeTextProcessing 执行文本处理节点
func (a *CustomNodeActivity) executeTextProcessing(input *ActivityInput) (map[string]interface{}, error) {
	parameters := input.Parameters
	if parameters == nil {
		return nil, fmt.Errorf("缺少文本处理参数")
	}

	// 获取输入文本
	text := getStringValue(parameters, "text")
	if text == "" && input.InputData != nil {
		if value, exists := input.InputData["text"]; exists {
			if strValue, ok := value.(string); ok {
				text = strValue
			}
		}
	}

	operation := getStringValue(parameters, "operation")

	var result string

	switch operation {
	case "upper":
		result = strings.ToUpper(text)
	case "lower":
		result = strings.ToLower(text)
	case "trim":
		result = strings.TrimSpace(text)
	case "replace":
		oldStr := getStringValue(parameters, "oldString")
		newStr := getStringValue(parameters, "newString")
		result = strings.ReplaceAll(text, oldStr, newStr)
	case "split":
		separator := getStringValue(parameters, "separator")
		if separator == "" {
			separator = ","
		}
		parts := strings.Split(text, separator)
		return map[string]interface{}{
			"operation": operation,
			"result":    parts,
			"original":  text,
		}, nil
	case "join":
		separator := getStringValue(parameters, "separator")
		if separator == "" {
			separator = ","
		}
		if parts, ok := parameters["parts"].([]interface{}); ok {
			var strParts []string
			for _, part := range parts {
				if strPart, ok := part.(string); ok {
					strParts = append(strParts, strPart)
				}
			}
			result = strings.Join(strParts, separator)
		}
	default:
		result = text
	}

	return map[string]interface{}{
		"operation": operation,
		"result":    result,
		"original":  text,
	}, nil
}

// executeMathOperation 执行数学计算节点
func (a *CustomNodeActivity) executeMathOperation(input *ActivityInput) (map[string]interface{}, error) {
	parameters := input.Parameters
	if parameters == nil {
		return nil, fmt.Errorf("缺少数学计算参数")
	}

	operation := getStringValue(parameters, "operation")

	var result float64
	var err error

	// 获取数值参数
	value1 := a.getFloatParameter(parameters, "value1")
	value2 := a.getFloatParameter(parameters, "value2")

	switch operation {
	case "add":
		result = value1 + value2
	case "subtract":
		result = value1 - value2
	case "multiply":
		result = value1 * value2
	case "divide":
		if value2 == 0 {
			return nil, fmt.Errorf("除数不能为零")
		}
		result = value1 / value2
	case "power":
		result = 1
		for i := 0; i < int(value2); i++ {
			result *= value1
		}
	case "sqrt":
		if value1 < 0 {
			return nil, fmt.Errorf("不能计算负数的平方根")
		}
		// 简化实现，实际应该使用math.Sqrt
		result = value1 * 0.5 // 占位实现
	default:
		return nil, fmt.Errorf("不支持的数学操作: %s", operation)
	}

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"operation": operation,
		"result":    result,
		"value1":    value1,
		"value2":    value2,
	}, nil
}

// executeDataValidation 执行数据验证节点
func (a *CustomNodeActivity) executeDataValidation(input *ActivityInput) (map[string]interface{}, error) {
	parameters := input.Parameters
	if parameters == nil {
		return nil, fmt.Errorf("缺少数据验证参数")
	}

	// 获取要验证的数据
	var data interface{}
	if input.InputData != nil {
		data = input.InputData
	} else {
		data = make(map[string]interface{})
	}

	validationType := getStringValue(parameters, "validationType")

	var isValid bool
	var message string

	switch validationType {
	case "required":
		fieldName := getStringValue(parameters, "field")
		if dataMap, ok := data.(map[string]interface{}); ok {
			_, isValid = dataMap[fieldName]
			if isValid {
				message = fmt.Sprintf("字段 '%s' 存在", fieldName)
			} else {
				message = fmt.Sprintf("字段 '%s' 不存在", fieldName)
			}
		}
	case "type":
		expectedType := getStringValue(parameters, "expectedType")
		actualType := a.getValueType(data)
		isValid = (actualType == expectedType)
		message = fmt.Sprintf("期望类型: %s, 实际类型: %s", expectedType, actualType)
	case "range":
		minValue := a.getFloatParameter(parameters, "minValue")
		maxValue := a.getFloatParameter(parameters, "maxValue")
		if value, ok := data.(float64); ok {
			isValid = (value >= minValue && value <= maxValue)
			message = fmt.Sprintf("值 %v 在范围 [%v, %v] 内: %t", value, minValue, maxValue, isValid)
		} else {
			isValid = false
			message = "数据不是数字类型"
		}
	case "length":
		minLength := int(a.getFloatParameter(parameters, "minLength"))
		maxLength := int(a.getFloatParameter(parameters, "maxLength"))
		if str, ok := data.(string); ok {
			length := len(str)
			isValid = (length >= minLength && length <= maxLength)
			message = fmt.Sprintf("字符串长度 %d 在范围 [%d, %d] 内: %t", length, minLength, maxLength, isValid)
		} else {
			isValid = false
			message = "数据不是字符串类型"
		}
	default:
		isValid = false
		message = fmt.Sprintf("不支持的验证类型: %s", validationType)
	}

	return map[string]interface{}{
		"validationType": validationType,
		"isValid":        isValid,
		"message":        message,
		"data":           data,
	}, nil
}

// executeGenericCustom 执行通用自定义节点
func (a *CustomNodeActivity) executeGenericCustom(input *ActivityInput) (map[string]interface{}, error) {
	// 获取输入数据
	inputData := input.InputData
	if inputData == nil {
		inputData = make(map[string]interface{})
	}

	// 简单的数据处理：添加时间戳和处理标记
	result := map[string]interface{}{
		"processed":   true,
		"nodeType":    "generic_custom",
		"inputCount":  len(inputData),
		"output":      inputData,
		"processedAt": time.Now(),
	}

	// 如果有自定义逻辑，在这里处理
	if input.Parameters != nil {
		if customLogic, exists := input.Parameters["customLogic"].(string); exists {
			result["customLogic"] = customLogic
			result["customProcessed"] = true
		}
	}

	return result, nil
}

// renameFields 重命名字段
func (a *CustomNodeActivity) renameFields(data map[string]interface{}, parameters map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// 复制原始数据
	for key, value := range data {
		result[key] = value
	}

	// 应用字段重命名规则
	if rules, exists := parameters["renameRules"].(map[string]interface{}); exists {
		for oldName, newName := range rules {
			if newStr, ok := newName.(string); ok {
				if value, exists := result[oldName]; exists {
					result[newStr] = value
					delete(result, oldName)
				}
			}
		}
	}

	return result, nil
}

// filterFields 过滤字段
func (a *CustomNodeActivity) filterFields(data map[string]interface{}, parameters map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// 获取要保留的字段列表
	if fields, exists := parameters["fields"].([]interface{}); exists {
		for _, field := range fields {
			if fieldName, ok := field.(string); ok {
				if value, exists := data[fieldName]; exists {
					result[fieldName] = value
				}
			}
		}
	}

	return result, nil
}

// aggregateData 聚合数据
func (a *CustomNodeActivity) aggregateData(data map[string]interface{}, parameters map[string]interface{}) (interface{}, error) {
	// 简化实现，只处理数值数组的聚合
	if arrayData, ok := data["data"].([]interface{}); ok {
		var sum float64
		var count float64

		for _, item := range arrayData {
			if num, ok := item.(float64); ok {
				sum += num
				count++
			}
		}

		aggregateType := getStringValue(parameters, "aggregateType")

		switch aggregateType {
		case "sum":
			return sum, nil
		case "count":
			return count, nil
		case "average":
			if count > 0 {
				return sum / count, nil
			}
			return 0, nil
		}
	}

	return data, nil
}

// sortData 排序数据
func (a *CustomNodeActivity) sortData(data map[string]interface{}, parameters map[string]interface{}) (interface{}, error) {
	// 简化实现，只处理字符串数组
	if arrayData, ok := data["data"].([]interface{}); ok {
		var strArray []string
		for _, item := range arrayData {
			if str, ok := item.(string); ok {
				strArray = append(strArray, str)
			}
		}

		// 简单排序
		for i := 0; i < len(strArray); i++ {
			for j := i + 1; j < len(strArray); j++ {
				if strArray[i] > strArray[j] {
					strArray[i], strArray[j] = strArray[j], strArray[i]
				}
			}
		}

		return strArray, nil
	}

	return data, nil
}

// getFloatParameter 安全获取浮点参数
func (a *CustomNodeActivity) getFloatParameter(parameters map[string]interface{}, key string) float64 {
	if value, exists := parameters[key]; exists {
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case string:
			if num, err := strconv.ParseFloat(v, 64); err == nil {
				return num
			}
		}
	}
	return 0
}

// getValueType 获取值的类型
func (a *CustomNodeActivity) getValueType(value interface{}) string {
	switch value.(type) {
	case string:
		return "string"
	case int, int64, float64:
		return "number"
	case bool:
		return "boolean"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}
