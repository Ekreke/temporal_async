package node

import (
	"context"
	"errors"
	"fmt"
	"n8n2temporal/notify"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	activitySdk "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
)

// Activity 统一的节点接口
type Activity interface {
	// GetNodeInfo 获取节点基本信息
	GetNodeInfo() *WkFLowNode
	// Execute 执行节点逻辑
	Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error)
	// ValidateInput 验证输入参数
	ValidateInput(input *ActivityInput) error
	// GetLogger 获取logger
	GetLogger(ctx context.Context) log.Logger
}

// WkFLowNode 节点定义结构
type WkFLowNode struct {
	ID               string                 `json:"id"`                // 节点ID，同一个工作流中每个节点ID都是唯一的
	Name             string                 `json:"name"`              // 节点名称，这是展示在工作流上的名称，和ID一样也是唯一
	Type             string                 `json:"type"`              // 节点类型
	Parameters       map[string]interface{} `json:"parameters"`        // 节点参数
	ParametersSource string                 `json:"parameters_source"` // 节点参数来源,当is_remote为false，该值无意义。枚举值：$var. 变量节点；$global. 全局参数；某个具体的node名称（获取指定节点）
	Version          float64                `json:"version"`           // 节点的版本，每次更新节点的时候，都需要增加版本号
	IsRemote         bool                   `json:"is_remote"`         // 远端执行标识，false表示本地执行节点（python、condition等），否则表示远端执行（调度）
}

func (wn *WkFLowNode) Check() error {
	if wn.ID == "" {
		return fmt.Errorf("node ID is empty")
	}
	if wn.Name == "" {
		return fmt.Errorf("node name is empty")
	}
	if wn.Type == "" {
		return fmt.Errorf("node type is empty")
	}
	if wn.Parameters == nil {
		return fmt.Errorf("node parameters is empty")
	}
	if wn.Version == 0 {
		return fmt.Errorf("node type version is zero")
	}
	return nil
}

// ActivityInput 节点输入数据节点ID
type ActivityInput struct {
	ExecID      string                   `json:"activity_id"` // 节点执行ID
	NodeID      string                   `json:"nodeId"`      // 节点类型
	NodeName    string                   `json:"nodeName"`    // 节点名称
	NodeType    string                   `json:"nodeType"`    // 节点类型
	InputData   []map[string]interface{} `json:"inputData"`   // 节点需要直接执行的数据
	Parameters  map[string]interface{}   `json:"parameters"`  // 节点参数
	SignalInput *notify.SignalData       `json:"signalInput"` // 上个信息，唤醒信号
	WorkflowID  string                   `json:"workflowId"`  // 工作流ID
	ExecutionID string                   `json:"executionId"` // 执行ID
	StreamRsp   bool                     `json:"stream_rsp"`  // 是否流式响应
}

// ActivityOutput 节点输出数据
type ActivityOutput struct {
	NodeID      string                   `json:"nodeId"`      // 节点ID
	NodeName    string                   `json:"nodeName"`    // 节点名称
	NodeType    string                   `json:"nodeType"`    // 节点类型
	Success     bool                     `json:"success"`     // 执行是否成功
	Data        []map[string]interface{} `json:"data"`        // 输出数据
	Error       string                   `json:"error"`       // 错误信息
	ProcessedAt time.Time                `json:"processedAt"` // 处理时间
	Metadata    map[string]interface{}   `json:"metadata"`    // 元数据
	//AddTaskNum    int                      `json:"addTaskNum"`    // 新增任务数
	//FinishTaskNum int                      `json:"finishTaskNum"` // 已完成任务数
}

// BaseActivity Activity基类，提供通用功能
type BaseActivity struct {
	NodeInfo            *WkFLowNode
	expressionEvaluator *ExpressionEvaluator // 表达式评估器
}

// GetNodeInfo 获取节点信息（BaseActivity实现）
func (a *BaseActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
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

// GetLogger 获取activity logger
func (a *BaseActivity) GetLogger(ctx context.Context) log.Logger {
	return activitySdk.GetLogger(ctx)
}

// GetExpressionEvaluator 获取表达式评估器
func (a *BaseActivity) GetExpressionEvaluator() *ExpressionEvaluator {
	return a.expressionEvaluator
}

// AddNodeDataToContext 将节点执行结果添加到工作流上下文
func (a *BaseActivity) AddNodeDataToContext(nodeName string, data map[string]interface{}) {
	if a.expressionEvaluator != nil && a.expressionEvaluator.WorkflowContext != nil {
		a.expressionEvaluator.WorkflowContext.SetNodeData(nodeName, data)
	}
}

// GetGlobalParametersForNode 为指定节点获取全局参数
func (a *BaseActivity) GetGlobalParametersForNode(nodeName string) map[string]interface{} {
	if a.expressionEvaluator == nil || a.expressionEvaluator.WorkflowContext == nil {
		return nil
	}

	globalData, exists := a.expressionEvaluator.WorkflowContext.GetNodeData(ExpressGlobalNodeName)
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
	if a.expressionEvaluator == nil || a.expressionEvaluator.WorkflowContext == nil {
		return nil
	}

	varData, exists := a.expressionEvaluator.WorkflowContext.GetNodeData(ExpressVariablesNodeName)
	if !exists {
		return nil
	}

	return varData
}

// CreateSuccessOutput 创建成功输出
func (a *BaseActivity) CreateSuccessOutput(input *ActivityInput, execRes *ExecNodeFuncResult) *ActivityOutput {
	if execRes == nil {
		execRes = &ExecNodeFuncResult{
			Data: make([]map[string]interface{}, 0),
		}
	}
	return &ActivityOutput{
		NodeID:      input.NodeID,
		NodeName:    input.NodeName,
		NodeType:    input.NodeType,
		Success:     true,
		Data:        execRes.Data,
		ProcessedAt: time.Now(),
		Metadata: map[string]interface{}{
			"executionTime": time.Now().Unix(),
			"nodeVersion":   a.NodeInfo.Version,
		},
		//AddTaskNum:    execRes.AddTaskNum,
		//FinishTaskNum: execRes.FinishTaskNum,
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

// ExecNodeFuncResult 节点执行结果
type ExecNodeFuncResult struct {
	Data []map[string]interface{} `json:"data"` // 节点执行结果
	//AddTaskNum    int                      `json:"addTaskNum"`    // 结果添加的任务总数
	//FinishTaskNum int                      `json:"finishTaskNum"` // 完成的任务数
}

// 执行逻辑
type nodeExecFunc func(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error)

// ExecuteWithExecuteTiming 带时间监控的节点执行
func (a *BaseActivity) ExecuteWithExecuteTiming(ctx context.Context, input *ActivityInput, executeFunc nodeExecFunc) (*ActivityOutput, error) {
	logger := a.GetLogger(ctx)
	startTime := time.Now()
	logger.Info("开始执行节点", "nodeType", input.NodeType, "nodeId", input.NodeID, "nodeName", input.NodeName)
	// 执行具体逻辑
	data, err := executeFunc(ctx, input)
	duration := time.Since(startTime)
	if err != nil {
		logger.Error("节点执行失败", "nodeType", input.NodeType, "nodeId", input.NodeID, "error", err, "duration", duration.String())
		return a.CreateErrorOutput(input, err), nil
	}
	// 创建输出
	output := a.CreateSuccessOutput(input, data)
	output.Metadata["executionDuration"] = duration.String()
	logger.Info("节点执行成功", "nodeType", input.NodeType, "nodeId", input.NodeID, "duration", duration.String())
	return output, nil
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
	Context map[string]interface{} // 存储每个节点的最新执行结果
}

// NewWorkflowContext 创建新的工作流上下文
func NewWorkflowContext() *WorkflowContext {
	return &WorkflowContext{
		Context: make(map[string]interface{}),
	}
}

// SetNodeData 设置节点的最新数据（覆盖之前的）
func (wc *WorkflowContext) SetNodeData(nodeName string, data map[string]interface{}) {
	wc.Context[nodeName] = data
}

// GetNodeData 获取节点的最新数据
func (wc *WorkflowContext) GetNodeData(nodeName string) (map[string]interface{}, bool) {
	data, exists := wc.Context[nodeName]
	if !exists {
		return nil, false
	}
	if nodeMap, ok := data.(map[string]interface{}); ok {
		return nodeMap, true
	}
	return nil, false
}

// SetNodeDataKV 设置节点数据，KV数据 -- key支持以 . 分割，递归设置数据
func (wc *WorkflowContext) SetNodeDataKV(nodeName string, key string, value interface{}) error {
	// 获取或创建节点数据
	nodeData, ok := wc.Context[nodeName]
	if !ok {
		wc.Context[nodeName] = make(map[string]interface{})
		nodeData = wc.Context[nodeName]
	}
	data, ok := nodeData.(map[string]interface{})
	if !ok {
		data = make(map[string]interface{})
		wc.Context[nodeName] = data
	}
	// 处理空键的情况
	key = strings.TrimSpace(key)
	if key == "" {
		return errors.New("设置变量-'键'不能为空")
	}
	// 开始递归设置key的值
	keyArr := strings.Split(strings.TrimSpace(key), ".")
	curData := data
	for ki, kArr := range keyArr {
		ak := strings.TrimSpace(kArr)
		if ak == "" {
			return errors.New("设置变量-'键'不能出现连续分割符")
		}
		// 跳过最后一层(赋值)的逻辑
		if ki == len(keyArr)-1 {
			break
		}
		curDataV, exists := curData[ak]
		if !exists {
			// 创建新的嵌套map
			curData[ak] = make(map[string]interface{})
			curData = curData[ak].(map[string]interface{})
		} else if nextData, ok := curDataV.(map[string]interface{}); ok {
			// 继续使用已存在的map
			curData = nextData
		} else {
			return errors.New("设置变量-'值'类型错误1")
		}
	}
	// 设置最终的值
	finalKey := strings.TrimSpace(keyArr[len(keyArr)-1])
	if finalKey != "" {
		curData[finalKey] = value
	}
	return nil
}

// GetNodeDataKV 获取节点中指定key的数据 -- key支持以 . 分割，递归获取数据，bool值返回false表示没有指定数据，返回true则表示有
func (wc *WorkflowContext) GetNodeDataKV(nodeName string, key string) (interface{}, bool) {
	nodeData, ok := wc.Context[nodeName]
	if !ok {
		return nil, false
	}
	nodeDataMap, ok := nodeData.(map[string]interface{})
	if !ok {
		return nil, false
	}
	// 分割key
	keyArr := strings.Split(strings.TrimSpace(key), ".")
	curData := nodeDataMap
	for ki, kArr := range keyArr {
		ak := strings.TrimSpace(kArr)
		// 结束
		if ki == len(keyArr)-1 {
			data, ok := curData[ak]
			if !ok {
				return nil, false
			}
			return data, true
		}
		// 递归查值
		curDataV, ok := curData[ak]
		if !ok {
			return nil, false
		}
		curData, ok = curDataV.(map[string]interface{})
		if !ok {
			return nil, false
		}
	}
	return nil, false
}

// GetAllContext 获取完整的上下文
func (wc *WorkflowContext) GetAllContext() map[string]interface{} {
	return wc.Context
}

// ExpressionEvaluator 表达式评估器
type ExpressionEvaluator struct {
	WorkflowContext *WorkflowContext `json:"workflow_context"` // 工作流上下文管理器
}

// NewExpressionEvaluator 创建新的表达式评估器
func NewExpressionEvaluator(workflowContext *WorkflowContext) *ExpressionEvaluator {
	if workflowContext == nil {
		workflowContext = NewWorkflowContext()
	}
	return &ExpressionEvaluator{
		WorkflowContext: workflowContext,
	}
}

// GetWorkflowContext 获取工作流上下文
func (e *ExpressionEvaluator) GetWorkflowContext() *WorkflowContext {
	return e.WorkflowContext
}

// EvaluateExpression 评估表达式（支持完整的工作流上下文）
func (e *ExpressionEvaluator) EvaluateExpression(expression string, inputData map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	// 处理各种n8n表达式模式
	switch {
	// 处理变量数据 $var.
	case strings.HasPrefix(expression, "$var."):
		inputData, exists := e.GetWorkflowContext().GetNodeData(ExpressVariablesNodeName)
		if !exists {
			inputData = make(map[string]interface{})
		}
		return e.extractFieldValue(strings.TrimSpace(strings.TrimPrefix(expression, "$var.")), inputData)

	// 处理节点引用格式(开始节点数据也在其中) - $('NodeName').item.json.field 或 $('NodeName').item.field
	case strings.HasPrefix(expression, "$('") && strings.Contains(expression, "')"):
		return e.resolveNodeReference(expression)

	//// 接收上个节点的数据 $before.
	//case strings.HasPrefix(expression, "$before."):
	//	return e.extractFieldValue(expression, inputData)

	//// 处理函数调用，如 $now.format(), $json.length() 等
	//case strings.HasPrefix(expression, "$") && strings.Contains(expression, "(") && strings.Contains(expression, ")"):
	//	return e.evaluateFunctionCall(expression, inputData)

	//// 处理 $json.field 格式 - 引用当前节点的JSON数据
	//case strings.HasPrefix(expression, "$json."):
	//	return e.extractJsonValue(expression, inputData)

	//// 处理二进制数据 $binary.data
	//case strings.HasPrefix(expression, "$binary."):
	//	return e.extractBinaryData(expression, inputData)

	//// 处理执行上下文 $execution.id, $execution.mode 等
	//case strings.HasPrefix(expression, "$execution."):
	//	return e.extractExecutionContext(expression)

	// 处理工作流信息 $workflow.id, $workflow.name 等
	case strings.HasPrefix(expression, "$workflow."):
		return e.extractWorkflowInfo(expression)

	// 处理当前时间
	case expression == "$now":
		return time.Now(), nil

	// 处理当前日期
	case expression == "$today":
		return time.Now().Format("2006-01-02"), nil

	// 处理当前时间戳
	case expression == "$timestamp":
		return time.Now().Unix(), nil

	//// 处理数学表达式
	//case strings.Contains(expression, "+") || strings.Contains(expression, "-") ||
	//	strings.Contains(expression, "*") || strings.Contains(expression, "/"):
	//	return e.evaluateMathExpression(expression, inputData)

	//// 处理直接值（等号开头但不是表达式）
	//case strings.HasPrefix(expression, "="):
	//	directValue := strings.TrimPrefix(expression, "=")
	//	return strings.TrimSpace(directValue), nil

	// 处理字符串字面量
	case strings.HasPrefix(expression, "\"") && strings.HasSuffix(expression, "\""):
		return strings.TrimPrefix(strings.TrimSuffix(expression, "\""), "\""), nil

	// 处理单引号字符串字面量
	case strings.HasPrefix(expression, "'") && strings.HasSuffix(expression, "'"):
		return strings.TrimPrefix(strings.TrimSuffix(expression, "'"), "'"), nil

	default:
		//// 尝试作为简单字段路径处理
		//if value, exists := inputData[expression]; exists {
		//	return value, nil
		//}
		//// 尝试解析为数字
		//if num, err := strconv.ParseFloat(expression, 64); err == nil {
		//	return num, nil
		//}
		//// 尝试解析为布尔值
		//if strings.ToLower(expression) == "true" {
		//	return true, nil
		//}
		//if strings.ToLower(expression) == "false" {
		//	return false, nil
		//}
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
	// 检查工作流上下文
	if e.WorkflowContext == nil {
		return fmt.Sprintf("{{ %s }}", expression), nil
	}

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
	nodeData, exists := e.WorkflowContext.GetNodeData(nodeName)
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
				return nil, fmt.Errorf("字段路径 '%s' 中1缺少 'json' 字段", fieldPath)
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
				return nil, fmt.Errorf("字段路径 '%s' 中2缺少 '%s'", fieldPath, fieldName)
			}
		}

		// 普通字段访问
		value, exists := current[part]
		if !exists {
			return nil, fmt.Errorf("字段路径 '%s' 中3缺少 '%s'", fieldPath, part)
		}
		if i == len(parts)-1 {
			return value, nil
		}
		nextMap, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("字段路径 '%s' 在 '%s' 处不是对象", fieldPath, part)
		}
		current = nextMap
	}
	return nil, fmt.Errorf("字段路径 '%s' 无效", fieldPath)
}

// SetWorkflowContext 设置工作流上下文
func (e *ExpressionEvaluator) SetWorkflowContext(workflowContext *WorkflowContext) {
	e.WorkflowContext = workflowContext
}
