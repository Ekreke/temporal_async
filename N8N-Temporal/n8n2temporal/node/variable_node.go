package node

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/log"
	"sync"
)

// VariableNode 变量节点，负责存储和管理工作流变量
type VariableNode struct {
	*BaseActivity
}

// VariableNodeParameters 变量节点参数
type VariableNodeParameters struct {
	Variables      map[string]interface{} `json:"variables"`      // 要存储的变量
	Operation      string                 `json:"operation"`      // 操作类型: "set", "get", "delete", "clear"
	OverwriteMode  string                 `json:"overwriteMode"`  // 覆盖模式: "overwrite", "merge", "skip"
	VariablesScope string                 `json:"variablesScope"` // 变量作用域: "global", "local"
}

// GlobalVariables 全局变量存储器（确保并发安全）
var GlobalVariables = &VariableStorage{
	data: make(map[string]interface{}),
	mu:   sync.RWMutex{},
}

// VariableStorage 变量存储器，提供并发安全的存储功能
type VariableStorage struct {
	data map[string]interface{}
	mu   sync.RWMutex
}

// SetVariable 设置变量
func (vs *VariableStorage) SetVariable(key string, value interface{}) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.data[key] = value
}

// GetVariable 获取变量
func (vs *VariableStorage) GetVariable(key string) (interface{}, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	value, exists := vs.data[key]
	return value, exists
}

// GetAllVariables 获取所有变量
func (vs *VariableStorage) GetAllVariables() map[string]interface{} {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	// 创建副本避免并发修改问题
	result := make(map[string]interface{})
	for k, v := range vs.data {
		result[k] = v
	}
	return result
}

// DeleteVariable 删除变量
func (vs *VariableStorage) DeleteVariable(key string) bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if _, exists := vs.data[key]; exists {
		delete(vs.data, key)
		return true
	}
	return false
}

// ClearVariables 清空所有变量
func (vs *VariableStorage) ClearVariables() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.data = make(map[string]interface{})
}

// SetMultipleVariables 设置多个变量
func (vs *VariableStorage) SetMultipleVariables(variables map[string]interface{}) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for k, v := range variables {
		vs.data[k] = v
	}
}

// NewVariableNodeActivity 创建变量节点实例
func NewVariableNodeActivity() Activity {
	return &VariableNode{
		BaseActivity: &BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "variable-node",
				Name:        "Variable Node",
				Type:        "n8n-nodes-base.variable",
				Description: "工作流变量节点，存储和管理工作流变量",
				Version:     "1.0.0",
				Category:    "core",
				Icon:        "🗃️",
			},
		},
	}
}

// Execute 执行变量节点逻辑
func (v *VariableNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	// 在测试环境中可能没有Temporal上下文，直接执行
	data, err := v.executeVariableNode(input)
	if err != nil {
		return v.CreateErrorOutput(input, err), nil
	}
	return v.CreateSuccessOutput(input, data), nil
}

// executeVariableNode 变量节点的具体执行逻辑
func (v *VariableNode) executeVariableNode(input *ActivityInput) (map[string]interface{}, error) {
	logger := &SimpleLogger{}

	// 解析参数
	var params VariableNodeParameters
	if err := v.parseParameters(input.Parameters, &params); err != nil {
		return nil, fmt.Errorf("解析变量节点参数失败: %w", err)
	}

	// 根据操作类型执行相应操作
	var result map[string]interface{}
	var err error

	switch params.Operation {
	case "set":
		result, err = v.executeSetOperation(input, params)
	case "get":
		result, err = v.executeGetOperation(params)
	case "delete":
		result, err = v.executeDeleteOperation(params)
	case "clear":
		result, err = v.executeClearOperation(params)
	default:
		// 默认为set操作
		result, err = v.executeSetOperation(input, params)
	}

	if err != nil {
		logger.Error("变量节点操作失败", "operation", params.Operation, "error", err)
		return nil, err
	}

	logger.Info("变量节点执行成功", "operation", params.Operation, "variablesCount", len(params.Variables))

	// 将变量数据也存储到工作流上下文中，供其他节点使用
	if v.expressionEvaluator != nil && v.expressionEvaluator.workflowContext != nil {
		v.expressionEvaluator.workflowContext.SetNodeData("__variables__", GlobalVariables.GetAllVariables())
	}

	return result, nil
}

// parseParameters 解析参数
func (v *VariableNode) parseParameters(parameters map[string]interface{}, params *VariableNodeParameters) error {
	// 设置默认值
	params.Operation = "set"
	params.OverwriteMode = "overwrite"
	params.VariablesScope = "global"
	params.Variables = make(map[string]interface{})

	// 解析操作类型
	if operation, exists := parameters["operation"]; exists {
		if operationStr, ok := operation.(string); ok {
			if operationStr == "set" || operationStr == "get" || operationStr == "delete" || operationStr == "clear" {
				params.Operation = operationStr
			} else {
				return fmt.Errorf("无效的operation值: %s，支持: set, get, delete, clear", operationStr)
			}
		} else {
			return fmt.Errorf("operation必须是字符串类型")
		}
	}

	// 解析覆盖模式
	if overwriteMode, exists := parameters["overwriteMode"]; exists {
		if overwriteModeStr, ok := overwriteMode.(string); ok {
			if overwriteModeStr == "overwrite" || overwriteModeStr == "merge" || overwriteModeStr == "skip" {
				params.OverwriteMode = overwriteModeStr
			} else {
				return fmt.Errorf("无效的overwriteMode值: %s，支持: overwrite, merge, skip", overwriteModeStr)
			}
		}
	}

	// 解析变量作用域
	if variablesScope, exists := parameters["variablesScope"]; exists {
		if variablesScopeStr, ok := variablesScope.(string); ok {
			if variablesScopeStr == "global" || variablesScopeStr == "local" {
				params.VariablesScope = variablesScopeStr
			} else {
				return fmt.Errorf("无效的variablesScope值: %s，支持: global, local", variablesScopeStr)
			}
		}
	}

	// 解析变量数据
	if variables, exists := parameters["variables"]; exists {
		if variablesMap, ok := variables.(map[string]interface{}); ok {
			params.Variables = variablesMap
		} else {
			return fmt.Errorf("variables格式无效，应为map[string]interface{}")
		}
	}

	return nil
}

// executeSetOperation 执行设置变量操作
func (v *VariableNode) executeSetOperation(input *ActivityInput, params VariableNodeParameters) (map[string]interface{}, error) {
	if len(params.Variables) == 0 {
		return map[string]interface{}{
			"success":   true,
			"message":   "没有要设置的变量",
			"operation": params.Operation,
		}, nil
	}

	setCount := 0
	skippedCount := 0

	for key, value := range params.Variables {
		switch params.OverwriteMode {
		case "overwrite":
			GlobalVariables.SetVariable(key, value)
			setCount++
		case "skip":
			if _, exists := GlobalVariables.GetVariable(key); !exists {
				GlobalVariables.SetVariable(key, value)
				setCount++
			} else {
				skippedCount++
			}
		case "merge":
			// 对于复杂类型进行合并，简单类型直接覆盖
			if existingValue, exists := GlobalVariables.GetVariable(key); exists {
				if existingMap, ok := existingValue.(map[string]interface{}); ok {
					if newValueMap, ok := value.(map[string]interface{}); ok {
						// 合并两个map
						mergedMap := make(map[string]interface{})
						for k, v := range existingMap {
							mergedMap[k] = v
						}
						for k, v := range newValueMap {
							mergedMap[k] = v
						}
						GlobalVariables.SetVariable(key, mergedMap)
						setCount++
						continue
					}
				}
			}
			// 如果不能合并或不存在，直接设置
			GlobalVariables.SetVariable(key, value)
			setCount++
		}
	}

	return map[string]interface{}{
		"success":      true,
		"message":      fmt.Sprintf("成功设置 %d 个变量", setCount),
		"operation":    params.Operation,
		"setCount":     setCount,
		"skippedCount": skippedCount,
		"variablesSet": setCount,
	}, nil
}

// executeGetOperation 执行获取变量操作
func (v *VariableNode) executeGetOperation(params VariableNodeParameters) (map[string]interface{}, error) {
	allVariables := GlobalVariables.GetAllVariables()

	if len(params.Variables) == 0 {
		// 获取所有变量
		return map[string]interface{}{
			"success":       true,
			"message":       fmt.Sprintf("获取到 %d 个变量", len(allVariables)),
			"operation":     params.Operation,
			"variables":     allVariables,
			"variableCount": len(allVariables),
		}, nil
	}

	// 获取指定的变量
	result := make(map[string]interface{})
	foundCount := 0

	for key := range params.Variables {
		if value, exists := GlobalVariables.GetVariable(key); exists {
			result[key] = value
			foundCount++
		}
	}

	return map[string]interface{}{
		"success":        true,
		"message":        fmt.Sprintf("获取到 %d 个变量，共请求 %d 个", foundCount, len(params.Variables)),
		"operation":      params.Operation,
		"variables":      result,
		"foundCount":     foundCount,
		"requestedCount": len(params.Variables),
	}, nil
}

// executeDeleteOperation 执行删除变量操作
func (v *VariableNode) executeDeleteOperation(params VariableNodeParameters) (map[string]interface{}, error) {
	if len(params.Variables) == 0 {
		return map[string]interface{}{
			"success":   true,
			"message":   "没有要删除的变量",
			"operation": params.Operation,
		}, nil
	}

	deletedCount := 0

	for key := range params.Variables {
		if GlobalVariables.DeleteVariable(key) {
			deletedCount++
		}
	}

	return map[string]interface{}{
		"success":        true,
		"message":        fmt.Sprintf("成功删除 %d 个变量", deletedCount),
		"operation":      params.Operation,
		"deletedCount":   deletedCount,
		"requestedCount": len(params.Variables),
	}, nil
}

// executeClearOperation 执行清空变量操作
func (v *VariableNode) executeClearOperation(params VariableNodeParameters) (map[string]interface{}, error) {
	varCountBefore := len(GlobalVariables.GetAllVariables())
	GlobalVariables.ClearVariables()

	return map[string]interface{}{
		"success":      true,
		"message":      fmt.Sprintf("成功清空 %d 个变量", varCountBefore),
		"operation":    params.Operation,
		"clearedCount": varCountBefore,
	}, nil
}

// GetLogger 获取logger
func (v *VariableNode) GetLogger(ctx context.Context) log.Logger {
	return v.BaseActivity.GetLogger(ctx)
}

// ValidateInput 验证输入参数
func (v *VariableNode) ValidateInput(input *ActivityInput) error {
	if err := v.BaseActivity.ValidateInput(input); err != nil {
		return err
	}

	// 变量节点的基本验证
	if input.NodeType != "n8n-nodes-base.variable" {
		return fmt.Errorf("变量节点的类型必须为 n8n-nodes-base.variable")
	}

	// 验证参数
	if input.Parameters != nil {
		// 验证operation
		if operation, exists := input.Parameters["operation"]; exists {
			if operationStr, ok := operation.(string); ok {
				validOps := []string{"set", "get", "delete", "clear"}
				isValid := false
				for _, op := range validOps {
					if op == operationStr {
						isValid = true
						break
					}
				}
				if !isValid {
					return fmt.Errorf("无效的operation值: %s", operationStr)
				}
			} else {
				return fmt.Errorf("operation必须是字符串类型")
			}
		}

		// 验证variables格式
		if variables, exists := input.Parameters["variables"]; exists {
			if _, ok := variables.(map[string]interface{}); !ok {
				return fmt.Errorf("variables格式无效，应为map[string]interface{}")
			}
		}
	}

	return nil
}

// GetAllStoredVariables 获取所有存储的变量（静态方法）
func GetAllStoredVariables() map[string]interface{} {
	return GlobalVariables.GetAllVariables()
}

// ClearAllStoredVariables 清空所有存储的变量（静态方法）
func ClearAllStoredVariables() {
	GlobalVariables.ClearVariables()
}
