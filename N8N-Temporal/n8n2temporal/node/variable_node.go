package node

import (
	"context"
	"errors"
	"fmt"
	"go.temporal.io/sdk/log"
)

const (
	ExpressVariablesNodeName = "__variables__"
	ExpressGlobalNodeName    = "__global__"
)

// VariableNode 变量节点，负责存储和管理工作流变量
type VariableNode struct {
	*BaseActivity
}

// VariableNodeParameters 变量节点参数
type VariableNodeParameters struct {
	Variables     map[string]interface{} `json:"variables"`     // 要存储的变量
	Operation     string                 `json:"operation"`     // 操作类型: "set"设置
	OverwriteMode string                 `json:"overwriteMode"` // 覆盖模式: "overwrite", "merge", "skip"
}

// NewVariableNodeActivity 创建变量节点实例
func NewVariableNodeActivity(node WkFLowNode, express *ExpressionEvaluator) Activity {
	varNode := &VariableNode{
		BaseActivity: &BaseActivity{
			NodeInfo:            &node,
			expressionEvaluator: express,
		},
	}
	// 注册节点
	return varNode
}

// GetLogger 获取log对象
func (v *VariableNode) GetLogger(ctx context.Context) log.Logger {
	return v.BaseActivity.GetLogger(ctx)
}

// GetNodeInfo 获取当前节点信息
func (v *VariableNode) GetNodeInfo() *WkFLowNode {
	return v.NodeInfo
}

// Execute 执行变量节点逻辑
func (v *VariableNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return v.ExecuteWithExecuteTiming(ctx, input, v.executeVariableNode)
}

// executeVariableNode 变量节点的具体执行逻辑
func (v *VariableNode) executeVariableNode(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	// 解析参数
	var params VariableNodeParameters
	if err := v.parseParameters(input.Parameters, &params); err != nil {
		return nil, fmt.Errorf("解析变量节点参数失败: %w", err)
	}
	// 根据操作类型执行相应操作
	var result = make(map[string]interface{})
	var err error
	// 操作选择
	switch params.Operation {
	case "set":
		result, err = v.executeSetOperation(params)
	default:
		return nil, errors.New("无效的变量操作符")
	}
	if err != nil {
		return nil, errors.New("变量节点操作失败")
	}
	res := &ExecNodeFuncResult{
		Data: []map[string]interface{}{result},
	}
	return res, nil
}

// parseParameters 解析参数
func (v *VariableNode) parseParameters(parameters map[string]interface{}, params *VariableNodeParameters) error {
	// 设置默认值
	params.Operation = "set"
	params.OverwriteMode = "overwrite"
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
func (v *VariableNode) executeSetOperation(params VariableNodeParameters) (map[string]interface{}, error) {
	// 获取工作流上下文，确保不为空
	wkContext := v.GetExpressionEvaluator().GetWorkflowContext()
	// 设置变量
	setCount := 0     // 设置成功次数
	skippedCount := 0 // 跳过次数
	// 循环处理
	for key, value := range params.Variables {
		switch params.OverwriteMode { // 选择模式
		case "overwrite": // 覆盖模式
			err := wkContext.SetNodeDataKV(ExpressVariablesNodeName, key, value)
			if err != nil {
				return nil, err
			}
			setCount++
		case "skip": // 跳过
			_, exists := wkContext.GetNodeDataKV(ExpressVariablesNodeName, key)
			if exists {
				skippedCount++
				continue
			}
			err := wkContext.SetNodeDataKV(ExpressVariablesNodeName, key, value)
			if err != nil {
				return nil, err
			}
			setCount++
		case "merge": // 对于复杂类型进行合并，简单类型直接覆盖
			existingValue, exists := wkContext.GetNodeDataKV(ExpressVariablesNodeName, key)
			if !exists { // 如果不能合并或不存在，直接设置
				err := wkContext.SetNodeDataKV(ExpressVariablesNodeName, key, value)
				if err != nil {
					return nil, err
				}
				setCount++
				continue
			}
			existingMap, ok := existingValue.(map[string]interface{})
			if !ok {
				continue
			}
			newValueMap, ok := value.(map[string]interface{})
			if !ok {
				continue
			}
			// 合并两个map
			mergedMap := make(map[string]interface{})
			for k, v := range existingMap {
				mergedMap[k] = v
			}
			for k, v := range newValueMap {
				mergedMap[k] = v
			}
			err := wkContext.SetNodeDataKV(ExpressVariablesNodeName, key, mergedMap)
			if err != nil {
				return nil, err
			}
			setCount++
		}
	}
	res, exits := wkContext.GetNodeData(ExpressVariablesNodeName)
	if !exits {
		return nil, errors.New("无有效的variable数据")
	}
	return res, nil
}

// executeGetOperation 执行获取变量操作
func (v *VariableNode) executeGetOperation(params VariableNodeParameters) (map[string]interface{}, error) {
	express := v.GetExpressionEvaluator()
	if express == nil {
		return nil, fmt.Errorf("表达式评估器为空")
	}

	wkContext := express.GetWorkflowContext()
	if wkContext == nil {
		// 如果工作流上下文为空，返回空结果
		return map[string]interface{}{
			"success":       true,
			"message":       "工作流上下文为空，没有变量",
			"operation":     params.Operation,
			"variables":     map[string]interface{}{},
			"variableCount": 0,
		}, nil
	}

	allVariables, _ := wkContext.GetNodeData(ExpressVariablesNodeName)

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
		value, exists := wkContext.GetNodeDataKV(ExpressVariablesNodeName, key)
		if exists {
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

// executeClearOperation 执行清空变量操作
func (v *VariableNode) executeClearOperation(params VariableNodeParameters) (map[string]interface{}, error) {
	res := map[string]interface{}{
		"success":      true,
		"operation":    params.Operation,
		"clearedCount": 0,
	}

	express := v.GetExpressionEvaluator()
	if express == nil {
		return res, nil
	}

	wkContext := express.GetWorkflowContext()
	if wkContext == nil {
		return res, nil
	}

	nodeName, ok := wkContext.GetNodeData(ExpressVariablesNodeName)
	if !ok {
		return res, nil
	}
	wkContext.SetNodeData(ExpressVariablesNodeName, map[string]interface{}{})
	res["clearedCount"] = len(nodeName)
	return res, nil
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
