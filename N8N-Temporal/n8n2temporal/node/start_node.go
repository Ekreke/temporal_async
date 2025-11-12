package node

import (
	"context"
	"errors"
	"fmt"
	"go.temporal.io/sdk/log"
)

// StartNode 开始节点，负责收拢所有参数并传递给后续节点
type StartNode struct {
	*BaseActivity
}

// StartNodeParameters 开始节点参数
type StartNodeParameters struct {
	GlobalParameters map[string]map[string]interface{} `json:"globalParameters"` // 全局参数配置，key为节点名，value为该节点的参数
}

// NewStartNodeActivity 创建开始节点实例
func NewStartNodeActivity(node WkFLowNode, express *ExpressionEvaluator) Activity {
	sNode := &StartNode{
		BaseActivity: &BaseActivity{
			NodeInfo:            &node,
			expressionEvaluator: express,
		},
	}
	// 注册节点
	return sNode
}

// GetNodeInfo 获取当前节点信息
func (s *StartNode) GetNodeInfo() *WkFLowNode {
	return s.NodeInfo
}

// Execute 执行开始节点逻辑
func (s *StartNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return s.ExecuteWithExecuteTiming(ctx, input, s.executeStartNode)
}

// executeStartNode 开始节点的具体执行逻辑
func (s *StartNode) executeStartNode(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	// 验证输入参数
	if input.Parameters == nil {
		return nil, fmt.Errorf("开始节点参数不能为空")
	}
	// 解析全局参数
	var params StartNodeParameters
	if err := s.parseParameters(input.Parameters, &params); err != nil {
		return nil, fmt.Errorf("解析开始节点参数失败: %w", err)
	}
	// 验证全局参数结构
	if len(params.GlobalParameters) == 0 {
		return nil, errors.New("开始节点未配置全局参数，使用空参数集")
	}
	// 将全局参数存储到WorkflowContext中，供后续节点使用
	if s.expressionEvaluator != nil && s.expressionEvaluator.WorkflowContext != nil {
		// 存储全局参数到特殊上下文键
		globalContextData := map[string]interface{}{
			"globalParameters": params.GlobalParameters,
		}
		s.expressionEvaluator.WorkflowContext.SetNodeData(ExpressGlobalNodeName, globalContextData)
	}
	res := &ExecNodeFuncResult{
		Data:          []map[string]interface{}{},
		AddTaskNum:    1,
		FinishTaskNum: 1,
	}
	return res, nil
}

// parseParameters 解析参数
func (s *StartNode) parseParameters(parameters map[string]interface{}, params *StartNodeParameters) error {
	// 尝试从parameters中获取globalParameters
	if globalParams, exists := parameters["globalParameters"]; exists {
		if globalParamsMap, ok := globalParams.(map[string]interface{}); ok {
			params.GlobalParameters = make(map[string]map[string]interface{})
			for nodeName, nodeParams := range globalParamsMap {
				if nodeParamsMap, ok := nodeParams.(map[string]interface{}); ok {
					params.GlobalParameters[nodeName] = nodeParamsMap
				} else {
					return fmt.Errorf("节点 %s 的参数格式无效，应为map[string]interface{}", nodeName)
				}
			}
		} else {
			return fmt.Errorf("globalParameters格式无效，应为map[string]interface{}")
		}
	} else {
		// 如果没有globalParameters字段，检查是否直接将整个parameters作为全局参数
		params.GlobalParameters = make(map[string]map[string]interface{})
		for key, value := range parameters {
			if nodeParamsMap, ok := value.(map[string]interface{}); ok {
				params.GlobalParameters[key] = nodeParamsMap
			} else {
				// 忽略非map类型的参数
				continue
			}
		}
	}

	return nil
}

// GetNodeParametersForNode 为指定节点获取参数
func (s *StartNode) GetNodeParametersForNode(nodeName string) map[string]interface{} {
	if s.expressionEvaluator == nil || s.expressionEvaluator.WorkflowContext == nil {
		return nil
	}

	globalData, exists := s.expressionEvaluator.WorkflowContext.GetNodeData(ExpressGlobalNodeName)
	if !exists {
		return nil
	}

	globalParamsInterface, exists := globalData["globalParameters"]
	if !exists {
		return nil
	}
	globalParams := globalParamsInterface.(map[string]map[string]interface{})

	if nodeParams, exists := globalParams[nodeName]; exists {
		return nodeParams
	}

	return nil
}

// GetAllGlobalParameters 获取所有全局参数
func (s *StartNode) GetAllGlobalParameters() map[string]map[string]interface{} {
	if s.expressionEvaluator == nil || s.expressionEvaluator.WorkflowContext == nil {
		return nil
	}

	globalData, exists := s.expressionEvaluator.WorkflowContext.GetNodeData(ExpressGlobalNodeName)
	if !exists {
		return nil
	}

	globalParamsInterface, exists := globalData["globalParameters"]
	if !exists {
		return nil
	}
	return globalParamsInterface.(map[string]map[string]interface{})
}

// GetLogger 获取logger
func (s *StartNode) GetLogger(ctx context.Context) log.Logger {
	return s.BaseActivity.GetLogger(ctx)
}

// ValidateInput 验证输入参数
func (s *StartNode) ValidateInput(input *ActivityInput) error {
	if err := s.BaseActivity.ValidateInput(input); err != nil {
		return err
	}

	// 开始节点的基本验证
	if input.NodeType != "n8n-nodes-base.start" {
		return fmt.Errorf("开始节点的类型必须为 n8n-nodes-base.start")
	}

	// 验证参数不能为空
	if input.Parameters == nil {
		return fmt.Errorf("开始节点参数不能为空")
	}

	// 验证参数格式
	if globalParams, exists := input.Parameters["globalParameters"]; exists {
		if globalParamsMap, ok := globalParams.(map[string]interface{}); ok {
			// 验证每个节点的参数格式
			for nodeName, nodeParams := range globalParamsMap {
				if _, ok := nodeParams.(map[string]interface{}); !ok {
					return fmt.Errorf("节点 %s 的参数格式无效，应为map[string]interface{}", nodeName)
				}
			}
		} else {
			return fmt.Errorf("globalParameters格式无效，应为map[string]interface{}")
		}
	}

	return nil
}
