package node

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/log"
)

// StartNode 开始节点，负责收拢所有参数并传递给后续节点
type StartNode struct {
	*BaseActivity
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
	//// 将全局参数存储到WorkflowContext中，供后续节点使用, 这里的代码改到workflow中执行
	//if s.expressionEvaluator != nil && s.expressionEvaluator.WorkflowContext != nil {
	//	// 存储全局参数到特殊上下文键
	//	s.expressionEvaluator.WorkflowContext.SetNodeData(ExpressGlobalNodeName, input.Parameters)
	//}
	res := &ExecNodeFuncResult{
		Data: []map[string]interface{}{},
	}
	return res, nil
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
