package node

import (
	"context"
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
	// 组装开始节点的parameters 以及 传入的inputData参数;优先级为inputData的同步key覆盖parameters中的变量。这里的返回值将会被设置到全局变量
	var result = map[string]interface{}{}
	if len(input.Parameters) > 0 {
		result = input.Parameters
	}
	if len(input.InputData) > 0 && input.InputData[0] != nil {
		for k, v := range input.InputData[0] {
			result[k] = v
		}
	}
	res := &ExecNodeFuncResult{
		Data: []map[string]interface{}{result},
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
	return nil
}
