package node

import (
	"context"
	"go.temporal.io/sdk/log"
)

var _ Activity = (*EndNode)(nil)

// EndNode 结束节点，负责展示工作流执行结果
type EndNode struct {
	*BaseActivity
}

// NewEndNode 创建结束节点实例
func NewEndNode() *EndNode {
	endNode := &EndNode{}
	return endNode
}

// GetNodeInfo 获取当前节点信息
func (e *EndNode) GetNodeInfo() *WkFLowNode {
	return e.NodeInfo
}

// End 逻辑入口
func (e *EndNode) End(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	e.BaseActivity = &BaseActivity{
		NodeInfo:            input.Node,
		expressionEvaluator: input.Express,
	}
	if err := e.ValidateInput(input); err != nil {
		return nil, err
	}
	return e.ExecuteWithExecuteTiming(ctx, input, e.executeEndNode)
}

// executeEndNode 结束节点的具体执行逻辑
func (e *EndNode) executeEndNode(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	// 返回成功结果
	return &ExecNodeFuncResult{
		Data: nil,
	}, nil
}

// GetLogger 获取logger
func (e *EndNode) GetLogger(ctx context.Context) log.Logger {
	return e.BaseActivity.GetLogger(ctx)
}

// ValidateInput 验证输入参数
func (e *EndNode) ValidateInput(input *ActivityInput) error {
	if err := e.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	return nil
}

// IsEndNode 检查是否为结束节点
func (e *EndNode) IsEndNode() bool {
	return true
}
