package node

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/log"
)

// EndNode 结束节点，负责展示工作流执行结果
type EndNode struct {
	*BaseActivity
}

// NewEndNodeActivity 创建结束节点实例
func NewEndNodeActivity(node WkFLowNode, express *ExpressionEvaluator) Activity {
	endNode := &EndNode{
		BaseActivity: &BaseActivity{
			NodeInfo:            &node,
			expressionEvaluator: express,
		},
	}
	// 注册节点
	return endNode
}

// GetNodeInfo 获取当前节点信息
func (e *EndNode) GetNodeInfo() *WkFLowNode {
	return e.NodeInfo
}

// Execute 执行结束节点逻辑
func (e *EndNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
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

	// 结束节点的基本验证
	if input.NodeType != "n8n-nodes-base.end" {
		return fmt.Errorf("结束节点的类型必须为 n8n-nodes-base.end")
	}

	// 验证参数
	if input.Parameters != nil {
		if resultMode, exists := input.Parameters["resultMode"]; exists {
			if resultModeStr, ok := resultMode.(string); ok {
				if resultModeStr != "last" && resultModeStr != "all" {
					return fmt.Errorf("无效的resultMode值: %s，只支持 'last' 或 'all'", resultModeStr)
				}
			} else {
				return fmt.Errorf("resultMode必须是字符串类型")
			}
		}
	}

	return nil
}

// IsEndNode 检查是否为结束节点
func (e *EndNode) IsEndNode() bool {
	return true
}
