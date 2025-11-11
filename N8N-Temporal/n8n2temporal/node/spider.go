package node

import (
	"context"
	"fmt"
	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
)

var _ Activity = (*SpiderActivity)(nil)

// SpiderActivity 爬虫节点 Activity
type SpiderActivity struct {
	grpcCli taskv2.TaskManagerServiceClient
	*BaseActivity
}

// NewSpiderActivity 创建新的爬虫节点
func NewSpiderActivity(grpcClient taskv2.TaskManagerServiceClient) *SpiderActivity {
	activity := &SpiderActivity{
		grpcCli: grpcClient,
	}
	return activity
}

// RegisterSpider 注册节点到worker
func (a *SpiderActivity) RegisterSpider(ctx context.Context, input *ActivityInput,
	express *ExpressionEvaluator, node WkFLowNode) (*ActivityOutput, error) {
	if a.grpcCli == nil {
		return nil, fmt.Errorf("RegisterSpider Error, grpc client not initialized")
	}
	a.BaseActivity = &BaseActivity{
		NodeInfo:            &node,
		expressionEvaluator: express,
	}
	return a.Execute(ctx, input)
}

// GetNodeInfo 获取当前节点信息
func (a *SpiderActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// Execute 执行节点逻辑
func (a *SpiderActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return nil, nil
}

// ValidateInput 验证输入参数
func (a *SpiderActivity) ValidateInput(input *ActivityInput) error {
	// 统一基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 参数检查
	return nil
}

// executeSpider 执行实际逻辑
func (a *SpiderActivity) executeSpider(input *ActivityInput) (map[string]interface{}, error) {
	return nil, nil
}
