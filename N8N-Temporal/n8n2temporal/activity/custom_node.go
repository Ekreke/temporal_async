package activity

import (
	"context"
	"go.temporal.io/sdk/activity"
	"time"
)

// CustomNodeActivity 自定义节点 Activity
type CustomNodeActivity struct {
	NodeType string
}

func (a *CustomNodeActivity) ExecuteCustomNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("执行自定义节点活动", "nodeType", a.NodeType, "input", input)

	// 根据节点类型执行不同的业务逻辑
	result := map[string]interface{}{
		"nodeType":  a.NodeType,
		"processed": true,
		"timestamp": time.Now(),
	}

	return result, nil
}
