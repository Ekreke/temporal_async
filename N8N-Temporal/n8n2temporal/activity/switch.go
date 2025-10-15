package activity

import (
	"context"
	"go.temporal.io/sdk/activity"
	"time"
)

// SwitchNodeActivity Switch 节点 Activity
type SwitchNodeActivity struct{}

func (a *SwitchNodeActivity) ExecuteSwitchNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("执行 Switch 节点活动", "input", input)

	// 实现多路分支逻辑
	branch := "default"
	if value, exists := input["value"]; exists {
		switch value {
		case "1":
			branch = "case1"
		case "2":
			branch = "case2"
		case "3":
			branch = "case3"
		}
	}

	result := map[string]interface{}{
		"selectedBranch": branch,
		"timestamp":      time.Now(),
	}

	return result, nil
}
