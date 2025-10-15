package activity

import (
	"context"
	"go.temporal.io/sdk/activity"
	"time"
)

// PythonCodeActivity Python 代码执行 Activity（模拟）
type PythonCodeActivity struct{}

func (a *PythonCodeActivity) ExecutePythonCode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("执行 Python 代码活动", "input", input)

	// 模拟 Python 代码执行逻辑
	result := map[string]interface{}{
		"executed":    true,
		"myNewField":  1,
		"processedAt": time.Now(),
	}

	// 这里可以集成 Go 的 Python 解释器或调用外部 Python 脚本
	return result, nil
}
