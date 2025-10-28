package node

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/log"
	"sort"
	"time"
)

// EndNode 结束节点，负责展示工作流执行结果
type EndNode struct {
	*BaseActivity
	ResultMode string
}

// EndNodeParameters 结束节点参数
type EndNodeParameters struct {
	ResultMode string `json:"resultMode"` // 结果展示模式: "last" 或 "all"
}

// NewEndNodeActivity 创建结束节点实例
func NewEndNodeActivity(express *ExpressionEvaluator) Activity {
	return &EndNode{
		BaseActivity: &BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "end-node",
				Name:        "End Node",
				Type:        "n8n-nodes-base.end",
				Description: "工作流结束节点，配置流水线结果展示策略",
				Version:     "1.0.0",
				Category:    "core",
				Icon:        "⏹️",
			},
			expressionEvaluator: express,
		},
	}
}

// Execute 执行结束节点逻辑
func (e *EndNode) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	data, err := e.executeEndNode(input)
	if err != nil {
		return e.CreateErrorOutput(input, err), nil
	}
	return e.CreateSuccessOutput(input, data), nil
}

// executeEndNode 结束节点的具体执行逻辑
func (e *EndNode) executeEndNode(input *ActivityInput) (map[string]interface{}, error) {
	logger := &SimpleLogger{}

	// 解析参数
	var params EndNodeParameters
	if err := e.parseParameters(input.Parameters, &params); err != nil {
		return nil, fmt.Errorf("解析结束节点参数失败: %w", err)
	}

	// 获取工作流上下文中的所有节点数据
	var allNodeData map[string]interface{}
	if e.expressionEvaluator != nil && e.expressionEvaluator.workflowContext != nil {
		allNodeData = e.expressionEvaluator.workflowContext.GetAllContext()
	}

	// 根据结果展示模式生成结果
	var result interface{}
	switch params.ResultMode {
	case "last":
		e.ResultMode = "last"
		result = e.generateLastNodeResult(allNodeData)
	case "all":
		e.ResultMode = "all"
		result = e.generateAllNodesResult(allNodeData)
	default:
		// 默认为所有节点结果
		e.ResultMode = "all"
		result = e.generateAllNodesResult(allNodeData)
	}

	logger.Info("结束节点执行成功", "resultMode", params.ResultMode, "nodeCount", len(allNodeData))

	// 返回成功结果
	return map[string]interface{}{
		"success":     true,
		"message":     "工作流执行完成",
		"resultMode":  params.ResultMode,
		"result":      result,
		"nodeCount":   len(allNodeData),
		"completedAt": time.Now().Format("2006-01-02T15:04:05Z07:00"),
	}, nil
}

// parseParameters 解析参数
func (e *EndNode) parseParameters(parameters map[string]interface{}, params *EndNodeParameters) error {
	// 默认值为 all
	params.ResultMode = "all"

	// 从parameters中获取resultMode
	if resultMode, exists := parameters["resultMode"]; exists {
		if resultModeStr, ok := resultMode.(string); ok {
			if resultModeStr == "last" || resultModeStr == "all" {
				params.ResultMode = resultModeStr
			} else {
				return fmt.Errorf("无效的resultMode值: %s，只支持 'last' 或 'all'", resultModeStr)
			}
		} else {
			return fmt.Errorf("resultMode必须是字符串类型")
		}
	}

	return nil
}

// generateLastNodeResult 生成最后节点的结果
func (e *EndNode) generateLastNodeResult(allNodeData map[string]interface{}) interface{} {
	if len(allNodeData) == 0 {
		return map[string]interface{}{
			"message": "工作流中没有节点数据",
		}
	}

	// 过滤掉系统节点
	var nodeNames []string
	for nodeName := range allNodeData {
		if nodeName != ExpressGlobalNodeName && nodeName != ExpressVariablesNodeName {
			nodeNames = append(nodeNames, nodeName)
		}
	}

	if len(nodeNames) == 0 {
		return map[string]interface{}{
			"message": "工作流中没有用户节点数据",
		}
	}

	// 获取最后一个节点（按名称排序）
	sort.Strings(nodeNames)
	lastNodeName := nodeNames[len(nodeNames)-1]
	lastNodeData := allNodeData[lastNodeName]

	return map[string]interface{}{
		"lastNodeName": lastNodeName,
		"lastNodeData": lastNodeData,
	}
}

// generateAllNodesResult 生成所有节点的结果汇总
func (e *EndNode) generateAllNodesResult(allNodeData map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// 添加所有节点结果
	for nodeName, nodeData := range allNodeData {
		if nodeName == ExpressGlobalNodeName || nodeName == ExpressVariablesNodeName {
			// 系统节点数据放入特殊区域
			if result["system"] == nil {
				result["system"] = make(map[string]interface{})
			}
			systemData := result["system"].(map[string]interface{})
			systemData[nodeName] = nodeData
		} else {
			// 用户节点数据直接放入结果
			result[nodeName] = nodeData
		}
	}

	// 添加统计信息
	stats := map[string]interface{}{
		"totalNodes":    len(allNodeData),
		"userNodes":     len(allNodeData) - 0,
		"executionTime": time.Now().Format("2006-01-02T15:04:05Z07:00"),
	}

	// 计算实际用户节点数量
	userNodeCount := 0
	for nodeName := range allNodeData {
		if nodeName != ExpressGlobalNodeName && nodeName != ExpressVariablesNodeName {
			userNodeCount++
		}
	}
	stats["userNodes"] = userNodeCount

	result["statistics"] = stats
	return result
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

// GetResultMode 获取结果展示模式
func (e *EndNode) GetResultMode() string {
	return e.ResultMode
}
