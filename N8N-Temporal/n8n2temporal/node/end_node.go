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
		Data:          nil,
		AddTaskNum:    0,
		FinishTaskNum: 1,
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
