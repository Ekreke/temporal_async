package node

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/activity"
)

// NodeExecutor 节点执行器
type NodeExecutor struct {
	registry *NodeRegistry
}

// NewNodeExecutor 创建新的节点执行器
func NewNodeExecutor() *NodeExecutor {
	return &NodeExecutor{
		registry: GlobalNodeRegistry,
	}
}

// ExecuteNode 执行节点（向后兼容接口）
func (e *NodeExecutor) ExecuteNode(ctx context.Context, nodeType string, input map[string]interface{}) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("执行节点", "nodeType", nodeType)

	// 获取节点实例
	node, err := e.registry.GetNode(nodeType)
	if err != nil {
		logger.Error("获取节点失败", "nodeType", nodeType, "error", err)
		return map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("获取节点失败: %v", err),
		}, nil
	}

	// 转换输入格式
	nodeInput := &ActivityInput{
		NodeID:      getStringValue(input, "nodeId"),
		NodeName:    getStringValue(input, "nodeName"),
		NodeType:    nodeType,
		InputData:   getInputData(input),
		Parameters:  getParameters(input),
		WorkflowID:  getStringValue(input, "workflowId"),
		ExecutionID: getStringValue(input, "executionId"),
	}

	// 验证输入
	if err := node.ValidateInput(nodeInput); err != nil {
		logger.Error("输入验证失败", "nodeType", nodeType, "error", err)
		return map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("输入验证失败: %v", err),
		}, nil
	}

	// 执行节点
	output, err := node.Execute(ctx, nodeInput)
	if err != nil {
		logger.Error("节点执行失败", "nodeType", nodeType, "error", err)
		return map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("节点执行失败: %v", err),
		}, nil
	}

	// 转换输出格式
	if output.Success {
		return map[string]interface{}{
			"success": true,
			"data":    output.Data,
		}, nil
	} else {
		return map[string]interface{}{
			"success": false,
			"error":   output.Error,
		}, nil
	}
}

// ExecuteGenericNode 执行通用节点（用于未知节点类型）
func (e *NodeExecutor) ExecuteGenericNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	logger := activity.GetLogger(ctx)

	// 尝试从输入中获取节点类型
	nodeType := getStringValue(input, "nodeType")
	if nodeType == "" {
		if parameters, ok := input["parameters"].(map[string]interface{}); ok {
			nodeType = getStringValue(parameters, "nodeType")
		}
	}

	if nodeType == "" {
		nodeType = "CUSTOM.customNode" // 默认类型
	}

	logger.Info("执行通用节点", "nodeType", nodeType)
	return e.ExecuteNode(ctx, nodeType, input)
}

// GetNodeInfo 获取节点信息
func (e *NodeExecutor) GetNodeInfo(nodeType string) (*ActivityInfo, error) {
	node, err := e.registry.GetNode(nodeType)
	if err != nil {
		return nil, err
	}
	return node.GetNodeInfo(), nil
}

// ListAllNodes 列出所有可用节点
func (e *NodeExecutor) ListAllNodes() map[string]*ActivityInfo {
	return GetAllRegisteredNodes()
}

// ValidateNodeType 验证节点类型是否支持
func (e *NodeExecutor) ValidateNodeType(nodeType string) bool {
	_, exists := e.registry.nodes[nodeType]
	return exists
}
