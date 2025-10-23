package node

import (
	"fmt"
)

// NodeRegistry 节点注册表
type NodeRegistry struct {
	nodes map[string]func() Activity
}

// NewNodeRegistry 创建新的节点注册表
func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{
		nodes: make(map[string]func() Activity),
	}
}

// RegisterNode 注册节点类型
func (r *NodeRegistry) RegisterNode(nodeType string, factory func() Activity) {
	r.nodes[nodeType] = factory
}

// GetNode 获取节点实例
func (r *NodeRegistry) GetNode(nodeType string) (Activity, error) {
	factory, exists := r.nodes[nodeType]
	if !exists {
		return nil, fmt.Errorf("未知的节点类型: %s", nodeType)
	}
	return factory(), nil
}

// GetAllNodeTypes 获取所有已注册的节点类型
func (r *NodeRegistry) GetAllNodeTypes() []string {
	var types []string
	for nodeType := range r.nodes {
		types = append(types, nodeType)
	}
	return types
}

// 全局节点注册表
var GlobalNodeRegistry = NewNodeRegistry()

// RegisterAllNodes 注册所有内置节点
func RegisterAllNodes() {
	// 注册开始节点
	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.start", func() Activity {
		return NewStartNodeActivity()
	})

	// 注册结束节点
	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.end", func() Activity {
		return NewEndNodeActivity()
	})

	// 注册变量节点
	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.variable", func() Activity {
		return NewVariableNodeActivity()
	})

	// 注册统一条件节点
	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.conditional", func() Activity {
		return NewConditionalNodeActivity()
	})

	// 注册Python Docker节点
	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.pythonDocker", func() Activity {
		return NewPythonDockerNodeActivity()
	})

	// 注册自定义节点
	GlobalNodeRegistry.RegisterNode("CUSTOM.customNode", func() Activity {
		return NewCustomNodeActivity()
	})

	// 注册兼容节点类型别名
	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.pythonCode", func() Activity {
		return NewPythonDockerNodeActivity() // 使用Docker节点替代
	})

	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.domainResolve", func() Activity {
		return NewDomainResolveActivity()
	})

	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.if", func() Activity {
		return NewConditionalNodeActivity() // 使用统一条件节点替代
	})

	GlobalNodeRegistry.RegisterNode("n8n-nodes-base.custom", func() Activity {
		return NewCustomNodeActivity()
	})
}

// GetNodeByType 根据类型获取节点
func GetNodeByType(nodeType string) (Activity, error) {
	return GlobalNodeRegistry.GetNode(nodeType)
}

// GetAllRegisteredNodes 获取所有已注册的节点信息
func GetAllRegisteredNodes() map[string]*ActivityInfo {
	result := make(map[string]*ActivityInfo)
	for nodeType := range GlobalNodeRegistry.nodes {
		if node, err := GlobalNodeRegistry.GetNode(nodeType); err == nil {
			result[nodeType] = node.GetNodeInfo()
		}
	}
	return result
}

// 初始化所有节点
func init() {
	RegisterAllNodes()
}
