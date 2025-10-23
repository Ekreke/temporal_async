package node

import (
	"testing"
)

// TestNodeRegistry 测试节点注册表
func TestNodeRegistry(t *testing.T) {
	registry := NewNodeRegistry()

	// 测试注册节点
	registry.RegisterNode("test.node", func() Activity {
		return NewPythonDockerNodeActivity()
	})

	// 测试获取节点
	node, err := registry.GetNode("test.node")
	if err != nil {
		t.Fatalf("获取节点失败: %v", err)
	}

	if node.GetNodeInfo().Type != "n8n-nodes-base.pythonDocker" {
		t.Errorf("期望节点类型为 'n8n-nodes-base.pythonDocker', 实际为 '%s'", node.GetNodeInfo().Type)
	}

	// 测试获取不存在的节点
	_, err = registry.GetNode("nonexistent.node")
	if err == nil {
		t.Error("期望获取不存在的节点时失败")
	}

	// 测试获取所有节点类型
	types := registry.GetAllNodeTypes()
	if len(types) == 0 {
		t.Error("期望至少有一个节点类型")
	}

	found := false
	for _, nodeType := range types {
		if nodeType == "test.node" {
			found = true
			break
		}
	}
	if !found {
		t.Error("期望在节点类型列表中找到 'test.node'")
	}
}

// TestGlobalNodeRegistry 测试全局节点注册表
func TestGlobalNodeRegistry(t *testing.T) {
	// 测试v2.0.0核心节点是否已注册
	coreNodes := []string{
		"n8n-nodes-base.start",
		"n8n-nodes-base.end",
		"n8n-nodes-base.variable",
		"n8n-nodes-base.conditional",
		"n8n-nodes-base.pythonDocker",
		"n8n-nodes-base.domainResolve",
		"CUSTOM.customNode",
	}

	for _, nodeType := range coreNodes {
		node, err := GlobalNodeRegistry.GetNode(nodeType)
		if err != nil {
			t.Errorf("期望核心节点 '%s' 已注册, 但获取失败: %v", nodeType, err)
			continue
		}

		info := node.GetNodeInfo()
		if info.Type != nodeType {
			t.Errorf("期望节点类型为 '%s', 实际为 '%s'", nodeType, info.Type)
		}
	}

	// 测试兼容性节点别名
	compatNodes := []string{
		"n8n-nodes-base.pythonCode",
		"n8n-nodes-base.if",
		"n8n-nodes-base.custom",
	}

	for _, nodeType := range compatNodes {
		_, err := GlobalNodeRegistry.GetNode(nodeType)
		if err != nil {
			t.Errorf("期望兼容节点 '%s' 已注册, 但获取失败: %v", nodeType, err)
			continue
		}
		// 兼容节点的实际类型可能与别名不同，这是正常的
	}

	// 测试获取所有注册节点信息
	allNodes := GetAllRegisteredNodes()
	expectedMinCount := len(coreNodes) + len(compatNodes)
	if len(allNodes) < expectedMinCount {
		t.Errorf("期望至少有 %d 个节点, 实际有 %d 个", expectedMinCount, len(allNodes))
	}
}

// TestNodeExecutor 测试节点执行器
func TestNodeExecutor(t *testing.T) {
	executor := NewNodeExecutor()

	// 测试验证节点类型
	if !executor.ValidateNodeType("PYTHON.pythonCode") {
		t.Error("期望 'PYTHON.pythonCode' 是有效的节点类型")
	}

	if executor.ValidateNodeType("nonexistent.node") {
		t.Error("期望 'nonexistent.node' 不是有效的节点类型")
	}

	// 测试获取节点信息
	info, err := executor.GetNodeInfo("PYTHON.pythonCode")
	if err != nil {
		t.Fatalf("获取节点信息失败: %v", err)
	}

	if info.Type != "PYTHON.pythonCode" {
		t.Errorf("期望节点类型为 'PYTHON.pythonCode', 实际为 '%s'", info.Type)
	}

	// 测试列出所有节点
	allNodes := executor.ListAllNodes()
	if len(allNodes) == 0 {
		t.Error("期望至少有一个节点")
	}
}

// TestNodeFactoryFunctions 测试节点工厂函数
func TestNodeFactoryFunctions(t *testing.T) {
	// 测试Python Docker节点工厂
	pythonNode := NewPythonDockerNodeActivity()
	if pythonNode.GetNodeInfo().Type != "n8n-nodes-base.pythonDocker" {
		t.Errorf("期望Python Docker节点类型为 'n8n-nodes-base.pythonDocker', 实际为 '%s'", pythonNode.GetNodeInfo().Type)
	}

	// 测试域名解析节点工厂
	domainNode := NewDomainResolveActivity()
	if domainNode.GetNodeInfo().Type != "DNS.domainResolve" {
		t.Errorf("期望域名解析节点类型为 'DNS.domainResolve', 实际为 '%s'", domainNode.GetNodeInfo().Type)
	}

	// 测试统一条件节点工厂
	conditionNode := NewConditionalNodeActivity()
	if conditionNode.GetNodeInfo().Type != "n8n-nodes-base.conditional" {
		t.Errorf("期望统一条件节点类型为 'n8n-nodes-base.conditional', 实际为 '%s'", conditionNode.GetNodeInfo().Type)
	}

	// 测试自定义节点工厂
	customNode := NewCustomNodeActivity()
	if customNode.GetNodeInfo().Type != "CUSTOM.customNode" {
		t.Errorf("期望自定义节点类型为 'CUSTOM.customNode', 实际为 '%s'", customNode.GetNodeInfo().Type)
	}

	// 测试开始节点工厂
	startNode := NewStartNodeActivity()
	if startNode.GetNodeInfo().Type != "n8n-nodes-base.start" {
		t.Errorf("期望开始节点类型为 'n8n-nodes-base.start', 实际为 '%s'", startNode.GetNodeInfo().Type)
	}

	// 测试结束节点工厂
	endNode := NewEndNodeActivity()
	if endNode.GetNodeInfo().Type != "n8n-nodes-base.end" {
		t.Errorf("期望结束节点类型为 'n8n-nodes-base.end', 实际为 '%s'", endNode.GetNodeInfo().Type)
	}

	// 测试变量节点工厂
	variableNode := NewVariableNodeActivity()
	if variableNode.GetNodeInfo().Type != "n8n-nodes-base.variable" {
		t.Errorf("期望变量节点类型为 'n8n-nodes-base.variable', 实际为 '%s'", variableNode.GetNodeInfo().Type)
	}
}

// TestNodeAliases 测试节点别名
func TestNodeAliases(t *testing.T) {
	// 测试N8N原始节点类型别名
	n8nAliases := []string{
		"n8n-nodes-base.pythonCode", // 映射到pythonDocker
		"n8n-nodes-base.domainResolve",
		"n8n-nodes-base.if", // 映射到conditional
		"n8n-nodes-base.custom",
	}

	for _, alias := range n8nAliases {
		node, err := GlobalNodeRegistry.GetNode(alias)
		if err != nil {
			t.Errorf("期望别名 '%s' 已注册, 但获取失败: %v", alias, err)
			continue
		}

		info := node.GetNodeInfo()
		if info.ID == "" {
			t.Errorf("期望节点 '%s' 有有效的ID", alias)
		}
	}
}

// TestRegisterAllNodes 测试注册所有节点函数
func TestRegisterAllNodes(t *testing.T) {
	// 注册所有节点
	RegisterAllNodes()

	// 验证全局注册表包含预期的节点
	expectedNodes := []string{
		"PYTHON.pythonCode",
		"DNS.domainResolve",
		"LOGIC.conditionCheck",
		"LOGIC.switchNode",
		"CUSTOM.customNode",
	}

	actualNodes := GlobalNodeRegistry.GetAllNodeTypes()

	for _, expected := range expectedNodes {
		found := false
		for _, actual := range actualNodes {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("期望在注册表中找到节点 '%s'", expected)
		}
	}
}
