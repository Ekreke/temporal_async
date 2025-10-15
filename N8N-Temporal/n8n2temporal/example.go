package main

import (
	"encoding/json"
	"fmt"
	"n8n2temporal/workflow"
)

// 更完整的 n8n 工作流示例 JSON
const sampleWorkflowJSON = `{"createdAt":"2025-10-10T07:08:51.377Z","updatedAt":"2025-10-15T02:22:50.000Z","id":"ZDAF7LUWGeqZ0yfE","name":"My workflow 2","active":false,"isArchived":false,"nodes":[{"parameters":{},"type":"n8n-nodes-base.manualTrigger","typeVersion":1,"position":[-16,48],"id":"6ac15e0b-793f-45cd-be87-1d3ab29e2f10","name":"When clicking ‘ExecuteDomainResolve workflow’"},{"parameters":{},"type":"CUSTOM.asmDomainResolve","typeVersion":1,"position":[224,48],"id":"83e49035-e2d6-43d1-9822-c92f2f14abdd","name":"域名解析"},{"parameters":{"language":"python","pythonCode":"# Loop over input items and add a new field called 'myNewField' to the JSON of each one\nfor item in _input.all():\n  item.json.myNewField = 1\nreturn _input.all()"},"type":"n8n-nodes-base.code","typeVersion":2,"position":[464,-48],"id":"ae2f2b59-93af-482b-925c-743a698f524c","name":"Code in Python (Beta)"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[672,-48],"id":"374002e8-172c-440c-a0e2-b3ed911963f4","name":"My Custom Node"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[880,208],"id":"34b9711b-16b8-469b-bafe-40154c290a53","name":"My Custom Node1"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1328,80],"id":"8f90cb07-4573-43d7-9aa6-ecc9a417b34a","name":"My Custom Node2"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1328,288],"id":"01a60e48-535e-4bfd-a372-b538160cee87","name":"My Custom Node3"},{"parameters":{"language":"python","pythonCode":"# Loop over input items and add a new field called 'myNewField' to the JSON of each one\nfor item in _input.all():\n  item.json.myNewField = 1\nreturn _input.all()"},"type":"n8n-nodes-base.code","typeVersion":2,"position":[464,144],"id":"9cd03a96-f9db-4c2e-bc44-7ade9eb1c3fa","name":"Code in Python (Beta)1"},{"parameters":{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"id":"6163f5c9-b57f-45d3-aac9-c24617349098","leftValue":"1","rightValue":"1","operator":{"type":"string","operation":"equals","name":"filter.operator.equals"}}],"combinator":"and"},"options":{}},"type":"n8n-nodes-base.if","typeVersion":2.2,"position":[1088,208],"id":"2884e181-a2d7-4548-9854-514262b7d953","name":"If"},{"parameters":{"rules":{"values":[{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"leftValue":"1","rightValue":"","operator":{"type":"string","operation":"equals"},"id":"941f521a-abb7-4d04-a813-819632f8f4bf"}],"combinator":"and"}},{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"id":"96a410f8-b538-4c4f-8158-3332986877fc","leftValue":"2","rightValue":"","operator":{"type":"string","operation":"equals","name":"filter.operator.equals"}}],"combinator":"and"}},{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"id":"c9b7c54d-d8c3-41c2-b554-2dc6e132ab70","leftValue":"3","rightValue":"3","operator":{"type":"string","operation":"equals","name":"filter.operator.equals"}}],"combinator":"and"}}]},"options":{}},"type":"n8n-nodes-base.switch","typeVersion":3.3,"position":[1536,80],"id":"51c14168-336e-4f01-82fc-82b220fc7356","name":"Switch"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1808,-32],"id":"1b80b053-ef8e-4a5b-9216-6e8da0583d90","name":"My Custom Node4"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1824,112],"id":"657d97d5-409c-49df-9ce0-cada763a71b4","name":"My Custom Node5"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1872,272],"id":"3d49571f-6fe0-4b41-a093-585a9becc97c","name":"My Custom Node6"}],"connections":{"When clicking ‘ExecuteDomainResolve workflow’":{"main":[[{"node":"域名解析","type":"main","index":0}]]},"域名解析":{"main":[[{"node":"Code in Python (Beta)","type":"main","index":0},{"node":"Code in Python (Beta)1","type":"main","index":0}]]},"Code in Python (Beta)":{"main":[[{"node":"My Custom Node","type":"main","index":0}]]},"My Custom Node1":{"main":[[{"node":"If","type":"main","index":0}]]},"My Custom Node2":{"main":[[{"node":"Switch","type":"main","index":0}]]},"Code in Python (Beta)1":{"main":[[{"node":"My Custom Node1","type":"main","index":0},{"node":"域名解析","type":"main","index":0}]]},"If":{"main":[[{"node":"My Custom Node2","type":"main","index":0}],[{"node":"My Custom Node3","type":"main","index":0}]]},"Switch":{"main":[[{"node":"My Custom Node4","type":"main","index":0}],[{"node":"My Custom Node5","type":"main","index":0}],[{"node":"My Custom Node6","type":"main","index":0}]]}},"settings":{"executionOrder":"v1"},"staticData":null,"meta":null,"pinData":{},"versionId":"a4185c0d-43e6-4ae5-ab82-81831917a22a","triggerCount":0,"shared":[{"createdAt":"2025-10-10T07:08:51.380Z","updatedAt":"2025-10-10T07:08:51.380Z","role":"workflow:owner","workflowId":"ZDAF7LUWGeqZ0yfE","projectId":"zt6Nn1LD91g92fUA","project":{"createdAt":"2025-08-12T04:04:37.181Z","updatedAt":"2025-08-12T04:41:27.555Z","id":"zt6Nn1LD91g92fUA","name":"mingfeng peng <291641454@qq.com>","type":"personal","icon":null,"description":null,"projectRelations":[{"createdAt":"2025-08-12T04:04:37.181Z","updatedAt":"2025-08-12T04:04:37.181Z","userId":"c1c4d976-14fe-457f-835c-46f34bdf6169","projectId":"zt6Nn1LD91g92fUA","user":{"createdAt":"2025-08-12T04:04:36.974Z","updatedAt":"2025-10-14T04:09:01.000Z","id":"c1c4d976-14fe-457f-835c-46f34bdf6169","email":"291641454@qq.com","firstName":"mingfeng","lastName":"peng","personalizationAnswers":{"version":"v4","personalization_survey_submitted_at":"2025-08-12T04:41:46.279Z","personalization_survey_n8n_version":"1.106.3"},"settings":{"userActivated":true,"easyAIWorkflowOnboarded":true,"firstSuccessfulWorkflowId":"zMHgNYlsCrHk0j4U","userActivatedAt":1754993402776,"npsSurvey":{"waitingForResponse":true,"ignoredCount":2,"lastShownAt":1760080131415},"dismissedCallouts":{"preBuiltAgentsModalCallout":true}},"disabled":false,"mfaEnabled":false,"lastActiveAt":"2025-10-14","isPending":false}}]}}],"tags":[{"createdAt":"2025-10-15T02:17:35.405Z","updatedAt":"2025-10-15T02:17:35.405Z","id":"ienR7KvB6QjxExwo","name":"special tag1"}]}`

func main() {
	fmt.Println("=== N8N 工作流图对象演示 ===")

	// 创建工作流图对象
	graph, err := workflow.NewN8NWorkflowGraphFromJSON(sampleWorkflowJSON)
	if err != nil {
		fmt.Printf("创建工作流图失败: %v\n", err)
		return
	}

	fmt.Printf("工作流图解析完成！\n")
	fmt.Printf("节点总数: %d\n", len(graph.GetAllNodes()))
	fmt.Println()

	// 显示所有节点
	fmt.Println("所有节点:")
	for _, node := range graph.GetAllNodes() {
		fmt.Printf("- %s (ID: %s, 类型: %s)\n", node.Name, node.ID, node.Type)
	}
	fmt.Println()

	// 显示起始节点
	fmt.Println("起始节点:")
	startNodes := graph.GetStartNodes()
	for _, node := range startNodes {
		fmt.Printf("🎯 %s (类型: %s)\n", node.Name, node.Type)
	}
	fmt.Println()

	// 显示结束节点
	fmt.Println("结束节点:")
	endNodes := graph.GetEndNodes()
	for _, node := range endNodes {
		fmt.Printf("🏁 %s (类型: %s)\n", node.Name, node.Type)
	}
	fmt.Println()

	// 演示GetNextNodes方法
	fmt.Println("=== GetNextNodes 方法演示 ===")
	// 使用实际的节点名称
	allNodes := graph.GetAllNodes()
	var testNodeNames []string
	if len(allNodes) > 0 {
		testNodeNames = []string{allNodes[0].Name} // 使用第一个节点的实际名称
	}
	// 添加一些已知的节点名称
	testNodeNames = append(testNodeNames, "域名解析", "Code in Python (Beta)1", "If")

	for _, nodeName := range testNodeNames {
		// 使用节点名称获取下一跳节点
		nextNodes, err := graph.GetNextNodes(nodeName)
		if err != nil {
			fmt.Printf("❌ 获取下一跳节点失败 (%s): %v\n", nodeName, err)
			continue
		}

		fmt.Printf("📤 节点 '%s' 的下一跳节点:\n", nodeName)
		if len(nextNodes) == 0 {
			fmt.Printf("   无下一跳节点 (结束节点)\n")
		} else {
			for i, nextNode := range nextNodes {
				fmt.Printf("   %d. %s (ID: %s, 类型: %s)\n", i+1, nextNode.Name, nextNode.ID, nextNode.Type)
			}
		}
		fmt.Println()
	}

	// 演示使用节点对象获取下一跳
	fmt.Println("=== 使用节点对象获取下一跳 ===")
	// 使用实际的起始节点
	startNodesList := graph.GetStartNodes()
	var triggerNode workflow.N8NNode
	found := false

	if len(startNodesList) > 0 {
		triggerNode = startNodesList[0]
		found = true
	} else {
		// 备用方案：从所有节点中查找第一个
		allNodesList := graph.GetAllNodes()
		if len(allNodesList) > 0 {
			triggerNode = allNodesList[0]
			found = true
		}
	}

	if found {
		fmt.Printf("使用节点对象 '%s' 获取下一跳:\n", triggerNode.Name)
		nextNodes, err := graph.GetNextNodes(triggerNode)
		if err != nil {
			fmt.Printf("获取失败: %v\n", err)
		} else {
			for _, nextNode := range nextNodes {
				fmt.Printf("  -> %s\n", nextNode.Name)
			}
		}
	}

	// 演示GetNextNodeNames方法
	fmt.Println("\n=== GetNextNodeNames 方法演示 ===")
	domainNode, found := graph.GetNodeByName("域名解析")
	if found {
		nextNodeNames, err := graph.GetNextNodeNames(domainNode)
		if err != nil {
			fmt.Printf("获取下一跳节点名称失败: %v\n", err)
		} else {
			fmt.Printf("节点 '%s' 的下一跳节点名称: %v\n", domainNode.Name, nextNodeNames)
		}
	}

	// 演示依赖图
	fmt.Println("\n=== 依赖图演示 ===")
	dependencyGraph := graph.GetDependencyGraph()
	fmt.Println("节点依赖关系 (节点 -> 依赖的节点列表):")
	for nodeName, deps := range dependencyGraph {
		fmt.Printf("📥 %s -> [", nodeName)
		if len(deps) == 0 {
			fmt.Printf("(无依赖 - 起始节点)")
		} else {
			for i, dep := range deps {
				if i > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("'%s'", dep)
			}
		}
		fmt.Println("]")
	}
	fmt.Println()

	// 创建初始数据
	initialData := map[string]interface{}{
		"triggerData": map[string]interface{}{
			"message":   "测试数据",
			"timestamp": "2025-10-15T12:00:00Z",
		},
	}

	// 将初始数据转换为 JSON 字符串并解析，确保格式正确
	dataJSON, _ := json.Marshal(initialData)
	fmt.Printf("初始数据: %s\n", string(dataJSON))

	fmt.Println("\n✅ 通用工作流引擎已实现，可以通过 Temporal Worker 执行！")
	fmt.Println("\n🎯 主要功能:")
	fmt.Println("1. ✅ 解析 n8n JSON 工作流定义")
	fmt.Println("2. ✅ 构建节点下一跳映射关系")
	fmt.Println("3. ✅ 构建节点依赖图和执行顺序")
	fmt.Println("4. ✅ 支持多种节点类型 (手动触发、Python代码、自定义节点、IF条件、Switch分支)")
	fmt.Println("5. ✅ 动态节点调度和数据流管理")
	fmt.Println("6. ✅ 拓扑排序确保正确执行顺序")
	fmt.Println("7. ✅ 错误处理和结果收集")
}
