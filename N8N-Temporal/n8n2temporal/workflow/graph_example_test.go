package workflow

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/client"
	"log"
	"testing"
)

// TestN8NWorkflowGraph 测试N8NWorkflowGraph的基本功能
func TestN8NWorkflowGraph(t *testing.T) {
	// 创建一个简单的n8n工作流JSON示例
	workflowJSON := `{
		"id": "test-workflow",
		"name": "测试工作流",
		"nodes": [
			{
				"id": "node1",
				"name": "开始",
				"type": "n8n-nodes-base.manualTrigger",
				"position": [100, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node2",
				"name": "域名解析",
				"type": "custom.domainResolve",
				"position": [300, 100],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node3",
				"name": "Python代码1",
				"type": "n8n-nodes-base.code",
				"position": [500, 50],
				"parameters": {},
				"typeVersion": 1
			},
			{
				"id": "node4",
				"name": "Python代码2",
				"type": "n8n-nodes-base.code",
				"position": [500, 150],
				"parameters": {},
				"typeVersion": 1
			}
		],
		"connections": {
			"开始": {
				"main": [
					[
						{"node": "域名解析"}
					]
				]
			},
			"域名解析": {
				"main": [
					[
						{"node": "Python代码1"},
						{"node": "Python代码2"}
					]
				]
			}
		}
	}`

	// 使用便捷函数创建图对象
	graph, err := NewN8NWorkflowGraphFromJSON(workflowJSON)
	if err != nil {
		t.Fatalf("创建图对象失败: %v", err)
	}

	// 测试获取所有节点
	allNodes := graph.GetAllNodes()
	if len(allNodes) != 4 {
		t.Errorf("期望4个节点，实际得到%d个", len(allNodes))
	}

	// 测试获取起始节点
	startNodes := graph.GetStartNodes()
	if len(startNodes) != 1 {
		t.Errorf("期望1个起始节点，实际得到%d个", len(startNodes))
	}
	if startNodes[0].Name != "开始" {
		t.Errorf("期望起始节点名称为'开始'，实际为'%s'", startNodes[0].Name)
	}

	// 测试获取下一跳节点 - 使用节点对象
	nextNodes, err := graph.GetNextNodes(startNodes[0])
	if err != nil {
		t.Fatalf("获取下一跳节点失败: %v", err)
	}
	if len(nextNodes) != 1 {
		t.Errorf("期望1个下一跳节点，实际得到%d个", len(nextNodes))
	}
	if nextNodes[0].Name != "域名解析" {
		t.Errorf("期望下一跳节点名称为'域名解析'，实际为'%s'", nextNodes[0].Name)
	}

	// 测试获取下一跳节点 - 使用节点名称
	nextNodes2, err := graph.GetNextNodes("域名解析")
	if err != nil {
		t.Fatalf("获取下一跳节点失败: %v", err)
	}
	if len(nextNodes2) != 2 {
		t.Errorf("期望2个下一跳节点，实际得到%d个", len(nextNodes2))
	}

	// 测试获取下一跳节点名称
	nextNodeNames, err := graph.GetNextNodeNames("域名解析")
	if err != nil {
		t.Fatalf("获取下一跳节点名称失败: %v", err)
	}
	if len(nextNodeNames) != 2 {
		t.Errorf("期望2个下一跳节点名称，实际得到%d个", len(nextNodeNames))
	}

	// 测试根据名称获取节点
	node, found := graph.GetNodeByName("Python代码1")
	if !found {
		t.Error("未找到节点'Python代码1'")
	}
	if node.Type != "n8n-nodes-base.code" {
		t.Errorf("期望节点类型为'n8n-nodes-base.code'，实际为'%s'", node.Type)
	}

	// 测试根据ID获取节点
	nodeByID, found := graph.GetNodeByID("node3")
	if !found {
		t.Error("未找到ID为'node3'的节点")
	}
	if nodeByID.Name != "Python代码1" {
		t.Errorf("期望节点名称为'Python代码1'，实际为'%s'", nodeByID.Name)
	}

	// 测试获取结束节点
	endNodes := graph.GetEndNodes()
	// 在我们的测试工作流中，Python代码1和Python代码2都是结束节点
	if len(endNodes) < 1 {
		t.Errorf("期望至少1个结束节点，实际得到%d个", len(endNodes))
	}

	// 打印图结构信息（用于调试）
	fmt.Printf("图结构测试完成:\n")
	fmt.Printf("- 总节点数: %d\n", len(allNodes))
	fmt.Printf("- 起始节点: %s\n", startNodes[0].Name)
	fmt.Printf("- 结束节点数: %d\n", len(endNodes))
	for _, endNode := range endNodes {
		fmt.Printf("  - %s\n", endNode.Name)
	}
}

// ExampleN8NWorkflowGraph 演示N8NWorkflowGraph的完整用法
func ExampleN8NWorkflowGraph() {
	// 这里应该是一个实际的n8n工作流JSON
	workflowJSON := `{"id": "example", "name": "示例", "nodes": [], "connections": {}}`

	graph, err := NewN8NWorkflowGraphFromJSON(workflowJSON)
	if err != nil {
		fmt.Printf("创建图对象失败: %v\n", err)
		return
	}

	fmt.Printf("工作流图创建成功，节点数量: %d\n", len(graph.GetAllNodes()))

	// 获取起始节点并遍历
	startNodes := graph.GetStartNodes()
	for _, startNode := range startNodes {
		fmt.Printf("起始节点: %s\n", startNode.Name)

		nextNodes, err := graph.GetNextNodes(startNode)
		if err != nil {
			fmt.Printf("获取下一跳节点失败: %v\n", err)
			continue
		}

		for _, nextNode := range nextNodes {
			fmt.Printf("  -> %s (类型: %s)\n", nextNode.Name, nextNode.Type)
		}
	}
}

func TestGenericWorkflow(t *testing.T) {
	// 参数初始化
	//const sampleWorkflowJSON = `{"createdAt":"2025-10-10T07:08:51.377Z","updatedAt":"2025-10-15T02:22:50.000Z","id":"ZDAF7LUWGeqZ0yfE","name":"My workflow 2","active":false,"isArchived":false,"nodes":[{"parameters":{},"type":"n8n-nodes-base.manualTrigger","typeVersion":1,"position":[-16,48],"id":"6ac15e0b-793f-45cd-be87-1d3ab29e2f10","name":"When clicking ‘ExecuteDomainResolve workflow’"},{"parameters":{},"type":"CUSTOM.asmDomainResolve","typeVersion":1,"position":[224,48],"id":"83e49035-e2d6-43d1-9822-c92f2f14abdd","name":"域名解析"},{"parameters":{"language":"python","pythonCode":"# Loop over input items and add a new field called 'myNewField' to the JSON of each one\nfor item in _input.all():\n  item.json.myNewField = 1\nreturn _input.all()"},"type":"n8n-nodes-base.code","typeVersion":2,"position":[464,-48],"id":"ae2f2b59-93af-482b-925c-743a698f524c","name":"Code in Python (Beta)"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[672,-48],"id":"374002e8-172c-440c-a0e2-b3ed911963f4","name":"My Custom Node"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[880,208],"id":"34b9711b-16b8-469b-bafe-40154c290a53","name":"My Custom Node1"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1328,80],"id":"8f90cb07-4573-43d7-9aa6-ecc9a417b34a","name":"My Custom Node2"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1328,288],"id":"01a60e48-535e-4bfd-a372-b538160cee87","name":"My Custom Node3"},{"parameters":{"language":"python","pythonCode":"# Loop over input items and add a new field called 'myNewField' to the JSON of each one\nfor item in _input.all():\n  item.json.myNewField = 1\nreturn _input.all()"},"type":"n8n-nodes-base.code","typeVersion":2,"position":[464,144],"id":"9cd03a96-f9db-4c2e-bc44-7ade9eb1c3fa","name":"Code in Python (Beta)1"},{"parameters":{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"id":"6163f5c9-b57f-45d3-aac9-c24617349098","leftValue":"1","rightValue":"1","operator":{"type":"string","operation":"equals","name":"filter.operator.equals"}}],"combinator":"and"},"options":{}},"type":"n8n-nodes-base.if","typeVersion":2.2,"position":[1088,208],"id":"2884e181-a2d7-4548-9854-514262b7d953","name":"If"},{"parameters":{"rules":{"values":[{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"leftValue":"1","rightValue":"","operator":{"type":"string","operation":"equals"},"id":"941f521a-abb7-4d04-a813-819632f8f4bf"}],"combinator":"and"}},{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"id":"96a410f8-b538-4c4f-8158-3332986877fc","leftValue":"2","rightValue":"","operator":{"type":"string","operation":"equals","name":"filter.operator.equals"}}],"combinator":"and"}},{"conditions":{"options":{"caseSensitive":true,"leftValue":"","typeValidation":"strict","version":2},"conditions":[{"id":"c9b7c54d-d8c3-41c2-b554-2dc6e132ab70","leftValue":"3","rightValue":"3","operator":{"type":"string","operation":"equals","name":"filter.operator.equals"}}],"combinator":"and"}}]},"options":{}},"type":"n8n-nodes-base.switch","typeVersion":3.3,"position":[1536,80],"id":"51c14168-336e-4f01-82fc-82b220fc7356","name":"Switch"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1808,-32],"id":"1b80b053-ef8e-4a5b-9216-6e8da0583d90","name":"My Custom Node4"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1824,112],"id":"657d97d5-409c-49df-9ce0-cada763a71b4","name":"My Custom Node5"},{"parameters":{},"type":"CUSTOM.myCustomNode","typeVersion":1,"position":[1872,272],"id":"3d49571f-6fe0-4b41-a093-585a9becc97c","name":"My Custom Node6"}],"connections":{"When clicking ‘ExecuteDomainResolve workflow’":{"main":[[{"node":"域名解析","type":"main","index":0}]]},"域名解析":{"main":[[{"node":"Code in Python (Beta)","type":"main","index":0},{"node":"Code in Python (Beta)1","type":"main","index":0}]]},"Code in Python (Beta)":{"main":[[{"node":"My Custom Node","type":"main","index":0}]]},"My Custom Node1":{"main":[[{"node":"If","type":"main","index":0}]]},"My Custom Node2":{"main":[[{"node":"Switch","type":"main","index":0}]]},"Code in Python (Beta)1":{"main":[[{"node":"My Custom Node1","type":"main","index":0},{"node":"域名解析","type":"main","index":0}]]},"If":{"main":[[{"node":"My Custom Node2","type":"main","index":0}],[{"node":"My Custom Node3","type":"main","index":0}]]},"Switch":{"main":[[{"node":"My Custom Node4","type":"main","index":0}],[{"node":"My Custom Node5","type":"main","index":0}],[{"node":"My Custom Node6","type":"main","index":0}]]}},"settings":{"executionOrder":"v1"},"staticData":null,"meta":null,"pinData":{},"versionId":"a4185c0d-43e6-4ae5-ab82-81831917a22a","triggerCount":0,"shared":[{"createdAt":"2025-10-10T07:08:51.380Z","updatedAt":"2025-10-10T07:08:51.380Z","role":"workflow:owner","workflowId":"ZDAF7LUWGeqZ0yfE","projectId":"zt6Nn1LD91g92fUA","project":{"createdAt":"2025-08-12T04:04:37.181Z","updatedAt":"2025-08-12T04:41:27.555Z","id":"zt6Nn1LD91g92fUA","name":"mingfeng peng <291641454@qq.com>","type":"personal","icon":null,"description":null,"projectRelations":[{"createdAt":"2025-08-12T04:04:37.181Z","updatedAt":"2025-08-12T04:04:37.181Z","userId":"c1c4d976-14fe-457f-835c-46f34bdf6169","projectId":"zt6Nn1LD91g92fUA","user":{"createdAt":"2025-08-12T04:04:36.974Z","updatedAt":"2025-10-14T04:09:01.000Z","id":"c1c4d976-14fe-457f-835c-46f34bdf6169","email":"291641454@qq.com","firstName":"mingfeng","lastName":"peng","personalizationAnswers":{"version":"v4","personalization_survey_submitted_at":"2025-08-12T04:41:46.279Z","personalization_survey_n8n_version":"1.106.3"},"settings":{"userActivated":true,"easyAIWorkflowOnboarded":true,"firstSuccessfulWorkflowId":"zMHgNYlsCrHk0j4U","userActivatedAt":1754993402776,"npsSurvey":{"waitingForResponse":true,"ignoredCount":2,"lastShownAt":1760080131415},"dismissedCallouts":{"preBuiltAgentsModalCallout":true}},"disabled":false,"mfaEnabled":false,"lastActiveAt":"2025-10-14","isPending":false}}]}}],"tags":[{"createdAt":"2025-10-15T02:17:35.405Z","updatedAt":"2025-10-15T02:17:35.405Z","id":"ienR7KvB6QjxExwo","name":"special tag1"}]}`
	//const sampleWorkflowJSON = `{"createdAt": "2025-10-21T02:46:16.649Z", "updatedAt": "2025-10-22T02:34:13.000Z", "id": "ciQFzLxpGwEMwAoH", "name": "My workflow 3", "active": false, "isArchived": false, "nodes": [{"parameters": {"domain": "www.baidu.com"}, "type": "CUSTOM.asm_domain_resolve", "typeVersion": 1, "position": [-560, -336], "id": "5472dcaf-1957-4972-be0f-a25970887983", "name": "ASM Domain Resolve"}, {"parameters": {}, "type": "CUSTOM.asm_domain_brute", "typeVersion": 1, "position": [-128, -336], "id": "1240ad39-70ec-4fde-be45-05413119c02f", "name": "ASM Domain Brute"}, {"parameters": {"name": "={{ $('ASM Domain Resolve').item.json.domain }}"}, "type": "CUSTOM.asm_whois", "typeVersion": 1, "position": [112, -336], "id": "aa6ee402-a788-4eeb-a039-700bef93562b", "name": "ASM WHOIS1"}, {"parameters": {"website_link": "={{ $('ASM Domain Resolve').item.json.domain }}"}, "type": "CUSTOM.asm_spider", "typeVersion": 1, "position": [352, -336], "id": "36a047c8-19c7-466f-99e3-41b93fee1614", "name": "ASM Spider"}, {"parameters": {}, "type": "CUSTOM.asm_poc", "typeVersion": 1, "position": [544, -336], "id": "d94be65c-533e-4cef-be2f-c985d821ca00", "name": "ASM POC"}, {"parameters": {}, "type": "CUSTOM.asm_dir_brute", "typeVersion": 1, "position": [752, -336], "id": "f5a26d00-e95d-4996-a764-5cc8075f80a9", "name": "ASM Dir Brute"}, {"parameters": {}, "type": "CUSTOM.asm_service_probe", "typeVersion": 1, "position": [960, -336], "id": "2438dae7-6480-490b-90fd-f85ae8e67120", "name": "ASM Service Probe"}, {"parameters": {}, "type": "CUSTOM.asm_fingerprint", "typeVersion": 1, "position": [1168, -336], "id": "2e77509f-344c-4cf9-bc8e-3f58239f520a", "name": "ASM Fingerprint"}, {"parameters": {"language": "python", "pythonCode": "# Loop over input items and add a new field called 'myNewField' to the JSON of each one\nfor item in _input.all():\n  item.json.myNewField = 1\nreturn _input.all()"}, "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [-352, -336], "id": "bdfe9257-92dc-4f5c-876c-70e598ac99f7", "name": "Code in Python (Beta)"}], "connections": {"ASM Domain Resolve": {"main": [[{"node": "Code in Python (Beta)", "type": "main", "index": 0}]]}, "ASM Domain Brute": {"main": [[{"node": "ASM WHOIS1", "type": "main", "index": 0}]]}, "ASM WHOIS1": {"main": [[{"node": "ASM Spider", "type": "main", "index": 0}]]}, "ASM Spider": {"main": [[{"node": "ASM POC", "type": "main", "index": 0}]]}, "ASM POC": {"main": [[{"node": "ASM Dir Brute", "type": "main", "index": 0}]]}, "ASM Dir Brute": {"main": [[{"node": "ASM Service Probe", "type": "main", "index": 0}]]}, "ASM Service Probe": {"main": [[{"node": "ASM Fingerprint", "type": "main", "index": 0}]]}, "Code in Python (Beta)": {"main": [[{"node": "ASM Domain Brute", "type": "main", "index": 0}]]}}, "settings": {"executionOrder": "v1"}, "staticData": null, "meta": null, "pinData": {}, "versionId": "1a9a26a4-9a45-4152-a401-7a24889f596a", "triggerCount": 0, "shared": [{"createdAt": "2025-10-21T02:46:16.657Z", "updatedAt": "2025-10-21T02:46:16.657Z", "role": "workflow:owner", "workflowId": "ciQFzLxpGwEMwAoH", "projectId": "zt6Nn1LD91g92fUA", "project": {"createdAt": "2025-08-12T04:04:37.181Z", "updatedAt": "2025-08-12T04:41:27.555Z", "id": "zt6Nn1LD91g92fUA", "name": "mingfeng peng <291641454@qq.com>", "type": "personal", "icon": null, "description": null, "projectRelations": [{"createdAt": "2025-08-12T04:04:37.181Z", "updatedAt": "2025-08-12T04:04:37.181Z", "userId": "c1c4d976-14fe-457f-835c-46f34bdf6169", "projectId": "zt6Nn1LD91g92fUA", "user": {"createdAt": "2025-08-12T04:04:36.974Z", "updatedAt": "2025-10-21T09:47:54.000Z", "id": "c1c4d976-14fe-457f-835c-46f34bdf6169", "email": "291641454@qq.com", "firstName": "mingfeng", "lastName": "peng", "personalizationAnswers": {"version": "v4", "personalization_survey_submitted_at": "2025-08-12T04:41:46.279Z", "personalization_survey_n8n_version": "1.106.3"}, "settings": {"userActivated": true, "easyAIWorkflowOnboarded": true, "firstSuccessfulWorkflowId": "zMHgNYlsCrHk0j4U", "userActivatedAt": 1754993402776, "npsSurvey": {"responded": true, "lastShownAt": 1761035621148}, "dismissedCallouts": {"preBuiltAgentsModalCallout": true}}, "disabled": false, "mfaEnabled": false, "lastActiveAt": "2025-10-21", "isPending": false}}]}}], "tags": []}`
	const sampleWorkflowJSON = `{"id":"complete-n8n-workflow-v2","name":"完整节点演示工作流 v2.0.0","active":false,"isArchived":false,"nodes":[{"id":"start-node","name":"开始节点","type":"n8n-nodes-base.start","typeVersion":1,"position":[240,100],"parameters":{"globalParameters":{"python-node":{"timeout":30,"requirements":["requests","pandas"],"dockerImage":"python:3.11-slim"},"conditional-node":{"mode":"strict","evaluateMode":"first"},"variable-node":{"overwriteMode":"overwrite","scope":"global"}}}},{"id":"variable-node","name":"变量节点","type":"n8n-nodes-base.variable","typeVersion":1,"position":[460,100],"parameters":{"operation":"set","variables":{"user_id":"12345","session_token":"abc123xyz","request_count":0,"processing_flags":{"is_premium_user":true,"enable_logging":true,"max_retries":3}},"overwriteMode":"overwrite","variablesScope":"global"}},{"id":"domain-resolve","name":"域名解析","type":"n8n-nodes-base.domainResolve","typeVersion":1,"position":[680,50],"parameters":{"domain":"example.com","recordTypes":["A","AAAA","MX","TXT"]}},{"id":"conditional-node","name":"条件判断节点","type":"n8n-nodes-base.conditional","typeVersion":1,"position":[900,100],"parameters":{"nodeType":"if","conditions":[{"id":"premium-user-check","name":"高级用户检查","outputPath":"premium-branch","enabled":true,"logicOperator":"AND","conditions":[{"leftValue":"$var.processing_flags.is_premium_user","operator":"equals","rightValue":true,"caseSensitive":false},{"leftValue":"$var.request_count","operator":"less_than","rightValue":100}]},{"id":"regular-user-check","name":"普通用户检查","outputPath":"regular-branch","enabled":true,"logicOperator":"OR","conditions":[{"leftValue":"$var.user_id","operator":"not_empty","rightValue":""},{"leftValue":"$var.session_token","operator":"not_empty","rightValue":""}]}],"defaultBranch":"invalid-user-branch"}},{"id":"python-premium","name":"Python处理（高级用户）","type":"n8n-nodes-base.pythonDocker","typeVersion":1,"position":[1120,50],"parameters":{"code":"import time\nimport json\nfrom datetime import datetime\n\n# 获取输入数据\nuser_id = input_data.get('user_id', 'unknown')\nsession_token = input_data.get('session_token', '')\nrequest_count = input_data.get('request_count', 0)\nflags = input_data.get('processing_flags', {})\n\n# 高级用户处理逻辑\nprocessing_result = {\n    'user_id': user_id,\n    'processed_at': datetime.now().isoformat(),\n    'processing_type': 'premium',\n    'features_enabled': ['advanced_analytics', 'priority_processing', 'extended_limits'],\n    'next_request_count': request_count + 1,\n    'processing_time_ms': 50,\n    'status': 'success'\n}\n\n# 设置返回结果\nresult = processing_result\nprint(f'Premium user {user_id} processed successfully')","dockerImage":"python:3.11-slim","timeoutSeconds":60,"maxRetries":3,"requirements":["pandas","numpy","requests"],"environmentVars":{"LOG_LEVEL":"INFO","ENVIRONMENT":"production"}}},{"id":"python-regular","name":"Python处理（普通用户）","type":"n8n-nodes-base.pythonDocker","typeVersion":1,"position":[1120,150],"parameters":{"code":"import time\nfrom datetime import datetime\n\n# 获取输入数据\nuser_id = input_data.get('user_id', 'unknown')\nsession_token = input_data.get('session_token', '')\nrequest_count = input_data.get('request_count', 0)\nflags = input_data.get('processing_flags', {})\n\n# 普通用户处理逻辑\nprocessing_result = {\n    'user_id': user_id,\n    'processed_at': datetime.now().isoformat(),\n    'processing_type': 'regular',\n    'features_enabled': ['basic_processing'],\n    'next_request_count': request_count + 1,\n    'processing_time_ms': 100,\n    'status': 'success'\n}\n\n# 设置返回结果\nresult = processing_result\nprint(f'Regular user {user_id} processed successfully')","dockerImage":"python:3.11-slim","timeoutSeconds":45,"maxRetries":2,"requirements":["requests"],"environmentVars":{"LOG_LEVEL":"INFO"}}},{"id":"custom-handler","name":"自定义处理器","type":"CUSTOM.customNode","typeVersion":1,"position":[1340,100],"parameters":{"customLogic":"data_aggregation","outputFormat":"structured","enableMetrics":true}},{"id":"variable-update","name":"变量更新节点","type":"n8n-nodes-base.variable","typeVersion":1,"position":[1560,100],"parameters":{"operation":"set","variables":{"last_processed_user":"{{ $json.user_id }}","processing_complete":true,"final_status":"{{ $json.status }}","workflow_execution_time":"{{ $now }}"},"overwriteMode":"merge","variablesScope":"global"}},{"id":"end-node","name":"结束节点","type":"n8n-nodes-base.end","typeVersion":1,"position":[1780,100],"parameters":{"resultMode":"all","includeMetadata":true,"formatOutput":true}}],"connections":{"开始节点":{"main":[[{"node":"变量节点","type":"main","index":0},{"node":"域名解析","type":"main","index":0}]]},"变量节点":{"main":[[{"node":"条件判断节点","type":"main","index":0}]]},"域名解析":{"main":[[{"node":"条件判断节点","type":"main","index":0}]]},"条件判断节点":{"premium-branch":[[{"node":"Python处理（高级用户）","type":"main","index":0}]],"regular-branch":[[{"node":"Python处理（普通用户）","type":"main","index":0}]],"invalid-user-branch":[[{"node":"结束节点","type":"main","index":0}]]},"Python处理（高级用户）":{"main":[[{"node":"自定义处理器","type":"main","index":0}]]},"Python处理（普通用户）":{"main":[[{"node":"自定义处理器","type":"main","index":0}]]},"自定义处理器":{"main":[[{"node":"变量更新节点","type":"main","index":0}]]},"变量更新节点":{"main":[[{"node":"结束节点","type":"main","index":0}]]}},"settings":{"executionOrder":"v1"},"staticData":null,"meta":{"templateCredsSetupCompleted":true},"pinData":{},"versionId":"v2.0.0","triggerCount":0,"tags":[{"createdAt":"2024-01-01T00:00:00.000Z","updatedAt":"2024-01-01T00:00:00.000Z","id":"demo-workflow-tag","name":"完整工作流演示"}]}`
	var initData = make(map[string]interface{})
	initData["triggerData"] = map[string]interface{}{"message": "测试数据", "timestamp": "2025-10-15T12:00:00Z"}
	// 连接
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()
	// 解析初始输入值
	options := client.StartWorkflowOptions{
		ID:        "generic-workflow1",
		TaskQueue: "n8n-conversion-queue-new",
	}
	we, err := c.ExecuteWorkflow(context.Background(), options, GenericWorkflowWithMaxStep, sampleWorkflowJSON, initData, 1000)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	var result interface{}
	err = we.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow result:", result)
}
