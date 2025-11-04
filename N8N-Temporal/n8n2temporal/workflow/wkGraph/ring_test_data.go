package wkGraph

import (
	"encoding/json"
)

// 生成各种测试场景的测试数据

// GetLinearWorkflowData 获取线性工作流数据（无环）
func GetLinearWorkflowData() string {
	return `{
		"id": "linear-workflow",
		"name": "线性工作流",
		"active": false,
		"nodes": [
			{
				"id": "start-node",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"type_version": 1,
				"version": 1,
				"position": [240, 100],
				"parameters": {}
			},
			{
				"id": "process-node",
				"name": "处理节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 100],
				"parameters": {"code": "print('processing')"}
			},
			{
				"id": "end-node",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"type_version": 1,
				"version": 1,
				"position": [680, 100],
				"parameters": {}
			}
		],
		"connections": {
			"开始节点": {
				"main": [[{"node": "处理节点", "type": "main", "index": 0}]]
			},
			"处理节点": {
				"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
			}
		}
	}`
}

// GetSimpleRingWorkflowData 获取简单环工作流数据（单环，无条件节点）
func GetSimpleRingWorkflowData() string {
	return `{
		"id": "simple-ring-workflow",
		"name": "简单环工作流",
		"active": false,
		"nodes": [
			{
				"id": "start-node",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"type_version": 1,
				"version": 1,
				"position": [240, 100],
				"parameters": {}
			},
			{
				"id": "process-node",
				"name": "处理节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 100],
				"parameters": {"code": "print('processing')"}
			},
			{
				"id": "check-node",
				"name": "检查节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [680, 100],
				"parameters": {"code": "print('checking')"}
			},
			{
				"id": "end-node",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"type_version": 1,
				"version": 1,
				"position": [900, 100],
				"parameters": {}
			}
		],
		"connections": {
			"开始节点": {
				"main": [[{"node": "处理节点", "type": "main", "index": 0}]]
			},
			"处理节点": {
				"main": [[{"node": "检查节点", "type": "main", "index": 0}]]
			},
			"检查节点": {
				"main": [[{"node": "处理节点", "type": "main", "index": 0}]]
			}
		}
	}`
}

// GetConditionalRingWorkflowData 获取包含条件节点的环工作流数据（符合条件的环）
func GetConditionalRingWorkflowData() string {
	return `{
		"id": "conditional-ring-workflow",
		"name": "条件环工作流",
		"active": false,
		"nodes": [
			{
				"id": "start-node",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"type_version": 1,
				"version": 1,
				"position": [240, 100],
				"parameters": {}
			},
			{
				"id": "process-node",
				"name": "处理节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 100],
				"parameters": {"code": "print('processing')"}
			},
			{
				"id": "conditional-node",
				"name": "条件节点",
				"type": "n8n-nodes-base.conditional",
				"type_version": 1,
				"version": 1,
				"position": [680, 100],
				"parameters": {
					"conditions": [
						{
							"id": "check-condition",
							"name": "检查条件",
							"outputPath": "continue-branch",
							"enabled": true,
							"conditions": [
								{
									"leftValue": "$var.should_continue",
									"operator": "equals",
									"rightValue": true
								}
							]
						}
					],
					"defaultBranch": "exit-branch"
				}
			},
			{
				"id": "exit-node",
				"name": "出口节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [900, 50],
				"parameters": {"code": "print('exiting loop')"}
			},
			{
				"id": "end-node",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"type_version": 1,
				"version": 1,
				"position": [1120, 100],
				"parameters": {}
			}
		],
		"connections": {
			"开始节点": {
				"main": [[{"node": "处理节点", "type": "main", "index": 0}]]
			},
			"处理节点": {
				"main": [[{"node": "条件节点", "type": "main", "index": 0}]]
			},
			"条件节点": {
				"continue-branch": [[{"node": "处理节点", "type": "main", "index": 0}]],
				"exit-branch": [[{"node": "出口节点", "type": "main", "index": 0}]]
			},
			"出口节点": {
				"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
			}
		}
	}`
}

// GetMultiRingWorkflowData 获取多环工作流数据
func GetMultiRingWorkflowData() string {
	return `{
		"id": "multi-ring-workflow",
		"name": "多环工作流",
		"active": false,
		"nodes": [
			{
				"id": "start-node",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"type_version": 1,
				"version": 1,
				"position": [240, 100],
				"parameters": {}
			},
			{
				"id": "process-a",
				"name": "处理节点A",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 50],
				"parameters": {"code": "print('processing A')"}
			},
			{
				"id": "process-b",
				"name": "处理节点B",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 150],
				"parameters": {"code": "print('processing B')"}
			},
			{
				"id": "check-a",
				"name": "检查节点A",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [680, 50],
				"parameters": {"code": "print('checking A')"}
			},
			{
				"id": "check-b",
				"name": "检查节点B",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [680, 150],
				"parameters": {"code": "print('checking B')"}
			},
			{
				"id": "merge-node",
				"name": "合并节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [900, 100],
				"parameters": {"code": "print('merging')"}
			},
			{
				"id": "end-node",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"type_version": 1,
				"version": 1,
				"position": [1120, 100],
				"parameters": {}
			}
		],
		"connections": {
			"开始节点": {
				"main": [
					[{"node": "处理节点A", "type": "main", "index": 0}],
					[{"node": "处理节点B", "type": "main", "index": 0}]
				]
			},
			"处理节点A": {
				"main": [[{"node": "检查节点A", "type": "main", "index": 0}]]
			},
			"处理节点B": {
				"main": [[{"node": "检查节点B", "type": "main", "index": 0}]]
			},
			"检查节点A": {
				"main": [[{"node": "处理节点A", "type": "main", "index": 0}]]
			},
			"检查节点B": {
				"main": [[{"node": "处理节点B", "type": "main", "index": 0}]]
			},
			"合并节点": {
				"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
			}
		}
	}`
}

// GetNestedRingWorkflowData 获取嵌套环工作流数据（大环中包含小环）
func GetNestedRingWorkflowData() string {
	return `{
		"id": "nested-ring-workflow",
		"name": "嵌套环工作流",
		"active": false,
		"nodes": [
			{
				"id": "start-node",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"type_version": 1,
				"version": 1,
				"position": [240, 100],
				"parameters": {}
			},
			{
				"id": "outer-process-1",
				"name": "外层处理节点1",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 100],
				"parameters": {"code": "print('outer processing 1')"}
			},
			{
				"id": "inner-process-1",
				"name": "内层处理节点1",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [680, 50],
				"parameters": {"code": "print('inner processing 1')"}
			},
			{
				"id": "inner-process-2",
				"name": "内层处理节点2",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [900, 50],
				"parameters": {"code": "print('inner processing 2')"}
			},
			{
				"id": "inner-conditional",
				"name": "内层条件节点",
				"type": "n8n-nodes-base.conditional",
				"type_version": 1,
				"version": 1,
				"position": [1120, 50],
				"parameters": {
					"conditions": [
						{
							"id": "inner-check",
							"name": "内层检查",
							"outputPath": "inner-continue",
							"enabled": true,
							"conditions": [
								{
									"leftValue": "$var.inner_continue",
									"operator": "equals",
									"rightValue": true
								}
							]
						}
					],
					"defaultBranch": "inner-exit"
				}
			},
			{
				"id": "outer-process-2",
				"name": "外层处理节点2",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [900, 150],
				"parameters": {"code": "print('outer processing 2')"}
			},
			{
				"id": "outer-conditional",
				"name": "外层条件节点",
				"type": "n8n-nodes-base.conditional",
				"type_version": 1,
				"version": 1,
				"position": [1120, 150],
				"parameters": {
					"conditions": [
						{
							"id": "outer-check",
							"name": "外层检查",
							"outputPath": "outer-continue",
							"enabled": true,
							"conditions": [
								{
									"leftValue": "$var.outer_continue",
									"operator": "equals",
									"rightValue": true
								}
							]
						}
					],
					"defaultBranch": "outer-exit"
				}
			},
			{
				"id": "final-node",
				"name": "最终节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [1340, 100],
				"parameters": {"code": "print('final processing')"}
			},
			{
				"id": "end-node",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"type_version": 1,
				"version": 1,
				"position": [1560, 100],
				"parameters": {}
			}
		],
		"connections": {
			"开始节点": {
				"main": [[{"node": "外层处理节点1", "type": "main", "index": 0}]]
			},
			"外层处理节点1": {
				"main": [[{"node": "内层处理节点1", "type": "main", "index": 0}]]
			},
			"内层处理节点1": {
				"main": [[{"node": "内层处理节点2", "type": "main", "index": 0}]]
			},
			"内层处理节点2": {
				"main": [[{"node": "内层条件节点", "type": "main", "index": 0}]]
			},
			"内层条件节点": {
				"inner-continue": [[{"node": "内层处理节点1", "type": "main", "index": 0}]],
				"inner-exit": [[{"node": "外层处理节点2", "type": "main", "index": 0}]]
			},
			"外层处理节点2": {
				"main": [[{"node": "外层条件节点", "type": "main", "index": 0}]]
			},
			"外层条件节点": {
				"outer-continue": [[{"node": "外层处理节点1", "type": "main", "index": 0}]],
				"outer-exit": [[{"node": "最终节点", "type": "main", "index": 0}]]
			},
			"最终节点": {
				"main": [[{"node": "结束节点", "type": "main", "index": 0}]]
			}
		}
	}`
}

// GetInvalidConditionalRingWorkflowData 获取包含条件节点但无法脱离环的工作流数据
func GetInvalidConditionalRingWorkflowData() string {
	return `{
		"id": "invalid-conditional-ring-workflow",
		"name": "无效条件环工作流",
		"active": false,
		"nodes": [
			{
				"id": "start-node",
				"name": "开始节点",
				"type": "n8n-nodes-base.start",
				"type_version": 1,
				"version": 1,
				"position": [240, 100],
				"parameters": {}
			},
			{
				"id": "process-node",
				"name": "处理节点",
				"type": "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version": 1,
				"position": [460, 100],
				"parameters": {"code": "print('processing')"}
			},
			{
				"id": "conditional-node",
				"name": "条件节点",
				"type": "n8n-nodes-base.conditional",
				"type_version": 1,
				"version": 1,
				"position": [680, 100],
				"parameters": {
					"conditions": [
						{
							"id": "check-condition",
							"name": "检查条件",
							"outputPath": "branch-1",
							"enabled": true,
							"conditions": [
								{
									"leftValue": "$var.condition1",
									"operator": "equals",
									"rightValue": true
								}
							]
						},
						{
							"id": "check-condition-2",
							"name": "检查条件2",
							"outputPath": "branch-2",
							"enabled": true,
							"conditions": [
								{
									"leftValue": "$var.condition2",
									"operator": "equals",
									"rightValue": true
								}
							]
						}
					]
				}
			},
			{
				"id": "end-node",
				"name": "结束节点",
				"type": "n8n-nodes-base.end",
				"type_version": 1,
				"version": 1,
				"position": [900, 150],
				"parameters": {}
			}
		],
		"connections": {
			"开始节点": {
				"main": [[{"node": "处理节点", "type": "main", "index": 0}]]
			},
			"处理节点": {
				"main": [[{"node": "条件节点", "type": "main", "index": 0}]]
			},
			"条件节点": {
				"branch-1": [[{"node": "处理节点", "type": "main", "index": 0}]]
			}
		}
	}`
}

// GetComplexWorkflowData 获取用户提供的复杂工作流数据（原始数据，无环）
func GetComplexWorkflowData() string {
	workflow := map[string]interface{}{
		"id":          "complete-n8n-workflow-v2",
		"name":        "完整节点演示工作流 v2.0.0",
		"active":      false,
		"is_archived": false,
		"nodes": []map[string]interface{}{
			{
				"id":           "start-node",
				"name":         "开始节点",
				"type":         "n8n-nodes-base.start",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{240, 100},
				"parameters": map[string]interface{}{
					"globalParameters": map[string]interface{}{
						"python-node": map[string]interface{}{
							"timeout":      30,
							"requirements": []interface{}{"requests", "pandas"},
							"dockerImage":  "python:3.11-slim",
						},
						"conditional-node": map[string]interface{}{
							"mode":         "strict",
							"evaluateMode": "first",
						},
						"variable-node": map[string]interface{}{
							"overwriteMode": "overwrite",
							"scope":         "global",
						},
					},
				},
			},
			{
				"id":           "variable-node",
				"name":         "变量节点",
				"type":         "n8n-nodes-base.variable",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{460, 100},
				"parameters": map[string]interface{}{
					"operation": "set",
					"variables": map[string]interface{}{
						"user_id":       "12345",
						"session_token": "abc123xyz",
						"request_count": 0,
						"processing_flags": map[string]interface{}{
							"is_premium_user": true,
							"enable_logging":  true,
							"max_retries":     3,
						},
					},
					"overwriteMode":  "overwrite",
					"variablesScope": "global",
				},
			},
			{
				"id":           "domain-resolve",
				"name":         "域名解析",
				"type":         "n8n-nodes-base.domainResolve",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{680, 50},
				"parameters": map[string]interface{}{
					"domain":      "example.com",
					"recordTypes": []interface{}{"A", "AAAA", "MX", "TXT"},
				},
			},
			{
				"id":           "conditional-node",
				"name":         "条件判断节点",
				"type":         "n8n-nodes-base.conditional",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{900, 100},
				"parameters": map[string]interface{}{
					"nodeType": "if",
					"conditions": []map[string]interface{}{
						{
							"id":            "premium-user-check",
							"name":          "高级用户检查",
							"outputPath":    "premium-branch",
							"enabled":       true,
							"logicOperator": "AND",
							"conditions": []map[string]interface{}{
								{
									"leftValue":     "$var.processing_flags.is_premium_user",
									"operator":      "equals",
									"rightValue":    true,
									"caseSensitive": false,
								},
								{
									"leftValue":  "$var.request_count",
									"operator":   "less_than",
									"rightValue": 100,
								},
							},
						},
						{
							"id":            "regular-user-check",
							"name":          "普通用户检查",
							"outputPath":    "regular-branch",
							"enabled":       true,
							"logicOperator": "OR",
							"conditions": []map[string]interface{}{
								{
									"leftValue":  "$var.user_id",
									"operator":   "not_empty",
									"rightValue": "",
								},
								{
									"leftValue":  "$var.session_token",
									"operator":   "not_empty",
									"rightValue": "",
								},
							},
						},
					},
					"defaultBranch": "invalid-user-branch",
				},
			},
			{
				"id":           "python-premium",
				"name":         "Python处理（高级用户）",
				"type":         "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{1120, 50},
				"parameters": map[string]interface{}{
					"code":           "import time\nimport json\nfrom datetime import datetime\n\n# 获取输入数据\nuser_id = input_data.get('user_id', 'unknown')\nsession_token = input_data.get('session_token', '')\nrequest_count = input_data.get('request_count', 0)\nflags = input_data.get('processing_flags', {})\n\n# 高级用户处理逻辑\nprocessing_result = {\n    'user_id': user_id,\n    'processed_at': datetime.now().isoformat(),\n    'processing_type': 'premium',\n    'features_enabled': ['advanced_analytics', 'priority_processing', 'extended_limits'],\n    'next_request_count': request_count + 1,\n    'processing_time_ms': 50,\n    'status': 'success'\n}\n\n# 设置返回结果\nresult = processing_result",
					"dockerImage":    "python:3.11-slim",
					"timeoutSeconds": 60,
					"maxRetries":     3,
					"requirements":   []interface{}{"pandas", "numpy", "requests"},
					"environmentVars": map[string]interface{}{
						"LOG_LEVEL":   "INFO",
						"ENVIRONMENT": "production",
					},
				},
			},
			{
				"id":           "python-regular",
				"name":         "Python处理（普通用户）",
				"type":         "n8n-nodes-base.pythonDocker",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{1120, 150},
				"parameters": map[string]interface{}{
					"code":           "import time\nfrom datetime import datetime\n\n# 获取输入数据\nuser_id = input_data.get('user_id', 'unknown')\nsession_token = input_data.get('session_token', '')\nrequest_count = input_data.get('request_count', 0)\nflags = input_data.get('processing_flags', {})\n\n# 普通用户处理逻辑\nprocessing_result = {\n    'user_id': user_id,\n    'processed_at': datetime.now().isoformat(),\n    'processing_type': 'regular',\n    'features_enabled': ['basic_processing'],\n    'next_request_count': request_count + 1,\n    'processing_time_ms': 100,\n    'status': 'success'\n}\n\n# 设置返回结果\nresult = processing_result",
					"dockerImage":    "python:3.11-slim",
					"timeoutSeconds": 45,
					"maxRetries":     2,
					"requirements":   []interface{}{"requests"},
					"environmentVars": map[string]interface{}{
						"LOG_LEVEL": "INFO",
					},
				},
			},
			{
				"id":           "custom-handler",
				"name":         "自定义处理器",
				"type":         "CUSTOM.customNode",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{1340, 100},
				"parameters": map[string]interface{}{
					"customLogic":   "data_aggregation",
					"outputFormat":  "structured",
					"enableMetrics": true,
				},
			},
			{
				"id":           "variable-update",
				"name":         "变量更新节点",
				"type":         "n8n-nodes-base.variable",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{1560, 100},
				"parameters": map[string]interface{}{
					"operation": "set",
					"variables": map[string]interface{}{
						"last_processed_user":     "{{ $json.user_id }}",
						"processing_complete":     true,
						"final_status":            "{{ $json.status }}",
						"workflow_execution_time": "{{ $now }}",
					},
					"overwriteMode":  "merge",
					"variablesScope": "global",
				},
			},
			{
				"id":           "end-node",
				"name":         "结束节点",
				"type":         "n8n-nodes-base.end",
				"type_version": 1,
				"version":      1,
				"position":     []interface{}{1780, 100},
				"parameters": map[string]interface{}{
					"resultMode":      "all",
					"includeMetadata": true,
					"formatOutput":    true,
				},
			},
		},
		"connections": map[string]interface{}{
			"开始节点": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "变量节点",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"变量节点": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "域名解析",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"域名解析": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "条件判断节点",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"条件判断节点": map[string]interface{}{
				"premium-branch": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "Python处理（高级用户）",
							"type":  "main",
							"index": 0,
						},
					},
				},
				"regular-branch": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "Python处理（普通用户）",
							"type":  "main",
							"index": 0,
						},
					},
				},
				"invalid-user-branch": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "自定义处理器",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"Python处理（高级用户）": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "自定义处理器",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"Python处理（普通用户）": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "自定义处理器",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"自定义处理器": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "变量更新节点",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
			"变量更新节点": map[string]interface{}{
				"main": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"node":  "结束节点",
							"type":  "main",
							"index": 0,
						},
					},
				},
			},
		},
		"settings": map[string]interface{}{
			"executionOrder": "v1",
		},
		"staticData": nil,
		"meta": map[string]interface{}{
			"templateCredsSetupCompleted": true,
		},
		"pinData":      map[string]interface{}{},
		"version_id":   "v2.0.0",
		"triggerCount": 0,
		"tags": []map[string]interface{}{
			{
				"createdAt": "2024-01-01T00:00:00.000Z",
				"updatedAt": "2024-01-01T00:00:00.000Z",
				"id":        "demo-workflow-tag",
				"name":      "完整工作流演示",
			},
		},
	}

	data, _ := json.Marshal(workflow)
	return string(data)
}

// GetAllTestWorkflows 获取所有测试工作流数据
func GetAllTestWorkflows() map[string]string {
	return map[string]string{
		"linear":              GetLinearWorkflowData(),
		"simple_ring":         GetSimpleRingWorkflowData(),
		"conditional_ring":    GetConditionalRingWorkflowData(),
		"multi_ring":          GetMultiRingWorkflowData(),
		"nested_ring":         GetNestedRingWorkflowData(),
		"invalid_conditional": GetInvalidConditionalRingWorkflowData(),
		"complex":             GetComplexWorkflowData(),
	}
}
