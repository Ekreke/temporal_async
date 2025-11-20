package node

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// 全局变量存储 && 表达式评估器

// WorkflowContext 工作流上下文管理器
type WorkflowContext struct {
	Context map[string]interface{} // 存储每个节点的最新执行结果
}

// NewWorkflowContext 创建新的工作流上下文
func NewWorkflowContext() *WorkflowContext {
	return &WorkflowContext{
		Context: make(map[string]interface{}),
	}
}

// SetNodeData 设置节点的最新数据（覆盖之前的）
func (wc *WorkflowContext) SetNodeData(nodeName string, data map[string]interface{}) {
	wc.Context[nodeName] = data
}

// GetNodeData 获取节点的最新数据
func (wc *WorkflowContext) GetNodeData(nodeName string) (map[string]interface{}, bool) {
	data, exists := wc.Context[nodeName]
	if !exists {
		return nil, false
	}
	if nodeMap, ok := data.(map[string]interface{}); ok {
		return nodeMap, true
	}
	return nil, false
}

// SetNodeDataKV 设置节点数据，KV数据 -- key支持以 . 分割，递归设置数据
func (wc *WorkflowContext) SetNodeDataKV(nodeName string, key string, value interface{}) error {
	// 获取或创建节点数据
	nodeData, ok := wc.Context[nodeName]
	if !ok {
		wc.Context[nodeName] = make(map[string]interface{})
		nodeData = wc.Context[nodeName]
	}
	data, ok := nodeData.(map[string]interface{})
	if !ok {
		data = make(map[string]interface{})
		wc.Context[nodeName] = data
	}
	// 处理空键的情况
	key = strings.TrimSpace(key)
	if key == "" {
		return errors.New("设置变量-'键'不能为空")
	}
	// 开始递归设置key的值
	keyArr := strings.Split(strings.TrimSpace(key), ".")
	curData := data
	for ki, kArr := range keyArr {
		ak := strings.TrimSpace(kArr)
		if ak == "" {
			return errors.New("设置变量-'键'不能出现连续分割符")
		}
		// 跳过最后一层(赋值)的逻辑
		if ki == len(keyArr)-1 {
			break
		}
		curDataV, exists := curData[ak]
		if !exists {
			// 创建新的嵌套map
			curData[ak] = make(map[string]interface{})
			curData = curData[ak].(map[string]interface{})
		} else if nextData, ok := curDataV.(map[string]interface{}); ok {
			// 继续使用已存在的map
			curData = nextData
		} else {
			return errors.New("设置变量-'值'类型错误1")
		}
	}
	// 设置最终的值
	finalKey := strings.TrimSpace(keyArr[len(keyArr)-1])
	if finalKey != "" {
		curData[finalKey] = value
	}
	return nil
}

// GetNodeDataKV 获取节点中指定key的数据 -- key支持以 . 分割，递归获取数据，bool值返回false表示没有指定数据，返回true则表示有
func (wc *WorkflowContext) GetNodeDataKV(nodeName string, key string) (interface{}, bool) {
	nodeData, ok := wc.Context[nodeName]
	if !ok {
		return nil, false
	}
	nodeDataMap, ok := nodeData.(map[string]interface{})
	if !ok {
		return nil, false
	}
	// 分割key
	keyArr := strings.Split(strings.TrimSpace(key), ".")
	curData := nodeDataMap
	for ki, kArr := range keyArr {
		ak := strings.TrimSpace(kArr)
		// 结束
		if ki == len(keyArr)-1 {
			data, ok := curData[ak]
			if !ok {
				return nil, false
			}
			return data, true
		}
		// 递归查值
		curDataV, ok := curData[ak]
		if !ok {
			return nil, false
		}
		curData, ok = curDataV.(map[string]interface{})
		if !ok {
			return nil, false
		}
	}
	return nil, false
}

// GetAllContext 获取完整的上下文
func (wc *WorkflowContext) GetAllContext() map[string]interface{} {
	return wc.Context
}

// ExpressionEvaluator 表达式评估器
type ExpressionEvaluator struct {
	WorkflowContext *WorkflowContext `json:"workflow_context"` // 工作流上下文管理器
}

// NewExpressionEvaluator 创建新的表达式评估器
func NewExpressionEvaluator(workflowContext *WorkflowContext) *ExpressionEvaluator {
	if workflowContext == nil {
		workflowContext = NewWorkflowContext()
	}
	return &ExpressionEvaluator{
		WorkflowContext: workflowContext,
	}
}

// GetWorkflowContext 获取工作流上下文
func (e *ExpressionEvaluator) GetWorkflowContext() *WorkflowContext {
	return e.WorkflowContext
}

// SetWorkflowContext 设置工作流上下文
func (e *ExpressionEvaluator) SetWorkflowContext(workflowContext *WorkflowContext) {
	e.WorkflowContext = workflowContext
}

// EvaluateExpression 评估表达式,转换变量的为具体的值，目前规则为：变量节点 $var、全局节点 $global、指定节点 $('nodeName') 、上个节点响应 $、 常量 xxx
func (e *ExpressionEvaluator) EvaluateExpression(expression string, inputData map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	// 处理各种n8n表达式模式
	switch {
	// 处理变量数据 $var.
	case strings.HasPrefix(expression, "$var."):
		inputData, exists := e.GetWorkflowContext().GetNodeData(ExpressVariablesNodeName)
		if !exists {
			inputData = make(map[string]interface{})
		}
		return e.extractFieldValue(strings.TrimSpace(strings.TrimPrefix(expression, "$var.")), inputData)
	//	处理全局变量 $global
	case strings.HasPrefix(expression, "$global"):
		inputData, exists := e.GetWorkflowContext().GetNodeData(ExpressGlobalNodeName)
		if !exists {
			inputData = make(map[string]interface{})
		}
		return e.extractFieldValue(strings.TrimSpace(strings.TrimPrefix(expression, "$global.")), inputData)
	// 处理指定节点变量
	case strings.HasPrefix(expression, "$('") && strings.Contains(expression, "')"):
		return e.resolveNodeReference(expression)
	// 处理工作流信息 $workflow.id, $workflow.name 等
	case strings.HasPrefix(expression, "$workflow."):
		return e.extractWorkflowInfo(expression)
	// 处理当前时间
	case expression == "$now":
		return time.Now().Format(time.DateTime), nil
	// 处理当前日期
	case expression == "$today":
		return time.Now().Format("2006-01-02"), nil
	// 处理当前时间戳
	case expression == "$timestamp":
		return time.Now().Unix(), nil
	// 处理从inputData获取数据的情况（$开头）
	case strings.HasPrefix(expression, "$"):
		return e.extractFieldValue(strings.TrimPrefix(expression, "$"), inputData)
	// 处理字符串字面量
	case strings.HasPrefix(expression, "\"") && strings.HasSuffix(expression, "\""):
		return strings.TrimPrefix(strings.TrimSuffix(expression, "\""), "\""), nil
	// 处理单引号字符串字面量
	case strings.HasPrefix(expression, "'") && strings.HasSuffix(expression, "'"):
		return strings.TrimPrefix(strings.TrimSuffix(expression, "'"), "'"), nil
	default:
		// 返回原始字符串作为fallback
		return expression, nil
	}
}

// resolveNodeReference 解析节点引用
func (e *ExpressionEvaluator) resolveNodeReference(expression string) (interface{}, error) {
	// 检查工作流上下文
	if e.WorkflowContext == nil {
		return expression, nil
	}

	// 使用正则表达式解析节点引用
	// 匹配模式: $('NodeName').a.b.c
	// 匹配结果：[$('NodeName').a.b.c, NodeName, .a.b.c]
	re := regexp.MustCompile(`\$\('([^']*)'\)(.*)`)
	matches := re.FindStringSubmatch(expression)
	if len(matches) < 3 {
		return nil, fmt.Errorf("未找到匹配内容: %s", expression)
	}
	nodeName := strings.TrimSpace(matches[1])
	fieldPath := strings.Trim(strings.TrimSpace(matches[2]), ".")

	// 从工作流上下文中获取节点数据
	nodeData, exists := e.WorkflowContext.GetNodeData(nodeName)
	if !exists {
		// 如果找不到节点数据，返回表达式本身作为fallback
		return expression, nil
	}
	// 提取字段值
	return e.extractFieldValue(fieldPath, nodeData)
}

// extractWorkflowInfo 提取工作流信息
func (e *ExpressionEvaluator) extractWorkflowInfo(expression string) (interface{}, error) {
	// 简化实现，返回一些基本的工作流信息
	switch expression {
	case "$workflow.id":
		return "workflow_" + fmt.Sprintf("%d", time.Now().Unix()), nil
	case "$workflow.name":
		return "n8n-workflow", nil
	default:
		return expression, nil
	}
}

// extractFieldValue 从输入数据中提取字段值
func (e *ExpressionEvaluator) extractFieldValue(fieldPath string, inputData map[string]interface{}) (interface{}, error) {
	// 处理空字段路径
	if fieldPath == "" {
		return inputData, nil
	}

	// 简单的字段路径解析，支持 "json.field1.field2" 和 "json.array[0]" 格式
	parts := strings.Split(fieldPath, ".")
	current := inputData
	for i, part := range parts {
		// 跳过空的路径部分
		if part == "" {
			continue
		}
		//
		//// 如果是json前缀，需要进入json对象而不是跳过
		//if i == 0 && part == "json" {
		//	if jsonValue, exists := current["json"]; exists {
		//		if jsonMap, ok := jsonValue.(map[string]interface{}); ok {
		//			current = jsonMap
		//			continue
		//		} else {
		//			return nil, fmt.Errorf("json字段不是对象类型")
		//		}
		//	} else {
		//		return nil, fmt.Errorf("字段路径 '%s' 中1缺少 'json' 字段", fieldPath)
		//	}
		//}

		// 检查是否包含数组索引 [数字]
		if strings.Contains(part, "[") && strings.Contains(part, "]") {
			// 分离字段名和索引部分
			fieldName := part[:strings.Index(part, "[")]
			indexStr := part[strings.Index(part, "[")+1 : strings.LastIndex(part, "]")]

			// 获取字段值
			if value, exists := current[fieldName]; exists {
				if array, ok := value.([]interface{}); ok {
					// 解析索引
					if index, err := strconv.Atoi(indexStr); err == nil {
						if index >= 0 && index < len(array) {
							if i == len(parts)-1 {
								return array[index], nil
							}
							// 如果还有后续路径，检查当前元素是否是map
							if nextMap, ok := array[index].(map[string]interface{}); ok {
								current = nextMap
								continue
							} else {
								return nil, fmt.Errorf("数组索引 '%s' 的元素不是对象", fieldPath)
							}
						} else {
							return nil, fmt.Errorf("数组索引 '%d' 超出范围 [0, %d]", index, len(array))
						}
					} else {
						return nil, fmt.Errorf("无效的数组索引 '%s'", indexStr)
					}
				} else {
					return nil, fmt.Errorf("字段 '%s' 不是数组类型", fieldName)
				}
			} else {
				return nil, fmt.Errorf("字段路径 '%s' 中2缺少 '%s'", fieldPath, fieldName)
			}
		}

		// 普通字段访问
		value, exists := current[part]
		if !exists {
			return nil, fmt.Errorf("字段路径 '%s' 中3缺少 '%s'", fieldPath, part)
		}
		if i == len(parts)-1 {
			return value, nil
		}
		nextMap, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("字段路径 '%s' 在 '%s' 处不是对象", fieldPath, part)
		}
		current = nextMap
	}
	return nil, fmt.Errorf("字段路径 '%s' 无效", fieldPath)
}
