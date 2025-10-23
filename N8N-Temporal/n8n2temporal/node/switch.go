package node

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// SwitchNodeActivity Switch 节点 Activity
type SwitchNodeActivity struct {
	BaseActivity
}

// NewSwitchNodeActivity 创建新的Switch节点
func NewSwitchNodeActivity() *SwitchNodeActivity {
	activity := &SwitchNodeActivity{
		BaseActivity: BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "switch_node",
				Name:        "Switch Node",
				Type:        "LOGIC.switchNode",
				Description: "Switch between multiple output branches based on rules",
				Version:     "1.0.0",
				Category:    "logic",
				Icon:        "🔀",
			},
		},
	}
	// 初始化表达式评估器
	activity.InitExpressionEvaluator(nil)
	return activity
}

// Execute 执行节点逻辑（实现NodeActivity接口）
func (a *SwitchNodeActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return a.ExecuteWithExecuteTiming(ctx, input, a.executeSwitchNode)
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *SwitchNodeActivity) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}

	// 检查规则参数
	if input.Parameters == nil {
		return fmt.Errorf("缺少parameters参数")
	}

	if _, exists := input.Parameters["rules"]; !exists {
		return fmt.Errorf("缺少rules参数")
	}

	return nil
}

// ExecuteSwitchNode 执行Switch节点（向后兼容）
func (a *SwitchNodeActivity) ExecuteSwitchNode(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	// 转换旧格式到新格式
	nodeInput := &ActivityInput{
		NodeID:      getStringValue(input, "nodeId"),
		NodeName:    getStringValue(input, "nodeName"),
		NodeType:    "LOGIC.switchNode",
		InputData:   getInputData(input),
		Parameters:  getParameters(input),
		WorkflowID:  getStringValue(input, "workflowId"),
		ExecutionID: getStringValue(input, "executionId"),
	}

	// 执行新接口
	output, err := a.Execute(ctx, nodeInput)
	if err != nil {
		return nil, err
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

// executeSwitchNode 内部Switch节点逻辑
func (a *SwitchNodeActivity) executeSwitchNode(input *ActivityInput) (map[string]interface{}, error) {
	// 解析Switch规则
	rules, err := a.parseSwitchRulesFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("解析Switch规则失败: %v", err)
	}

	// 获取输入数据
	inputData := input.InputData
	if inputData == nil {
		inputData = make(map[string]interface{})
	}

	// 评估规则并选择分支
	selectedBranch, matchedRule, err := a.evaluateSwitchRules(rules, inputData)
	if err != nil {
		return nil, fmt.Errorf("评估Switch规则失败: %v", err)
	}

	// 查找匹配的规则索引
	var matchedIndex int = -1
	for i, rule := range rules {
		if rule.OutputValue == selectedBranch {
			matchedIndex = i
			break
		}
	}

	// 如果没有找到匹配的规则索引，返回 -1（默认分支）
	if matchedIndex == -1 {
		matchedRule = ""
	} else {
		// 确保matchedRule不为空
		matchedRule = rules[matchedIndex].ID
	}

	resultData := map[string]interface{}{
		"matchedIndex":   matchedIndex,
		"selectedBranch": selectedBranch,
		"matchedRule":    matchedRule,
		"rules":          rules,
		"inputData":      inputData,
	}

	return resultData, nil
}

// parseSwitchRulesFromInput 从输入中解析Switch规则
func (a *SwitchNodeActivity) parseSwitchRulesFromInput(input *ActivityInput) ([]SwitchRule, error) {
	// 从parameters中获取rules
	if input.Parameters == nil {
		return nil, fmt.Errorf("缺少parameters参数")
	}

	rulesData, ok := input.Parameters["rules"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少rules参数")
	}

	values, ok := rulesData["values"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("rules.values格式错误")
	}

	var rules []SwitchRule
	for i, ruleData := range values {
		ruleMap, ok := ruleData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("规则%d格式错误", i)
		}

		// 解析条件
		conditionsList, ok := ruleMap["conditions"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("规则%d的conditions格式错误", i)
		}

		var conditions []SwitchCondition
		for j, condData := range conditionsList {
			condMap, ok := condData.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("规则%d的条件%d格式错误", i, j)
			}

			condition := SwitchCondition{
				ID:         a.GetStringParameter(condMap, "id"),
				LeftValue:  condMap["leftValue"],
				RightValue: condMap["rightValue"],
				Operator:   a.GetStringParameter(condMap, "operator"),
			}

			conditions = append(conditions, condition)
		}

		rule := SwitchRule{
			ID:          a.GetStringParameter(ruleMap, "id"),
			Conditions:  conditions,
			OutputValue: a.GetStringParameter(ruleMap, "outputValue"),
		}

		rules = append(rules, rule)
	}

	return rules, nil
}

// SwitchRule Switch规则结构
type SwitchRule struct {
	ID          string            `json:"id"`
	Conditions  []SwitchCondition `json:"conditions"`
	OutputValue string            `json:"outputValue"`
}

// SwitchCondition Switch条件结构
type SwitchCondition struct {
	ID         string      `json:"id"`
	LeftValue  interface{} `json:"leftValue"`
	RightValue interface{} `json:"rightValue"`
	Operator   string      `json:"operator"`
}

// parseSwitchRules 解析Switch规则
func (a *SwitchNodeActivity) parseSwitchRules(input map[string]interface{}) ([]SwitchRule, error) {
	// 从parameters中获取rules
	parameters, ok := input["parameters"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少parameters参数")
	}

	rulesData, ok := parameters["rules"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少rules参数")
	}

	values, ok := rulesData["values"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("rules.values格式错误")
	}

	var rules []SwitchRule
	for i, ruleData := range values {
		ruleMap, ok := ruleData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("规则%d格式错误", i)
		}

		// 解析条件
		conditionsList, ok := ruleMap["conditions"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("规则%d的conditions格式错误", i)
		}

		var conditions []SwitchCondition
		for j, condData := range conditionsList {
			condMap, ok := condData.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("规则%d的条件%d格式错误", i, j)
			}

			condition := SwitchCondition{
				ID:         getStringValue(condMap, "id"),
				LeftValue:  condMap["leftValue"],
				RightValue: condMap["rightValue"],
				Operator:   getStringValue(condMap, "operator"),
			}

			conditions = append(conditions, condition)
		}

		rule := SwitchRule{
			ID:          getStringValue(ruleMap, "id"),
			Conditions:  conditions,
			OutputValue: getStringValue(ruleMap, "outputValue"),
		}

		rules = append(rules, rule)
	}

	return rules, nil
}

// evaluateSwitchRules 评估Switch规则
func (a *SwitchNodeActivity) evaluateSwitchRules(rules []SwitchRule, inputData map[string]interface{}) (string, string, error) {
	for _, rule := range rules {
		// 评估当前规则的所有条件（AND逻辑）
		allConditionsMet := true

		for _, condition := range rule.Conditions {
			conditionMet, err := a.evaluateSwitchCondition(condition, inputData)
			if err != nil {
				return "", "", fmt.Errorf("评估条件%s失败: %v", condition.ID, err)
			}

			if !conditionMet {
				allConditionsMet = false
				break
			}
		}

		if allConditionsMet {
			return rule.OutputValue, rule.ID, nil
		}
	}

	// 如果没有规则匹配，返回默认分支
	return "default", "", nil
}

// evaluateSwitchCondition 评估单个Switch条件
func (a *SwitchNodeActivity) evaluateSwitchCondition(condition SwitchCondition, inputData map[string]interface{}) (bool, error) {
	// 解析左右值
	leftValue, err := a.parseSwitchValue(condition.LeftValue, inputData)
	if err != nil {
		return false, fmt.Errorf("解析左值失败: %v", err)
	}

	rightValue, err := a.parseSwitchValue(condition.RightValue, inputData)
	if err != nil {
		return false, fmt.Errorf("解析右值失败: %v", err)
	}

	// 根据操作符进行比较
	switch condition.Operator {
	case "equals":
		return a.compareSwitchValues(leftValue, rightValue), nil
	case "not_equals":
		return !a.compareSwitchValues(leftValue, rightValue), nil
	case "contains":
		if leftStr, ok := leftValue.(string); ok {
			if rightStr, ok := rightValue.(string); ok {
				return strings.Contains(leftStr, rightStr), nil
			}
		}
		return false, nil
	case "not_contains":
		if leftStr, ok := leftValue.(string); ok {
			if rightStr, ok := rightValue.(string); ok {
				return !strings.Contains(leftStr, rightStr), nil
			}
		}
		return true, nil
	case "greater_than":
		return a.compareSwitchNumbers(leftValue, rightValue, true), nil
	case "greater_than_or_equal":
		return a.compareSwitchNumbers(leftValue, rightValue, false), nil
	case "less_than":
		return a.compareSwitchNumbers(leftValue, rightValue, true), nil
	case "less_than_or_equal":
		return a.compareSwitchNumbers(leftValue, rightValue, false), nil
	case "starts_with":
		if leftStr, ok := leftValue.(string); ok {
			if rightStr, ok := rightValue.(string); ok {
				return strings.HasPrefix(leftStr, rightStr), nil
			}
		}
		return false, nil
	case "ends_with":
		if leftStr, ok := leftValue.(string); ok {
			if rightStr, ok := rightValue.(string); ok {
				return strings.HasSuffix(leftStr, rightStr), nil
			}
		}
		return false, nil
	case "is_empty":
		if str, ok := leftValue.(string); ok {
			return str == "", nil
		}
		return false, nil
	case "is_not_empty":
		if str, ok := leftValue.(string); ok {
			return str != "", nil
		}
		return false, nil
	case "is_null":
		return leftValue == nil, nil
	case "is_not_null":
		return leftValue != nil, nil
	default:
		return false, nil
	}
}

// parseSwitchValue 解析Switch值
func (a *SwitchNodeActivity) parseSwitchValue(value interface{}, inputData map[string]interface{}) (interface{}, error) {
	// 如果是字符串且包含{{}}，则为字段路径
	if strValue, ok := value.(string); ok {
		if strings.Contains(strValue, "{{") && strings.Contains(strValue, "}}") {
			// 提取字段路径
			fieldPath := strings.TrimSuffix(strings.TrimPrefix(strValue, "{{"), "}}")
			fieldPath = strings.TrimSpace(fieldPath)

			// 从输入数据中获取字段值
			return a.extractSwitchFieldValue(fieldPath, inputData)
		}
	}

	return value, nil
}

// extractSwitchFieldValue 从输入数据中提取字段值
func (a *SwitchNodeActivity) extractSwitchFieldValue(fieldPath string, inputData map[string]interface{}) (interface{}, error) {
	// 简单的字段路径解析，支持 "json.field1.field2" 格式
	parts := strings.Split(fieldPath, ".")
	current := inputData

	for i, part := range parts {
		if i == 0 && part == "json" {
			// 跳过 "json" 前缀
			continue
		}

		if value, exists := current[part]; exists {
			if i == len(parts)-1 {
				return value, nil
			}

			if nextMap, ok := value.(map[string]interface{}); ok {
				current = nextMap
			} else {
				return nil, fmt.Errorf("字段路径 '%s' 在 '%s' 处不是对象", fieldPath, part)
			}
		} else {
			return nil, fmt.Errorf("字段路径 '%s' 中缺少 '%s'", fieldPath, part)
		}
	}

	return nil, fmt.Errorf("字段路径 '%s' 无效", fieldPath)
}

// compareSwitchValues 比较Switch值
func (a *SwitchNodeActivity) compareSwitchValues(left, right interface{}) bool {
	leftType := reflect.TypeOf(left)
	rightType := reflect.TypeOf(right)

	// 如果类型不同，转换为字符串比较
	if leftType != rightType {
		leftStr := fmt.Sprintf("%v", left)
		rightStr := fmt.Sprintf("%v", right)
		return leftStr == rightStr
	}

	switch leftType.Kind() {
	case reflect.String:
		leftStr := left.(string)
		rightStr := right.(string)
		return leftStr == rightStr
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		leftInt := reflect.ValueOf(left).Int()
		rightInt := reflect.ValueOf(right).Int()
		return leftInt == rightInt
	case reflect.Float32, reflect.Float64:
		leftFloat := reflect.ValueOf(left).Float()
		rightFloat := reflect.ValueOf(right).Float()
		return leftFloat == rightFloat
	case reflect.Bool:
		leftBool := left.(bool)
		rightBool := right.(bool)
		return leftBool == rightBool
	default:
		return false
	}
}

// compareSwitchNumbers 比较数字
func (a *SwitchNodeActivity) compareSwitchNumbers(left, right interface{}, greaterThan bool) bool {
	leftNum, leftErr := a.toSwitchFloat64(left)
	rightNum, rightErr := a.toSwitchFloat64(right)

	if leftErr != nil || rightErr != nil {
		return false
	}

	if greaterThan {
		return leftNum > rightNum
	}
	return leftNum >= rightNum
}

// toSwitchFloat64 转换为float64
func (a *SwitchNodeActivity) toSwitchFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("无法转换为数字: %v", value)
	}
}
