package node

import (
	"context"
	"fmt"
	"github.acme.red/wego/pkg/utils/maputil/v2"
	"go.temporal.io/sdk/log"
	"regexp"
	"strconv"
	"strings"
)

// ConditionalNode 统一条件节点，支持if条件和switch分支
type ConditionalNode struct {
	*BaseActivity
}

// ConditionalNodeParameters 条件节点参数
type ConditionalNodeParameters struct {
	Conditions    []ConditionRule        `json:"conditions"`    // 条件规则列表
	DefaultBranch string                 `json:"defaultBranch"` // 默认分支路径
	InputData     map[string]interface{} `json:"inputData"`     // 输入数据
}

// ConditionRule 条件规则
type ConditionRule struct {
	ID            string                `json:"id"`            // 规则ID
	Name          string                `json:"name"`          // 规则名称
	Conditions    []ConditionExpression `json:"conditions"`    // 条件表达式列表
	LogicOperator string                `json:"logicOperator"` // 逻辑操作符: "AND", "OR"
	OutputPath    string                `json:"outputPath"`    // 输出路径
	Enabled       bool                  `json:"enabled"`       // 是否启用
}

// ConditionExpression 条件表达式
type ConditionExpression struct {
	LeftValue     string      `json:"leftValue"`     // 左值
	Operator      string      `json:"operator"`      // 操作符
	RightValue    interface{} `json:"rightValue"`    // 右值
	CaseSensitive bool        `json:"caseSensitive"` // 是否区分大小写
}

// 条件节点逻辑执行结果
type execConditionResult struct {
	Success           bool     `json:"success"`            // 执行结论
	MatchedConditions []string `json:"matched_conditions"` // 规则ID,注意，我们是拿规则名称（OutputPaths）来匹配
	OutputPaths       string   `json:"output_paths"`       // 分支名称
	HasMatch          bool     `json:"has_match"`          // 是否有匹配
	ConditionsCount   int      `json:"conditions_count"`   // 匹配统计
}

var _ Activity = (*ConditionalNode)(nil)

// NewConditional 初始化实例
func NewConditional() *ConditionalNode {
	return &ConditionalNode{}
}

// Conditional 创建条件节点执行入口
func (c *ConditionalNode) Conditional(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	// 初始化数据
	c.BaseActivity = &BaseActivity{
		NodeInfo:            input.Node,
		expressionEvaluator: input.Express,
	}
	// 执行验证
	if err := c.ValidateInput(input); err != nil {
		return nil, err
	}
	// 执行逻辑
	return c.ExecuteWithExecuteTiming(ctx, input, c.execConditionBatch)
}

// GetLogger 获取log对象
func (c *ConditionalNode) GetLogger(ctx context.Context) log.Logger {
	return c.BaseActivity.GetLogger(ctx)
}

// GetNodeInfo 获取当前节点信息
func (c *ConditionalNode) GetNodeInfo() *WkFLowNode {
	return c.NodeInfo
}

// ValidateInput 验证输入参数
func (c *ConditionalNode) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := c.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 条件节点参数不能为空
	if input.Node.Parameters == nil {
		return fmt.Errorf("condition node has no parameters")
	}
	// 验证conditions格式
	if conditions, exists := input.Node.Parameters["conditions"]; exists {
		if _, ok := conditions.([]interface{}); !ok {
			return fmt.Errorf("conditions格式无效，应为数组")
		}
	}
	return nil
}

func (c *ConditionalNode) execConditionBatch(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	var resData = &ExecNodeFuncResult{
		Data: make([]map[string]interface{}, 0, len(input.Node.Parameters)),
	}
	for _, v := range input.InputData {
		res, err := c.execCondition(ctx, input, v)
		if err != nil {
			return nil, err
		}
		resData.Data = append(resData.Data, res)
	}
	return resData, nil
}

// 单个逻辑入口
func (c *ConditionalNode) execCondition(ctx context.Context, input *ActivityInput, inputData map[string]interface{}) (map[string]interface{}, error) {
	logger := c.BaseActivity.GetLogger(ctx)
	// 解析参数
	var params ConditionalNodeParameters
	if err := c.parseParameters(input.Node.Parameters, &params); err != nil {
		return nil, err
	}
	logger.Debug("开始执行条件节点", "条件数", len(params.Conditions))
	// 根据节点类型执行不同逻辑
	result, err := c.executeIfLogic(params, inputData)
	if err != nil {
		logger.Error("条件节点执行失败", "error", err)
		return nil, err
	}
	execRes := map[string]interface{}{"outputPaths": result.OutputPaths}
	logger.Info("条件节点执行成功", "matchedConditions", result.OutputPaths)
	return execRes, nil
}

// parseParameters 解析参数
func (c *ConditionalNode) parseParameters(parameters map[string]interface{}, params *ConditionalNodeParameters) error {
	// 设置默认值
	params.Conditions = make([]ConditionRule, 0)
	params.DefaultBranch = "default"
	params.InputData = make(map[string]interface{})
	// 解析默认分支
	if defaultBranch, exists := parameters["defaultBranch"]; exists {
		if defaultBranchStr, ok := defaultBranch.(string); ok {
			params.DefaultBranch = defaultBranchStr
		}
	}

	// 解析条件规则
	if conditions, exists := parameters["conditions"]; exists {
		if conditionsList, ok := conditions.([]interface{}); ok {
			for _, conditionInterface := range conditionsList {
				if conditionMap, ok := conditionInterface.(map[string]interface{}); ok {
					rule, err := c.parseConditionRule(conditionMap)
					if err != nil {
						return fmt.Errorf("解析条件规则失败: %w", err)
					}
					params.Conditions = append(params.Conditions, rule)
				} else {
					return fmt.Errorf("条件规则格式无效")
				}
			}
		} else {
			return fmt.Errorf("conditions格式无效，应为数组")
		}
	}

	// 解析输入数据
	if inputData, exists := parameters["inputData"]; exists {
		if inputDataMap, ok := inputData.(map[string]interface{}); ok {
			params.InputData = inputDataMap
		}
	}

	return nil
}

// parseConditionRule 解析条件规则
func (c *ConditionalNode) parseConditionRule(conditionMap map[string]interface{}) (ConditionRule, error) {
	rule := ConditionRule{
		Enabled:       true,
		LogicOperator: "AND",
	}

	// 解析基本字段
	if id, exists := conditionMap["id"]; exists {
		if idStr, ok := id.(string); ok {
			rule.ID = idStr
		}
	}

	if name, exists := conditionMap["name"]; exists {
		if nameStr, ok := name.(string); ok {
			rule.Name = nameStr
		}
	}

	if outputPath, exists := conditionMap["outputPath"]; exists {
		if outputPathStr, ok := outputPath.(string); ok {
			rule.OutputPath = outputPathStr
		}
	}

	if enabled, exists := conditionMap["enabled"]; exists {
		if enabledBool, ok := enabled.(bool); ok {
			rule.Enabled = enabledBool
		}
	}

	if logicOperator, exists := conditionMap["logicOperator"]; exists {
		if logicOperatorStr, ok := logicOperator.(string); ok {
			if logicOperatorStr == "AND" || logicOperatorStr == "OR" {
				rule.LogicOperator = logicOperatorStr
			}
		}
	}

	// 解析条件表达式
	if conditions, exists := conditionMap["conditions"]; exists {
		if conditionsList, ok := conditions.([]interface{}); ok {
			for _, exprInterface := range conditionsList {
				if exprMap, ok := exprInterface.(map[string]interface{}); ok {
					expr, err := c.parseConditionExpression(exprMap)
					if err != nil {
						return rule, fmt.Errorf("解析条件表达式失败: %w", err)
					}
					rule.Conditions = append(rule.Conditions, expr)
				}
			}
		}
	}

	return rule, nil
}

// parseConditionExpression 解析条件表达式
func (c *ConditionalNode) parseConditionExpression(exprMap map[string]interface{}) (ConditionExpression, error) {
	expr := ConditionExpression{
		CaseSensitive: true,
	}

	if leftValue, exists := exprMap["leftValue"]; exists {
		if leftValueStr, ok := leftValue.(string); ok {
			expr.LeftValue = leftValueStr
		}
	}

	if operator, exists := exprMap["operator"]; exists {
		if operatorStr, ok := operator.(string); ok {
			expr.Operator = operatorStr
		}
	}

	if rightValue, exists := exprMap["rightValue"]; exists {
		expr.RightValue = rightValue
	}

	if caseSensitive, exists := exprMap["caseSensitive"]; exists {
		if caseSensitiveBool, ok := caseSensitive.(bool); ok {
			expr.CaseSensitive = caseSensitiveBool
		}
	}

	return expr, nil
}

// executeIfLogic 执行if逻辑
func (c *ConditionalNode) executeIfLogic(params ConditionalNodeParameters, data map[string]interface{}) (*execConditionResult, error) {
	var matchedRules []string
	var outputPaths string
	// 评估每个条件规则
	for _, rule := range params.Conditions {
		if !rule.Enabled {
			continue
		}
		ruleResult, err := c.evaluateConditionRule(rule, data)
		if err != nil {
			return nil, fmt.Errorf("评估条件规则失败: %w", err)
		}
		if ruleResult {
			matchedRules = append(matchedRules, rule.ID)
			if rule.OutputPath != "" {
				outputPaths = rule.OutputPath
				break
			}
		}
	}

	// 如果没有匹配的条件，使用默认分支
	if len(matchedRules) == 0 && params.DefaultBranch != "" {
		outputPaths = params.DefaultBranch
	}

	return &execConditionResult{
		Success:           true,
		MatchedConditions: matchedRules,
		OutputPaths:       outputPaths,
		HasMatch:          len(matchedRules) > 0,
		ConditionsCount:   len(params.Conditions),
	}, nil
}

// executeSwitchLogic 执行switch逻辑
func (c *ConditionalNode) executeSwitchLogic(params ConditionalNodeParameters, data map[string]interface{}) (map[string]interface{}, error) {
	var matchedRules []string
	var outputPaths []string

	// switch模式下，评估所有启用的条件规则
	for _, rule := range params.Conditions {
		if !rule.Enabled {
			continue
		}

		ruleResult, err := c.evaluateConditionRule(rule, data)
		if err != nil {
			return nil, fmt.Errorf("评估条件规则失败: %w", err)
		}

		if ruleResult {
			matchedRules = append(matchedRules, rule.ID)
			if rule.OutputPath != "" {
				outputPaths = append(outputPaths, rule.OutputPath)
			}
		}
	}

	// 如果没有匹配的条件，使用默认分支（switch的else分支）
	if len(matchedRules) == 0 && params.DefaultBranch != "" {
		outputPaths = append(outputPaths, params.DefaultBranch)
		matchedRules = append(matchedRules, "default")
	}

	return map[string]interface{}{
		"success":           true,
		"matchedConditions": matchedRules,
		"outputPaths":       outputPaths,
		"hasMatch":          len(matchedRules) > 0,
		"conditionsCount":   len(params.Conditions),
	}, nil
}

// evaluateConditionRule 评估条件规则
func (c *ConditionalNode) evaluateConditionRule(rule ConditionRule, data map[string]interface{}) (bool, error) {
	if len(rule.Conditions) == 0 {
		return false, nil
	}
	results := make([]bool, 0, len(rule.Conditions))
	// 评估每个条件表达式
	for _, expr := range rule.Conditions {
		result, err := c.evaluateConditionExpression(expr, data)
		if err != nil {
			return false, fmt.Errorf("评估条件表达式失败: %w", err)
		}
		results = append(results, result)
	}
	// 根据逻辑操作符计算最终结果
	switch rule.LogicOperator {
	case "AND":
		for _, result := range results {
			if !result {
				return false, nil
			}
		}
		return true, nil
	case "OR":
		for _, result := range results {
			if result {
				return true, nil
			}
		}
		return false, nil
	default:
		// 默认为AND
		for _, result := range results {
			if !result {
				return false, nil
			}
		}
		return true, nil
	}
}

// evaluateConditionExpression 评估条件表达式
func (c *ConditionalNode) evaluateConditionExpression(expr ConditionExpression, data map[string]interface{}) (bool, error) {
	// 获取左值
	leftValue, err := c.extractValue(expr.LeftValue, data)
	if err != nil {
		return false, fmt.Errorf("提取左值失败: %w", err)
	}

	// 获取右值
	rightValue, err := c.extractValue(expr.RightValue, data)
	if err != nil {
		return false, fmt.Errorf("提取右值失败: %w", err)
	}

	// 执行比较操作
	return c.compareValues(leftValue, rightValue, expr.Operator, expr.CaseSensitive)
}

// extractValue 从数据中提取值
func (c *ConditionalNode) extractValue(value interface{}, data map[string]interface{}) (interface{}, error) {
	if valueStr, ok := value.(string); ok {
		// 如果是表达式，使用表达式评估器
		if strings.HasPrefix(valueStr, "$") {
			if c.expressionEvaluator != nil {
				return c.expressionEvaluator.EvaluateExpression(valueStr, data)
			}
		}
		// 如果是非变量节点
		if val := maputil.GetDeepMapValue[any](data, valueStr, nil); val != nil {
			return val, nil
		}
		//// 尝试解析为数字
		//if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
		//	return num, nil
		//}
		//// 尝试解析为布尔值
		//if strings.ToLower(valueStr) == "true" {
		//	return true, nil
		//}
		//if strings.ToLower(valueStr) == "false" {
		//	return false, nil
		//}
	}
	return value, nil
}

// extractFieldValue 从数据中提取字段值
func (c *ConditionalNode) extractFieldValue(fieldPath string, data map[string]interface{}) (interface{}, error) {
	parts := strings.Split(fieldPath, ".")
	current := data

	for i, part := range parts {
		if part == "" {
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
			return nil, fmt.Errorf("字段路径 '%s' 中4缺少 '%s'", fieldPath, part)
		}
	}

	return data, nil
}

// compareValues 比较两个值
func (c *ConditionalNode) compareValues(left, right interface{}, operator string, caseSensitive bool) (bool, error) {
	// 将值转换为字符串进行比较
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	if !caseSensitive {
		leftStr = strings.ToLower(leftStr)
		rightStr = strings.ToLower(rightStr)
	}

	switch operator {
	case "equals", "==":
		return leftStr == rightStr, nil
	case "not_equals", "!=":
		return leftStr != rightStr, nil
	case "contains":
		return strings.Contains(leftStr, rightStr), nil
	case "not_contains":
		return !strings.Contains(leftStr, rightStr), nil
	case "starts_with":
		return strings.HasPrefix(leftStr, rightStr), nil
	case "ends_with":
		return strings.HasSuffix(leftStr, rightStr), nil
	case "is_empty":
		return leftStr == "", nil
	case "is_not_empty":
		return leftStr != "", nil
	case "greater_than", ">":
		return c.compareNumeric(left, right, func(a, b float64) bool { return a > b })
	case "greater_than_or_equal", ">=":
		return c.compareNumeric(left, right, func(a, b float64) bool { return a >= b })
	case "less_than", "<":
		return c.compareNumeric(left, right, func(a, b float64) bool { return a < b })
	case "less_than_or_equal", "<=":
		return c.compareNumeric(left, right, func(a, b float64) bool { return a <= b })
	case "regex":
		matched, err := regexp.MatchString(rightStr, leftStr)
		return matched, err
	case "not_regex":
		matched, err := regexp.MatchString(rightStr, leftStr)
		return !matched, err
	default:
		return false, fmt.Errorf("不支持的操作符: %s", operator)
	}
}

// compareNumeric 数值比较
func (c *ConditionalNode) compareNumeric(left, right interface{}, compareFunc func(float64, float64) bool) (bool, error) {
	leftNum, err := strconv.ParseFloat(fmt.Sprintf("%v", left), 64)
	if err != nil {
		return false, fmt.Errorf("左值无法转换为数字: %v", left)
	}
	rightNum, err := strconv.ParseFloat(fmt.Sprintf("%v", right), 64)
	if err != nil {
		return false, fmt.Errorf("右值无法转换为数字: %v", right)
	}

	return compareFunc(leftNum, rightNum), nil
}
