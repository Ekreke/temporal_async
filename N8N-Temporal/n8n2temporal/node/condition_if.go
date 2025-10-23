package node

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// IF节点逻辑数据结构和表达式评估系统
/**
IF节点是n8n中的条件判断节点，用于根据指定的条件控制工作流的分支。

## 条件操作符（完整n8n兼容）

### 字符串操作符 (15个)
- `equals` / `==` - 等于
- `not_equals` / `!=` - 不等于
- `contains` - 包含
- `not_contains` - 不包含
- `starts_with` - 开头是
- `ends_with` - 结尾是
- `regex` - 正则表达式匹配
- `not_regex` - 正则表达式不匹配
- `is_empty` - 为空
- `is_not_empty` - 不为空
- `in` - 在列表中
- `not_in` - 不在列表中
- `len_eq` - 长度等于
- `len_gt` - 长度大于
- `len_lt` - 长度小于

### 数字操作符 (6个)
- `equals` / `==` - 等于
- `not_equals` / `!=` - 不等于
- `greater_than` / `>` - 大于
- `greater_than_or_equal` / `>=` - 大于等于
- `less_than` / `<` - 小于
- `less_than_or_equal` / `<=` - 小于等于

### 日期时间操作符 (6个)
- `equals` / `==` - 等于
- `not_equals` / `!=` - 不等于
- `greater_than` / `>` - 晚于
- `greater_than_or_equal` / `>=` - 晚于等于
- `less_than` / `<` - 早于
- `less_than_or_equal` / `<=` - 早于等于

### 布尔操作符 (4个)
- `equals` / `==` - 等于
- `not_equals` / `!=` - 不等于
- `is_true` - 为真
- `is_false` - 为假

### 数组操作符 (9个)
- `equals` / `==` - 等于
- `not_equals` / `!=` - 不等于
- `contains` - 包含元素
- `not_contains` - 不包含元素
- `size_eq` - 大小等于
- `size_gt` - 大小大于
- `size_lt` - 大小小于
- `is_empty` - 为空
- `is_not_empty` - 不为空

### 对象操作符 (5个)
- `equals` / `==` - 等于
- `not_equals` / `!=` - 不等于
- `has_key` - 包含键
- `not_has_key` - 不包含键
- `size_eq` - 键数量等于

## 条件组合器
- `and` - 所有条件都必须为真
- `or` - 任意一个条件为真即可

## 使用示例

### 示例1：域名检查
```json
{
    "leftValue": "={{ $json.domain }}",
    "rightValue": "=www.baidu.com",
    "operator": {
        "type": "string",
        "operation": "equals"
    }
}
```

### 示例2：引用其他节点数据
```json
{
    "leftValue": "={{ $('Python Script').item.json[\"result\"] }}",
    "rightValue": "=success",
    "operator": {
        "type": "string",
        "operation": "equals"
    }
}
```

### 示例3：多条件组合
```json
{
    "conditions": [
        {
            "leftValue": "={{ $json.status }}",
            "rightValue": "=active",
            "operator": {"operation": "equals"}
        },
        {
            "leftValue": "={{ $json.count }}",
            "rightValue": "=0",
            "operator": {"operation": "greater_than"}
        }
    ],
    "combinator": "and"
}
```

## 工作流上下文管理

为了支持节点间的数据引用，IF节点需要工作流上下文：

```go
// 创建IF节点并设置工作流上下文
ifNode := NewConditionCheckActivity()

// 设置工作流上下文（包含所有已执行节点的数据）
workflowContext := map[string]interface{}{
    "Code in Python (Beta)": map[string]interface{}{
        "json": map[string]interface{}{
            "@type": "success",
            "result": "processed",
        },
    },
}
ifNode.SetWorkflowContext(workflowContext)

// 动态添加节点数据
ifNode.AddNodeDataToContext("New Node", nodeOutputData)
```

## 输出格式

IF节点执行后返回：
```json
{
    "matchedIndex": 0,           // 匹配的分支索引 (0=真分支, 1=假分支)
    "conditionMet": true,         // 条件是否满足
    "output": {                   // 详细的执行结果
        "conditionMet": true,
        "conditions": [...],      // 评估的条件列表
        "combinator": "and",      // 条件组合器
        "inputData": {...},       // 输入数据
        "evaluationTime": 1640995200
    }
}
```

## 错误处理

- 表达式解析错误会返回详细的错误信息
- 找不到引用的节点数据时，会返回表达式字符串作为fallback
- 无效的字段路径会返回具体的路径错误
- 类型转换错误会提供明确的错误原因
*/

// ConditionCheckActivity 条件判断 Activity（对应 IF 节点）
type ConditionCheckActivity struct {
	BaseActivity
}

// NewConditionCheckActivity 创建新的条件判断节点
func NewConditionCheckActivity() *ConditionCheckActivity {
	return &ConditionCheckActivity{
		BaseActivity: BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "condition_check",
				Name:        "IF Condition",
				Type:        "LOGIC.conditionCheck",
				Description: "Evaluate conditional logic for branching",
				Version:     "1.0.0",
				Category:    "logic",
				Icon:        "🔀",
			},
		},
	}
}

// Execute 执行节点逻辑（实现NodeActivity接口）
func (a *ConditionCheckActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return a.BaseActivity.ExecuteWithExecuteTiming(ctx, input, a.executeIf)
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *ConditionCheckActivity) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 检查条件参数
	if input.Parameters == nil {
		return fmt.Errorf("缺少parameters参数")
	}
	if _, exists := input.Parameters["conditions"]; !exists {
		return fmt.Errorf("缺少conditions参数")
	}
	return nil
}

// SetWorkflowContext 设置工作流上下文，用于解析节点引用
func (a *ConditionCheckActivity) SetWorkflowContext(workflowContext *WorkflowContext) {
	a.InitExpressionEvaluator(workflowContext)
}

// executeIf 内部条件判断逻辑
func (a *ConditionCheckActivity) executeIf(input *ActivityInput) (map[string]interface{}, error) {
	// 解析条件参数
	paramsData, err := sonic.Marshal(input.Parameters)
	if err != nil {
		return nil, fmt.Errorf("条件逻辑序列化失败: %v", err)
	}
	conditionsData, err := a.parseConditionsFromInput(paramsData)
	if err != nil {
		return nil, fmt.Errorf("解析条件失败: %v", err)
	}
	// 获取输入数据
	inputData := input.InputData
	if inputData == nil {
		inputData = make(map[string]interface{})
	}

	// 评估条件
	conditionMet, err := a.evaluateConditions(conditionsData.Conditions, inputData, conditionsData.Combinator)
	if err != nil {
		return nil, fmt.Errorf("条件评估失败: %v", err)
	}

	// 根据条件结果确定匹配的规则索引
	var matchedIndex int
	var outputData map[string]interface{}

	if conditionMet {
		// 条件为真，匹配第一个规则 (index 0 - main分支)
		matchedIndex = 0
		outputData = map[string]interface{}{
			"conditionMet":   true,
			"conditions":     conditionsData.Conditions,
			"combinator":     conditionsData.Combinator,
			"inputData":      inputData,
			"evaluationTime": time.Now().Unix(),
		}
	} else {
		// 条件为假，匹配第二个规则 (index 1 - alternative分支)
		matchedIndex = 1
		outputData = map[string]interface{}{
			"conditionMet":   false,
			"conditions":     conditionsData.Conditions,
			"combinator":     conditionsData.Combinator,
			"inputData":      inputData,
			"evaluationTime": time.Now().Unix(),
		}
	}

	// 返回匹配规则索引和输出数据
	resultData := map[string]interface{}{
		"matchedIndex": matchedIndex,
		"output":       outputData,
		"conditions":   conditionsData.Conditions,
		"combinator":   conditionsData.Combinator,
		"conditionMet": conditionMet,
		"inputData":    inputData,
	}

	return resultData, nil
}

type CombinatorType string

var (
	CombinatorAND = CombinatorType("and") // 且逻辑
	CombinatorOR  = CombinatorType("or")  // 或逻辑
)

// ConditionsData 条件数据结构（包含条件和组合器，内部使用）
type ConditionsData struct {
	Conditions []Condition    `json:"conditions"`
	Combinator CombinatorType `json:"combinator"`
}

// if条件格式定义
type parametersIf struct {
	Conditions struct {
		Conditions []struct {
			Id         string `json:"id"`         // id,唯一标识
			LeftValue  string `json:"leftValue"`  // 左侧值
			RightValue string `json:"rightValue"` // 右侧值
			Operator   struct {
				Type      string `json:"type"`      // 类型，值类型
				Operation string `json:"operation"` // 匹配条件，equals等于、notEquals不等于
				Name      string `json:"name,omitempty"`
			} `json:"operator"` // 条件逻辑
		} `json:"conditions"` // 自定义条件
		Combinator string `json:"combinator"` // 条件间关系，and || or
	} `json:"conditions"` // 条件逻辑封装
}

// parseConditionsFromInput 从输入中解析条件
func (a *ConditionCheckActivity) parseConditionsFromInput(parameters []byte) (*ConditionsData, error) {
	// 从parameters中获取conditions
	if parameters == nil || len(parameters) == 0 {
		return nil, fmt.Errorf("缺少parameters参数")
	}
	// 条件组装
	var params *parametersIf
	err := sonic.Unmarshal(parameters, &params)
	if err != nil {
		return nil, fmt.Errorf("node if parameters error: %v", err)
	}
	// 获取条件逻辑
	if len(params.Conditions.Conditions) < 1 {
		return nil, fmt.Errorf("条件列表不能为空")
	}
	// 逐个遍历条件逻辑
	var conditions []Condition
	for _, condition := range params.Conditions.Conditions {
		condition := Condition{
			ID:             condition.Id,
			LeftValue:      condition.LeftValue,
			RightValue:     condition.RightValue,
			Operator:       condition.Operator.Operation,
			TypeValidation: condition.Operator.Type,
		}
		conditions = append(conditions, condition)
	}
	return &ConditionsData{Conditions: conditions, Combinator: CombinatorType(params.Conditions.Combinator)}, nil
}

// ExecuteIf 执行IF条件判断（向后兼容）
func (a *ConditionCheckActivity) ExecuteIf(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	// 转换旧格式到新格式
	nodeInput := &ActivityInput{
		NodeID:      getStringValue(input, "nodeId"),
		NodeName:    getStringValue(input, "nodeName"),
		NodeType:    "LOGIC.conditionCheck",
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

// Condition 条件结构
type Condition struct {
	ID             string      `json:"id"`             // 唯一标识
	LeftValue      interface{} `json:"leftValue"`      // 条件左侧的值
	RightValue     interface{} `json:"rightValue"`     // 条件右侧的值
	Operator       string      `json:"operator"`       // 条件（equals等于，notEquals不等于）
	TypeValidation string      `json:"typeValidation"` // 值类型（例如string）
}

// parseConditions 解析条件配置（向后兼容）
func (a *ConditionCheckActivity) parseConditions(input map[string]interface{}) ([]Condition, error) {
	// 从parameters中获取conditions
	parameters, ok := input["parameters"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少parameters参数")
	}

	conditionsData, ok := parameters["conditions"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("conditions参数格式错误，应为对象")
	}

	options, ok := conditionsData["options"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少conditions.options参数")
	}

	conditionsList, ok := options["conditions"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("conditions.options.conditions格式错误")
	}

	var conditions []Condition
	for i, condData := range conditionsList {
		condMap, ok := condData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("条件%d格式错误", i)
		}

		condition := Condition{
			ID:             getStringValue(condMap, "id"),
			LeftValue:      condMap["leftValue"],
			RightValue:     condMap["rightValue"],
			Operator:       getStringValue(condMap, "operator"),
			TypeValidation: getStringValue(condMap, "typeValidation"),
		}

		conditions = append(conditions, condition)
	}

	return conditions, nil
}

// parseConditionsWithCombinator 解析条件配置和组合器
func (a *ConditionCheckActivity) parseConditionsWithCombinator(input map[string]interface{}) (*ConditionsData, error) {
	conditions, err := a.parseConditions(input)
	if err != nil {
		return nil, err
	}

	// 获取combinator
	parameters, ok := input["parameters"].(map[string]interface{})
	if !ok {
		return &ConditionsData{Conditions: conditions, Combinator: "and"}, nil
	}

	conditionsData, ok := parameters["conditions"].(map[string]interface{})
	if !ok {
		return &ConditionsData{Conditions: conditions, Combinator: "and"}, nil
	}

	options, ok := conditionsData["options"].(map[string]interface{})
	if !ok {
		return &ConditionsData{Conditions: conditions, Combinator: "and"}, nil
	}

	combinator := getStringValue(options, "combinator")
	if combinator == "" {
		combinator = "and"
	}

	return &ConditionsData{
		Conditions: conditions,
		Combinator: CombinatorType(combinator),
	}, nil
}

// evaluateConditions 评估条件
func (a *ConditionCheckActivity) evaluateConditions(conditions []Condition, inputData map[string]interface{}, combinator CombinatorType) (bool, error) {
	results := make([]bool, 0, len(conditions))
	// 开始遍历全部条件
	for _, condition := range conditions {
		evalResult, err := a.evaluateSingleCondition(condition, inputData)
		if err != nil {
			return false, fmt.Errorf("评估条件%s失败: %v", condition.ID, err)
		}
		results = append(results, evalResult)
	}
	// 根据组合器计算最终结果
	switch combinator {
	case CombinatorAND:
		for _, result := range results {
			if !result {
				return false, nil
			}
		}
		return true, nil
	case CombinatorOR:
		for _, result := range results {
			if result {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("未定义的条件组合器: %s", combinator)
	}
}

// evaluateSingleCondition 评估单个条件
func (a *ConditionCheckActivity) evaluateSingleCondition(condition Condition, inputData map[string]interface{}) (bool, error) {
	// 解析左右值
	leftValue, err := a.parseValue(condition.LeftValue, inputData)
	if err != nil {
		return false, fmt.Errorf("解析左值失败: %v", err)
	}
	rightValue, err := a.parseValue(condition.RightValue, inputData)
	if err != nil {
		return false, fmt.Errorf("解析右值失败: %v", err)
	}
	// 根据操作符进行比较
	return a.compareValues(leftValue, rightValue, condition.Operator, condition.TypeValidation)
}

// parseValue 解析值（支持 N8N 表达式语法）
func (a *ConditionCheckActivity) parseValue(value interface{}, inputData map[string]interface{}) (interface{}, error) {
	if strValue, ok := value.(string); ok {
		// 处理 N8N 表达式格式
		if strings.HasPrefix(strValue, "={{") && strings.HasSuffix(strValue, "}}") {
			// 提取表达式内容，去掉 "={{" 和 "}}"
			expression := strings.TrimPrefix(strValue, "={{")
			expression = strings.TrimSuffix(expression, "}}")
			expression = strings.TrimSpace(expression)

			return a.evaluateN8NExpression(expression, inputData)
		} else if strings.HasPrefix(strValue, "=") && !strings.Contains(strValue, "{{") {
			// 处理直接值格式，如 "=www.baidu.com"，去掉等号
			directValue := strings.TrimPrefix(strValue, "=")
			trimmed := strings.TrimSpace(directValue)

			// 尝试转换为数字
			if intVal, err := strconv.Atoi(trimmed); err == nil {
				return intVal, nil
			}
			if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
				return floatVal, nil
			}
			// 尝试转换为布尔值
			if strings.ToLower(trimmed) == "true" {
				return true, nil
			}
			if strings.ToLower(trimmed) == "false" {
				return false, nil
			}

			return trimmed, nil
		}
	}
	return value, nil
}

// evaluateN8NExpression 评估 N8N 表达式（使用base中的表达式评估器）
func (a *ConditionCheckActivity) evaluateN8NExpression(expression string, inputData map[string]interface{}) (interface{}, error) {
	if a.expressionEvaluator == nil {
		return expression, nil
	}
	return a.expressionEvaluator.EvaluateExpression(expression, inputData)
}

// extractFieldValue 从输入数据中提取字段值（委托给expressionEvaluator）
func (a *ConditionCheckActivity) extractFieldValue(fieldPath string, inputData map[string]interface{}) (interface{}, error) {
	if a.expressionEvaluator == nil {
		return nil, fmt.Errorf("表达式评估器未初始化")
	}
	return a.expressionEvaluator.extractFieldValue(fieldPath, inputData)
}

// compareValues 比较两个值（完整n8n兼容）
func (a *ConditionCheckActivity) compareValues(left, right interface{}, operator, typeValidation string) (bool, error) {
	// 验证操作符
	if !a.isValidOperator(operator) {
		return false, fmt.Errorf("不支持的操作符: %s", operator)
	}

	// 根据操作符类型和值类型选择比较方法
	switch operator {
	// 正则表达式操作符
	case "regex", "not_regex":
		return a.compareRegex(left, right, operator)

	// 布尔特定操作符
	case "is_true", "is_false":
		return a.compareBooleanSpecific(left, operator)

	// 长度操作符
	case "len_eq", "len_gt", "len_lt":
		return a.compareLength(left, right, operator)

	// 数组大小操作符
	case "size_eq", "size_gt", "size_lt":
		return a.compareSize(left, right, operator)

	// 对象键操作符
	case "has_key", "not_has_key":
		return a.compareHasKey(left, right, operator)

	// 包含操作符（支持数组、字符串、对象）
	case "contains", "not_contains", "in", "not_in":
		return a.compareContains(left, right, operator)

	// 空值操作符
	case "is_empty", "is_not_empty":
		return a.compareEmpty(left, operator)

	// 默认比较操作符
	default:
		return a.compareDefault(left, right, operator, typeValidation)
	}
}

// compareRegex 正则表达式比较
func (a *ConditionCheckActivity) compareRegex(left, right interface{}, operator string) (bool, error) {
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	matched, err := regexp.MatchString(rightStr, leftStr)
	if err != nil {
		return false, fmt.Errorf("正则表达式错误: %v", err)
	}

	if operator == "not_regex" {
		return !matched, nil
	}
	return matched, nil
}

// compareBooleanSpecific 布尔特定比较
func (a *ConditionCheckActivity) compareBooleanSpecific(left interface{}, operator string) (bool, error) {
	var boolValue bool
	switch v := left.(type) {
	case bool:
		boolValue = v
	case string:
		boolValue = strings.ToLower(v) == "true"
	default:
		return false, fmt.Errorf("无法转换为布尔值: %T", left)
	}

	switch operator {
	case "is_true":
		return boolValue, nil
	case "is_false":
		return !boolValue, nil
	default:
		return false, fmt.Errorf("未知的布尔操作符: %s", operator)
	}
}

// compareLength 长度比较
func (a *ConditionCheckActivity) compareLength(left, right interface{}, operator string) (bool, error) {
	var length int
	switch v := left.(type) {
	case string:
		length = len(v)
	case []interface{}:
		length = len(v)
	case map[string]interface{}:
		length = len(v)
	default:
		return false, fmt.Errorf("无法获取类型 %T 的长度", left)
	}

	rightNum, err := a.toFloat64(right)
	if err != nil {
		return false, fmt.Errorf("右值无法转换为数字: %v", err)
	}

	switch operator {
	case "len_eq":
		return float64(length) == rightNum, nil
	case "len_gt":
		return float64(length) > rightNum, nil
	case "len_lt":
		return float64(length) < rightNum, nil
	default:
		return false, fmt.Errorf("未知的长度操作符: %s", operator)
	}
}

// compareSize 大小比较（数组/对象）
func (a *ConditionCheckActivity) compareSize(left, right interface{}, operator string) (bool, error) {
	var size int
	switch v := left.(type) {
	case []interface{}:
		size = len(v)
	case map[string]interface{}:
		size = len(v)
	default:
		return false, fmt.Errorf("无法获取类型 %T 的大小", left)
	}

	rightNum, err := a.toFloat64(right)
	if err != nil {
		return false, fmt.Errorf("右值无法转换为数字: %v", err)
	}

	switch operator {
	case "size_eq":
		return float64(size) == rightNum, nil
	case "size_gt":
		return float64(size) > rightNum, nil
	case "size_lt":
		return float64(size) < rightNum, nil
	default:
		return false, fmt.Errorf("未知的大小操作符: %s", operator)
	}
}

// compareHasKey 对象键比较
func (a *ConditionCheckActivity) compareHasKey(left, right interface{}, operator string) (bool, error) {
	leftMap, ok := left.(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("左值不是对象类型: %T", left)
	}

	key, ok := right.(string)
	if !ok {
		key = fmt.Sprintf("%v", right)
	}

	hasKey := false
	for k := range leftMap {
		if k == key {
			hasKey = true
			break
		}
	}

	switch operator {
	case "has_key":
		return hasKey, nil
	case "not_has_key":
		return !hasKey, nil
	default:
		return false, fmt.Errorf("未知的键操作符: %s", operator)
	}
}

// compareContains 包含比较（支持字符串、数组、对象）
func (a *ConditionCheckActivity) compareContains(left, right interface{}, operator string) (bool, error) {
	var contains bool

	switch l := left.(type) {
	case string:
		// 字符串包含
		rightStr := fmt.Sprintf("%v", right)
		contains = strings.Contains(l, rightStr)

	case []interface{}:
		// 数组包含
		rightStr := fmt.Sprintf("%v", right)
		for _, item := range l {
			if fmt.Sprintf("%v", item) == rightStr {
				contains = true
				break
			}
		}

	case map[string]interface{}:
		// 对象包含（检查键或值）
		rightStr := fmt.Sprintf("%v", right)
		for key, value := range l {
			if key == rightStr || fmt.Sprintf("%v", value) == rightStr {
				contains = true
				break
			}
		}

	default:
		return false, fmt.Errorf("类型 %T 不支持包含操作", left)
	}

	// 处理反操作符
	switch operator {
	case "not_contains":
		return !contains, nil
	case "in":
		// 'in' 操作符左右相反：检查左值是否在右值中
		return a.compareContains(right, left, "contains")
	case "not_in":
		// 'not_in' 操作符左右相反
		result, err := a.compareContains(right, left, "contains")
		return !result, err
	default:
		return contains, nil
	}
}

// compareEmpty 空值比较
func (a *ConditionCheckActivity) compareEmpty(left interface{}, operator string) (bool, error) {
	var isEmpty bool

	switch v := left.(type) {
	case string:
		isEmpty = v == ""
	case []interface{}:
		isEmpty = len(v) == 0
	case map[string]interface{}:
		isEmpty = len(v) == 0
	case nil:
		isEmpty = true
	default:
		isEmpty = false
	}

	switch operator {
	case "is_empty":
		return isEmpty, nil
	case "is_not_empty":
		return !isEmpty, nil
	default:
		return false, fmt.Errorf("未知的空值操作符: %s", operator)
	}
}

// compareDefault 默认比较（包含原有的比较逻辑）
func (a *ConditionCheckActivity) compareDefault(left, right interface{}, operator, typeValidation string) (bool, error) {
	// 类型转换
	switch typeValidation {
	case "strict":
		// 严格类型比较
		return a.compareStrict(left, right, operator)
	case "loose":
		// 松散类型比较
		return a.compareLoose(left, right, operator)
	default:
		// 默认松散比较
		return a.compareLoose(left, right, operator)
	}
}

// compareStrict 严格类型比较
func (a *ConditionCheckActivity) compareStrict(left, right interface{}, operator string) (bool, error) {
	leftType := reflect.TypeOf(left)
	rightType := reflect.TypeOf(right)

	if leftType != rightType {
		return false, nil
	}

	switch leftType.Kind() {
	case reflect.String:
		leftStr := left.(string)
		rightStr := right.(string)
		return a.compareStrings(leftStr, rightStr, operator), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		leftInt := reflect.ValueOf(left).Int()
		rightInt := reflect.ValueOf(right).Int()
		return a.compareNumbers(float64(leftInt), float64(rightInt), operator), nil
	case reflect.Float32, reflect.Float64:
		leftFloat := reflect.ValueOf(left).Float()
		rightFloat := reflect.ValueOf(right).Float()
		return a.compareNumbers(leftFloat, rightFloat, operator), nil
	case reflect.Bool:
		leftBool := left.(bool)
		rightBool := right.(bool)
		return a.compareBooleans(leftBool, rightBool, operator), nil
	default:
		return false, fmt.Errorf("不支持的严格比较类型: %v", leftType)
	}
}

// compareLoose 松散类型比较
func (a *ConditionCheckActivity) compareLoose(left, right interface{}, operator string) (bool, error) {
	// 尝试转换为数字进行比较
	leftNum, leftErr := a.toFloat64(left)
	rightNum, rightErr := a.toFloat64(right)

	if leftErr == nil && rightErr == nil {
		return a.compareNumbers(leftNum, rightNum, operator), nil
	}

	// 尝试转换为字符串进行比较
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	return a.compareStrings(leftStr, rightStr, operator), nil
}

// compareStrings 字符串比较
func (a *ConditionCheckActivity) compareStrings(left, right string, operator string) bool {
	switch operator {
	case "equals", "==":
		return left == right
	case "not_equals", "!=":
		return left != right
	case "contains":
		return strings.Contains(left, right)
	case "not_contains":
		return !strings.Contains(left, right)
	case "starts_with":
		return strings.HasPrefix(left, right)
	case "ends_with":
		return strings.HasSuffix(left, right)
	case "greater_than", ">":
		return left > right
	case "greater_than_or_equal", ">=":
		return left >= right
	case "less_than", "<":
		return left < right
	case "less_than_or_equal", "<=":
		return left <= right
	case "is_empty":
		return left == ""
	case "is_not_empty":
		return left != ""
	default:
		return false
	}
}

// compareNumbers 数字比较
func (a *ConditionCheckActivity) compareNumbers(left, right float64, operator string) bool {
	switch operator {
	case "equals", "==":
		return left == right
	case "not_equals", "!=":
		return left != right
	case "greater_than", ">":
		return left > right
	case "greater_than_or_equal", ">=":
		return left >= right
	case "less_than", "<":
		return left < right
	case "less_than_or_equal", "<=":
		return left <= right
	default:
		return false
	}
}

// compareBooleans 布尔值比较
func (a *ConditionCheckActivity) compareBooleans(left, right bool, operator string) bool {
	switch operator {
	case "equals", "==":
		return left == right
	case "not_equals", "!=":
		return left != right
	default:
		return false
	}
}

// isValidOperator 验证操作符是否有效（支持所有n8n操作符）
func (a *ConditionCheckActivity) isValidOperator(operator string) bool {
	validOperators := map[string]bool{
		// 字符串操作符
		"equals":       true,
		"==":           true,
		"not_equals":   true,
		"!=":           true,
		"contains":     true,
		"not_contains": true,
		"starts_with":  true,
		"ends_with":    true,
		"regex":        true,
		"not_regex":    true,
		"is_empty":     true,
		"is_not_empty": true,
		"in":           true,
		"not_in":       true,
		"len_eq":       true,
		"len_gt":       true,
		"len_lt":       true,

		// 数字操作符
		"greater_than":          true,
		">":                     true,
		"greater_than_or_equal": true,
		">=":                    true,
		"less_than":             true,
		"<":                     true,
		"less_than_or_equal":    true,
		"<=":                    true,

		// 布尔操作符
		"is_true":  true,
		"is_false": true,

		// 数组操作符
		"size_eq": true,
		"size_gt": true,
		"size_lt": true,

		// 对象操作符
		"has_key":     true,
		"not_has_key": true,
	}
	return validOperators[operator]
}

// toFloat64 转换为float64
func (a *ConditionCheckActivity) toFloat64(value interface{}) (float64, error) {
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
