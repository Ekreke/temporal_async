# 架构更新文档 - ExpressionEvaluator 重构

## 概述

本次重构将 `ExpressionEvaluator` 从 `condition_if.go` 移动到 `base.go` 中，使其成为所有节点都可以使用的通用组件。这个改动提升了代码复用性并改善了系统架构。

## 重构内容

### 1. ExpressionEvaluator 移动到 base 包

**之前**: `ExpressionEvaluator` 只在 IF 节点中使用
**现在**: `ExpressionEvaluator` 移动到 `BaseActivity` 中，所有节点都可以使用

### 2. 工作流上下文管理优化

**新增功能**:
- `WorkflowContext` 结构体用于管理工作流中的节点数据
- 支持节点数据的存储、获取和更新
- 支持有环工作流（只存储最新节点数据）

### 3. 自动上下文更新

**新增功能**:
- 节点执行完成后自动将结果添加到工作流上下文
- 通过 `ExecuteWithExecuteTiming` 方法实现

## 技术细节

### ExpressionEvaluator 新位置

```go
// 在 base.go 中
type ExpressionEvaluator struct {
    workflowContext *WorkflowContext // 工作流上下文管理器
}

type WorkflowContext struct {
    context map[string]interface{} // 存储每个节点的最新执行结果
}
```

### 节点初始化更新

所有节点现在都需要在构造函数中初始化表达式评估器：

```go
func NewSomeActivity() *SomeActivity {
    activity := &SomeActivity{
        BaseActivity: BaseActivity{
            NodeInfo: &ActivityInfo{
                // ... 节点信息
            },
        },
    }
    // 初始化表达式评估器
    activity.InitExpressionEvaluator(nil)
    return activity
}
```

### 支持的表达式类型

1. **字段引用**: `$json.field`, `$json.array[0]`
2. **节点引用**: `$('NodeName').item.json.field`
3. **函数调用**: `$now.format()`, `$json.length()`
4. **系统变量**: `$timestamp`, `$now`, `$today`
5. **执行上下文**: `$execution.id`, `$workflow.id`
6. **数学表达式**: `$json.field + 1`
7. **正则表达式**: 支持 regex 操作符

## 更新的节点

以下节点已更新以使用新的表达式评估器：

1. **ConditionCheckActivity** - IF 条件节点
2. **CustomNodeActivity** - 自定义节点
3. **PythonCodeActivity** - Python 代码执行节点
4. **SwitchNodeActivity** - Switch 分支节点
5. **DomainResolveActivity** - 域名解析节点

## API 变化

### 新增方法

```go
// 在 BaseActivity 中
func (a *BaseActivity) InitExpressionEvaluator(workflowContext *WorkflowContext)
func (a *BaseActivity) SetWorkflowContext(workflowContext *WorkflowContext)
func (a *BaseActivity) GetExpressionEvaluator() *ExpressionEvaluator
func (a *BaseActivity) AddNodeDataToContext(nodeName string, data map[string]interface{})

// 在 WorkflowContext 中
func (wc *WorkflowContext) SetNodeData(nodeName string, data map[string]interface{})
func (wc *WorkflowContext) GetNodeData(nodeName string) (map[string]interface{}, bool)
func (wc *WorkflowContext) GetAllContext() map[string]interface{}
```

## 使用示例

### 设置工作流上下文

```go
// 创建工作流上下文
workflowContext := NewWorkflowContext()

// 添加节点数据
workflowContext.SetNodeData("Previous Node", map[string]interface{}{
    "json": map[string]interface{}{
        "result": "success",
        "count": 42,
    },
})

// 设置到节点
ifNode := NewConditionCheckActivity()
ifNode.SetWorkflowContext(workflowContext)
```

### 使用表达式评估器

```go
// 在任何继承自 BaseActivity 的节点中
result, err := a.GetExpressionEvaluator().EvaluateExpression(
    "$('Previous Node').item.json.count",
    inputData,
)
```

## 向后兼容性

- 所有现有的节点 API 保持不变
- 测试用例已更新以使用新的 API
- 表达式语法完全兼容 N8N

## 测试状态

重构后的测试结果：
- ✅ 数组索引访问 (`$json.tags[0]`)
- ✅ JSON 函数调用 (`$json.length()`)
- ✅ 类型转换和严格比较
- ✅ 跨节点引用基础功能
- ⚠️ 部分复杂跨节点引用仍需完善

## 总结

这次重构显著提升了系统的可扩展性和代码复用性。ExpressionEvaluator 现在是所有节点的通用组件，为未来添加更多节点类型和表达式功能奠定了基础。