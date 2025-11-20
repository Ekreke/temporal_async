package node

import (
	"context"
	"fmt"
	"n8n2temporal/notify"
	"time"

	activitySdk "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
)

// Activity 统一的节点接口
type Activity interface {
	// GetNodeInfo 获取节点基本信息
	GetNodeInfo() *WkFLowNode
	// Execute 执行节点逻辑
	//Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error)
	// ValidateInput 验证输入参数
	ValidateInput(input *ActivityInput) error
	// GetLogger 获取logger
	GetLogger(ctx context.Context) log.Logger
}

// WkFLowNode 节点定义结构
type WkFLowNode struct {
	ID               string                 `json:"id"`                // 节点ID，同一个工作流中每个节点ID都是唯一的
	Name             string                 `json:"name"`              // 节点名称，这是展示在工作流上的名称，和ID一样也是唯一
	Type             string                 `json:"type"`              // 节点类型
	Parameters       map[string]interface{} `json:"parameters"`        // 节点参数
	ParametersSource string                 `json:"parameters_source"` // 节点参数来源,当is_remote为false，该值无意义。枚举值：$var. 变量节点；$global. 全局参数；某个具体的node名称（获取指定节点）
	Version          float64                `json:"version"`           // 节点的版本，每次更新节点的时候，都需要增加版本号
	IsRemote         bool                   `json:"is_remote"`         // 远端执行标识，false表示本地执行节点（python、condition等），否则表示远端执行（调度）
}

func (wn WkFLowNode) Check() error {
	if wn.ID == "" {
		return fmt.Errorf("node ID is empty")
	}
	if wn.Name == "" {
		return fmt.Errorf("node name is empty")
	}
	if wn.Type == "" {
		return fmt.Errorf("node type is empty")
	}
	if wn.Parameters == nil {
		return fmt.Errorf("node parameters is empty")
	}
	if wn.Version == 0 {
		return fmt.Errorf("node type version is zero")
	}
	return nil
}

// ActivityInput 节点输入数据节点ID
type ActivityInput struct {
	ExecID string `json:"activity_id"` // 节点执行ID
	//NodeID      string                   `json:"nodeId"`      // 节点类型
	//NodeName    string                   `json:"nodeName"`    // 节点名称
	//NodeType    string                   `json:"nodeType"`    // 节点类型
	InputData []map[string]interface{} `json:"inputData"` // 节点需要直接执行的数据
	//Parameters  map[string]interface{}   `json:"parameters"`  // 节点参数
	SignalInput *notify.SignalData `json:"signalInput"` // 上个信息，唤醒信号 todo 考虑是否需要
	WorkflowID  string             `json:"workflowId"`  // 工作流ID
	ExecutionID string             `json:"executionId"` // 执行ID
	//StreamRsp   bool                     `json:"stream_rsp"`  // 是否流式响应
	Express *ExpressionEvaluator `json:"express"` // 表达式解析器
	Node    *WkFLowNode          `json:"node"`    // 节点信息
}

// ActivityOutput 节点输出数据
type ActivityOutput struct {
	NodeID      string                   `json:"nodeId"`      // 节点ID
	NodeName    string                   `json:"nodeName"`    // 节点名称
	NodeType    string                   `json:"nodeType"`    // 节点类型
	Success     bool                     `json:"success"`     // 执行是否成功
	Data        []map[string]interface{} `json:"data"`        // 输出数据
	Error       string                   `json:"error"`       // 错误信息
	ProcessedAt time.Time                `json:"processedAt"` // 处理时间
	Metadata    notify.SignalMetadata    `json:"metadata"`    // 元数据
}

// BaseActivity Activity基类，提供通用功能
type BaseActivity struct {
	NodeInfo            *WkFLowNode
	expressionEvaluator *ExpressionEvaluator // 表达式评估器
}

// GetNodeInfo 获取节点信息（BaseActivity实现）
func (a *BaseActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// ValidateInput 基础输入验证
func (a *BaseActivity) ValidateInput(input *ActivityInput) error {
	if input == nil {
		return fmt.Errorf("输入不能为空")
	}
	if input.Node.Type == "" {
		return fmt.Errorf("节点类型不能为空")
	}
	return nil
}

// GetLogger 获取activity logger
func (a *BaseActivity) GetLogger(ctx context.Context) log.Logger {
	return activitySdk.GetLogger(ctx)
}

// GetExpressionEvaluator 获取表达式评估器
func (a *BaseActivity) GetExpressionEvaluator() *ExpressionEvaluator {
	return a.expressionEvaluator
}

// CreateSuccessOutput 创建成功输出
func (a *BaseActivity) CreateSuccessOutput(input *ActivityInput, execRes *ExecNodeFuncResult) *ActivityOutput {
	if execRes == nil {
		execRes = &ExecNodeFuncResult{
			Data: make([]map[string]interface{}, 0),
		}
	}
	return &ActivityOutput{
		NodeID:      input.Node.ID,
		NodeName:    input.Node.Name,
		NodeType:    input.Node.Type,
		Success:     true,
		Data:        execRes.Data,
		ProcessedAt: time.Now(),
		Metadata:    input.SignalInput.Metadata,
	}
}

// CreateErrorOutput 创建错误输出
func (a *BaseActivity) CreateErrorOutput(input *ActivityInput, err error) *ActivityOutput {
	return &ActivityOutput{
		NodeID:      input.Node.ID,
		NodeName:    input.Node.Name,
		NodeType:    input.Node.Type,
		Success:     false,
		Error:       err.Error(),
		ProcessedAt: time.Now(),
		Metadata:    input.SignalInput.Metadata,
	}
}

// ExecNodeFuncResult 节点执行结果
type ExecNodeFuncResult struct {
	Data []map[string]interface{} `json:"data"` // 节点执行结果
}

// 节点逻辑函数抽象
type nodeExecFunc func(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error)

// ExecuteWithExecuteTiming 统一执行节点实际逻辑，带时间监控
func (a *BaseActivity) ExecuteWithExecuteTiming(ctx context.Context, input *ActivityInput, executeFunc nodeExecFunc) (*ActivityOutput, error) {
	logger := a.GetLogger(ctx)
	startTime := time.Now()
	logger.Info(input.Node.Name+" 开始执行节点", "nodeType", input.Node.Type, "nodeId", input.Node.ID, "nodeName", input.Node.Name)
	// 执行具体逻辑
	data, err := executeFunc(ctx, input)
	duration := time.Since(startTime)
	if err != nil {
		logger.Error(input.Node.Name+" 节点执行失败", "nodeType", input.Node.Type, "nodeId", input.Node.ID, "nodeName", input.Node.Name, "error", err, "duration", duration.String())
		return a.CreateErrorOutput(input, err), nil
	}
	// 创建输出
	output := a.CreateSuccessOutput(input, data)
	logger.Info(input.Node.Name+" 节点执行成功", "nodeType", "nodeType", input.Node.Type, "nodeId", input.Node.ID, "nodeName", input.Node.Name, "duration", duration.String())
	return output, nil
}
